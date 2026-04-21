from __future__ import annotations

import re
from typing import Any, Dict, Optional

from bsab_kg_qa_en.core.orchestrator import AnswerBundle, _GraphBaseOrchestrator
from bsab_kg_qa_en.core.llm_provider import LLMProvider
from bsab_kg_qa_en.intents.registry import IntentRegistry
from bsab_kg_qa_en.kg.neo4j_runner import Neo4jRunner
from bsab_kg_qa_en.query_frame_runtime import (
    FourDimensionStructureClassifier,
    FrameSkeleton,
    FrameSkeletonRegistry,
    MacroFrameSelector,
)


class QueryFrameSlotOrchestrator(_GraphBaseOrchestrator):
    """True structure-first query-frame pipeline.

    Runtime chain:
    1. macro frame family selection
    2. four-dimension structure classification
    3. skeleton selection
    4. slot filling + Cypher instantiation + execution
    """

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
    ):
        super().__init__(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        self.skeleton_registry = FrameSkeletonRegistry(registry)
        self.macro_selector = MacroFrameSelector(llm=llm, temperature=temperature_intent)
        self.structure_classifier = FourDimensionStructureClassifier(llm=llm, temperature=temperature_intent)

    def _select_skeleton(
        self,
        question: str,
        macro_family: str,
        structure_dims: Dict[str, Any],
    ) -> Dict[str, Any]:
        candidates = self.skeleton_registry.by_macro_family(macro_family)
        if not candidates:
            return {
                "skeleton": None,
                "selection_mode": "empty_macro_family",
                "candidate_count": 0,
            }

        selected_signature = str(structure_dims.get("selected_signature") or "").strip()
        if selected_signature:
            for skeleton in candidates:
                if skeleton.signature == selected_signature:
                    return {
                        "skeleton": skeleton,
                        "selection_mode": "signature_match",
                        "candidate_count": len(candidates),
                    }

        exact = self.skeleton_registry.find_exact_signature(
            macro_family=macro_family,
            constraint_signature=str(structure_dims.get("constraint_signature") or ""),
            operation_type=str(structure_dims.get("operation_type") or ""),
            output_type=str(structure_dims.get("output_type") or ""),
            time_scope=str(structure_dims.get("time_scope") or ""),
        )
        if exact:
            return {
                "skeleton": exact,
                "selection_mode": "exact_signature_lookup",
                "candidate_count": len(candidates),
            }

        def score(sk: FrameSkeleton) -> float:
            value = 0.0
            if sk.constraint_signature == structure_dims.get("constraint_signature"):
                value += 30.0
            if sk.operation_type == structure_dims.get("operation_type"):
                value += 40.0
            if sk.output_type == structure_dims.get("output_type"):
                value += 20.0
            if sk.time_scope == structure_dims.get("time_scope"):
                value += 10.0
            q_tokens = set(self._intent_route_tokens(question))
            c_tokens = set(self._intent_route_tokens(" ".join([sk.description] + sk.examples[:3])))
            value += min(10.0, float(len(q_tokens & c_tokens) * 2.0))
            return value

        ranked = sorted(((score(sk), sk) for sk in candidates), key=lambda item: (-item[0], item[1].name))
        return {
            "skeleton": ranked[0][1],
            "selection_mode": "best_candidate_fallback",
            "candidate_count": len(candidates),
            "candidate_scores": [
                {"skeleton": sk.name, "signature": sk.signature, "score": round(s, 2)}
                for s, sk in ranked[:5]
            ],
        }

    def _materialize_cypher(self, cypher: str, params: Dict[str, Any]) -> str:
        rendered = cypher or ""
        for key in sorted(params.keys(), key=len, reverse=True):
            value = params[key]
            if value is None:
                replacement = "null"
            elif isinstance(value, bool):
                replacement = "true" if value else "false"
            elif isinstance(value, (int, float)):
                replacement = str(value)
            else:
                text = str(value).replace("\\", "\\\\").replace("'", "\\'")
                replacement = f"'{text}'"
            rendered = rendered.replace(f"${key}", replacement)
        return rendered

    def _run_skeleton_query(self, skeleton: Optional[FrameSkeleton], question: str) -> Dict[str, Any]:
        extraction_debug: Dict[str, Any] = {"hits": {}, "missing": []}
        if not skeleton:
            return {
                "skeleton": None,
                "question": question,
                "params": {},
                "cypher": None,
                "rendered_cypher": None,
                "rows": 0,
                "graph_results": [],
                "entity_extraction": extraction_debug,
            }

        params: Dict[str, Any] = {}
        start_year, end_year = self._extract_year_range(question)
        for key, schema in skeleton.slots_schema.items():
            if key == "start_year" and start_year is not None:
                params[key] = start_year
                extraction_debug["hits"][key] = {"value": start_year, "matched_by": "regex_year_range", "score": 1.0, "category": "numeric"}
                continue
            if key == "end_year" and end_year is not None:
                params[key] = end_year
                extraction_debug["hits"][key] = {"value": end_year, "matched_by": "regex_year_range", "score": 1.0, "category": "numeric"}
                continue

            if key == "tp_name":
                tp = self.tp_resolver.resolve(question).tp_name
                if not tp:
                    val, match = self._extract_entity_param(question, key)
                    tp = self._normalize_tp_to_slash(val or "") if val else None
                    if not tp and val:
                        tp = val
                    if match:
                        extraction_debug["hits"][key] = {
                            "value": match.value,
                            "score": round(match.score, 4),
                            "matched_by": match.matched_by,
                            "category": match.category,
                        }
                if tp:
                    params[key] = tp
                    continue

            num = self._extract_numeric_param(question, key)
            if num is not None:
                params[key] = num
                extraction_debug["hits"][key] = {"value": num, "matched_by": "regex_numeric", "score": 1.0, "category": "numeric"}
                continue

            val, match = self._extract_entity_param(question, key)
            if val is not None:
                params[key] = val
                if match:
                    extraction_debug["hits"][key] = {
                        "value": match.value,
                        "score": round(match.score, 4),
                        "matched_by": match.matched_by,
                        "category": match.category,
                    }
                continue

            if schema.get("required", False):
                extraction_debug["missing"].append(key)
                return {
                    "skeleton": skeleton.name,
                    "question": question,
                    "params": params,
                    "cypher": skeleton.cypher_template,
                    "rendered_cypher": self._materialize_cypher(skeleton.cypher_template, params),
                    "rows": 0,
                    "graph_results": [],
                    "missing": key,
                    "entity_extraction": extraction_debug,
                }

            if key == "years":
                params[key] = 3
            elif key == "min_count":
                params[key] = 50
            else:
                params[key] = None

        graph_results = self.runner.run(skeleton.cypher_template, params)
        return {
            "skeleton": skeleton.name,
            "question": question,
            "params": params,
            "cypher": skeleton.cypher_template,
            "rendered_cypher": self._materialize_cypher(skeleton.cypher_template, params),
            "rows": len(graph_results),
            "graph_results": graph_results,
            "entity_extraction": extraction_debug,
        }

    def answer(self, question: str, mode: str = "query_frame_slot") -> AnswerBundle:
        macro_info = self.macro_selector.select(question, self.skeleton_registry)
        macro_family = str(macro_info.get("macro_frame_family") or "UNKNOWN")
        structure_dims = self.structure_classifier.classify(question, macro_family, self.skeleton_registry)
        skeleton_info = self._select_skeleton(question, macro_family, structure_dims)
        skeleton = skeleton_info.get("skeleton")

        debug: Dict[str, Any] = {
            "mode": "query_frame_slot",
            "frame": skeleton.name if skeleton else "UNKNOWN",
            "macro_family": macro_family,
            "macro_family_en": macro_info.get("macro_frame_family_en"),
            "macro_family_zh": macro_info.get("macro_frame_family_zh"),
            "macro_selector": macro_info,
            "structure_classifier": structure_dims,
            "structure_dims": {
                "constraint_signature": structure_dims.get("constraint_signature"),
                "operation_type": structure_dims.get("operation_type"),
                "output_type": structure_dims.get("output_type"),
                "time_scope": structure_dims.get("time_scope"),
            },
            "skeleton_selector": {
                k: v for k, v in skeleton_info.items() if k != "skeleton"
            },
            "skeleton_name": skeleton.name if skeleton else "",
            "skeleton_signature": skeleton.signature if skeleton else "",
        }

        result = self._run_skeleton_query(skeleton, question)
        debug.update(
            {
                "params": result.get("params"),
                "entity_extraction": result.get("entity_extraction"),
                "cypher": result.get("cypher"),
                "rendered_cypher": result.get("rendered_cypher"),
                "rows": result.get("rows"),
                "graph_results": result.get("graph_results"),
            }
        )

        if result.get("missing"):
            ans = self._clarify_missing(str(result["missing"]), skeleton.name if skeleton else macro_family)
            return AnswerBundle(mode="query_frame_slot", answer=ans, debug=debug)

        if result.get("cypher"):
            answer_label = skeleton.name if skeleton else macro_family
            ans = self.synth.answer_with_graph(question, answer_label, result.get("graph_results") or [])
            return AnswerBundle(mode="query_frame_slot", answer=ans, debug=debug)

        return AnswerBundle(
            mode="query_frame_slot",
            answer="Unable to map the question to a supported query-frame skeleton.",
            debug=debug,
        )
