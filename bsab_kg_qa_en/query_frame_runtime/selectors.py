from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional

from bsab_kg_qa_en.frame_structure_taxonomy.taxonomy import (
    infer_constraint_signature,
    infer_macro_frame_family,
    infer_operation,
    infer_output_type,
    infer_time_scope,
)
from bsab_kg_qa_en.query_frame_runtime.skeleton_registry import FrameSkeleton, FrameSkeletonRegistry


def _route_tokens(text: str) -> List[str]:
    stop = {
        "the", "a", "an", "of", "in", "on", "for", "to", "and", "or", "by", "with", "from",
        "that", "which", "what", "who", "whom", "is", "are", "was", "were", "be", "been",
        "this", "these", "those", "their", "there", "into", "across", "among", "within",
        "please", "provide", "return", "show", "list", "all", "any", "do", "does", "did",
        "have", "has", "had", "more", "most", "top", "last", "recent", "recently", "year",
        "years", "pair", "pairs", "target", "targets", "patent", "patents",
    }
    tokens = re.findall(r"[a-z0-9]+", (text or "").lower())
    return [tok for tok in tokens if tok not in stop and len(tok) > 1]


class MacroFrameSelector:
    def __init__(self, llm: Any, temperature: float = 0.0):
        self.llm = llm
        self.temperature = temperature

    def heuristic_select(self, question: str, registry: FrameSkeletonRegistry) -> Optional[str]:
        constraint = infer_constraint_signature("", question)
        operation = infer_operation("", question)
        output_type = infer_output_type("", question)
        time_scope = infer_time_scope("", question)
        macro_family = infer_macro_frame_family(constraint, operation, output_type, time_scope)
        if registry.by_macro_family(macro_family):
            return macro_family
        return None

    def select(self, question: str, registry: FrameSkeletonRegistry) -> Dict[str, Any]:
        heur = self.heuristic_select(question, registry)
        if heur:
            skeletons = registry.by_macro_family(heur)
            return {
                "macro_frame_family": heur,
                "macro_frame_family_en": skeletons[0].macro_frame_family_en if skeletons else heur,
                "macro_frame_family_zh": skeletons[0].macro_frame_family_zh if skeletons else heur,
                "selection_mode": "heuristic_macro_family",
            }

        cards = registry.macro_family_cards()
        if not cards:
            return {
                "macro_frame_family": "UNKNOWN",
                "macro_frame_family_en": "UNKNOWN",
                "macro_frame_family_zh": "UNKNOWN",
                "selection_mode": "empty_registry",
            }

        system_prompt = """
You are selecting a query-frame macro family for a BsAb patent KGQA system.
Choose exactly one macro_frame_family from the candidate list.
Return strict JSON only:
{"macro_frame_family":"..."}
""".strip()
        user_prompt = (
            f"Question:\n{question}\n\n"
            f"Candidate macro families:\n{json.dumps(cards, ensure_ascii=False, indent=2)}"
        )
        chosen = ""
        raw = ""
        try:
            raw = self.llm.chat(system_prompt, user_prompt, temperature=self.temperature)
            raw = re.sub(r"^```json\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE).strip()
            obj = json.loads(raw)
            if isinstance(obj, dict):
                chosen = str(obj.get("macro_frame_family") or "").strip()
        except Exception:
            chosen = ""

        if not chosen or not registry.by_macro_family(chosen):
            chosen = cards[0]["macro_frame_family"]
        skeletons = registry.by_macro_family(chosen)
        return {
            "macro_frame_family": chosen,
            "macro_frame_family_en": skeletons[0].macro_frame_family_en if skeletons else chosen,
            "macro_frame_family_zh": skeletons[0].macro_frame_family_zh if skeletons else chosen,
            "selection_mode": "llm_macro_family_selector",
            "raw_selector_output": raw,
        }


class FourDimensionStructureClassifier:
    def __init__(self, llm: Any, temperature: float = 0.0):
        self.llm = llm
        self.temperature = temperature

    def heuristic_dimensions(self, question: str) -> Dict[str, str]:
        return {
            "constraint_signature": infer_constraint_signature("", question),
            "operation_type": infer_operation("", question),
            "output_type": infer_output_type("", question),
            "time_scope": infer_time_scope("", question),
        }

    def _score_candidate(self, dims: Dict[str, str], skeleton: FrameSkeleton, question: str) -> float:
        score = 0.0
        if dims["constraint_signature"] == skeleton.constraint_signature:
            score += 30.0
        if dims["operation_type"] == skeleton.operation_type:
            score += 40.0
        if dims["output_type"] == skeleton.output_type:
            score += 20.0
        if dims["time_scope"] == skeleton.time_scope:
            score += 10.0

        q_tokens = set(_route_tokens(question))
        c_tokens = set(_route_tokens(" ".join([skeleton.description] + skeleton.examples[:3])))
        score += min(10.0, float(len(q_tokens & c_tokens) * 2.0))
        return score

    def classify(
        self,
        question: str,
        macro_family: str,
        registry: FrameSkeletonRegistry,
    ) -> Dict[str, Any]:
        dims = self.heuristic_dimensions(question)
        candidates = registry.by_macro_family(macro_family)
        if not candidates:
            return {
                **dims,
                "selection_mode": "empty_macro_family",
                "candidate_count": 0,
            }

        exact = registry.find_exact_signature(
            macro_family=macro_family,
            constraint_signature=dims["constraint_signature"],
            operation_type=dims["operation_type"],
            output_type=dims["output_type"],
            time_scope=dims["time_scope"],
        )
        if exact:
            return {
                **dims,
                "selection_mode": "heuristic_exact_signature",
                "candidate_count": len(candidates),
                "selected_signature": exact.signature,
            }

        scored = sorted(
            ((self._score_candidate(dims, sk, question), sk) for sk in candidates),
            key=lambda item: (-item[0], item[1].signature),
        )
        top_score, top = scored[0]
        if top_score >= 60.0:
            return {
                "constraint_signature": top.constraint_signature,
                "operation_type": top.operation_type,
                "output_type": top.output_type,
                "time_scope": top.time_scope,
                "selection_mode": "heuristic_best_signature",
                "candidate_count": len(candidates),
                "selected_signature": top.signature,
            }

        cards = [
            {
                "signature": sk.signature,
                "constraint_signature": sk.constraint_signature,
                "operation_type": sk.operation_type,
                "output_type": sk.output_type,
                "time_scope": sk.time_scope,
                "source_intents": sk.source_intents[:3],
                "examples": sk.examples[:2],
            }
            for sk in candidates[:18]
        ]
        system_prompt = """
You are classifying the question structure for a BsAb patent KGQA system.
Choose the best candidate structure signature from the candidate list.
Return strict JSON only:
{"signature":"..."}
""".strip()
        user_prompt = (
            f"Question:\n{question}\n\n"
            f"Candidate structure signatures:\n{json.dumps(cards, ensure_ascii=False, indent=2)}"
        )
        raw = ""
        chosen = ""
        try:
            raw = self.llm.chat(system_prompt, user_prompt, temperature=self.temperature)
            raw = re.sub(r"^```json\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE).strip()
            obj = json.loads(raw)
            if isinstance(obj, dict):
                chosen = str(obj.get("signature") or "").strip()
        except Exception:
            chosen = ""

        match = next((sk for sk in candidates if sk.signature == chosen), top)
        return {
            "constraint_signature": match.constraint_signature,
            "operation_type": match.operation_type,
            "output_type": match.output_type,
            "time_scope": match.time_scope,
            "selection_mode": "llm_structure_classifier" if chosen else "heuristic_fallback_signature",
            "candidate_count": len(candidates),
            "selected_signature": match.signature,
            "raw_selector_output": raw,
        }
