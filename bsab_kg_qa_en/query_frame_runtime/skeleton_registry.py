from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from bsab_kg_qa_en.frame_structure_taxonomy.taxonomy import make_annotation
from bsab_kg_qa_en.intents.registry import IntentDef, IntentRegistry


@dataclass
class FrameSkeleton:
    name: str
    macro_frame_family: str
    macro_frame_family_en: str
    macro_frame_family_zh: str
    constraint_signature: str
    operation_type: str
    output_type: str
    time_scope: str
    signature: str
    slots_schema: Dict[str, Any]
    cypher_template: str
    result_schema: Dict[str, str]
    source_intents: List[str]
    representative_intent: str
    description: str
    examples: List[str]
    notes: Dict[str, Any]


class FrameSkeletonRegistry:
    """Registry for executable query-frame skeletons.

    The current minimal implementation bootstraps skeletons from existing
    intent definitions, but stores them under structure-first keys:
    macro family -> 4-dimension signature -> executable skeleton.
    """

    def __init__(self, intent_registry: IntentRegistry):
        self.intent_registry = intent_registry
        self._skeletons: Dict[str, FrameSkeleton] = {}
        self._macro_index: Dict[str, List[str]] = {}
        self._definitions_dir = Path(__file__).resolve().parent / "definitions"
        self._load_from_definition_files()
        self._load_from_intents()

    def _load_from_intents(self) -> None:
        for idef in self.intent_registry.list(only_show=False):
            self._register_intent(idef)

        for macro_family, names in self._macro_index.items():
            self._macro_index[macro_family] = sorted(set(names))

    def _load_from_definition_files(self) -> None:
        if not self._definitions_dir.exists():
            return
        for path in sorted(self._definitions_dir.glob("*.json")):
            if path.name.startswith("_"):
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            skeleton = FrameSkeleton(
                name=str(payload["name"]),
                macro_frame_family=str(payload["macro_frame_family"]),
                macro_frame_family_en=str(payload.get("macro_frame_family_en") or payload["macro_frame_family"]),
                macro_frame_family_zh=str(payload.get("macro_frame_family_zh") or payload["macro_frame_family"]),
                constraint_signature=str(payload["constraint_signature"]),
                operation_type=str(payload["operation_type"]),
                output_type=str(payload["output_type"]),
                time_scope=str(payload["time_scope"]),
                signature=str(payload["signature"]),
                slots_schema=dict(payload.get("slots_schema") or {}),
                cypher_template=str(payload.get("cypher_template") or ""),
                result_schema=dict(payload.get("result_schema") or {}),
                source_intents=[],
                representative_intent="",
                description=str(payload.get("description") or ""),
                examples=list(payload.get("examples") or []),
                notes=dict(payload.get("notes") or {}),
            )
            self._skeletons[skeleton.name] = skeleton
            self._macro_index.setdefault(skeleton.macro_frame_family, []).append(skeleton.name)

    def _register_intent(self, idef: IntentDef) -> None:
        question_hint = self._pick_question_hint(idef)
        ann = make_annotation(idef.name, question_hint)
        signature = ann.frame_structure_class
        skeleton_name = f"SKELETON_{signature.upper()}"

        existing = self._skeletons.get(skeleton_name)
        if existing:
            if existing.notes.get("definition_source") == "independent_query_frame_skeleton_v1":
                return
            if idef.name not in existing.source_intents:
                existing.source_intents.append(idef.name)
            for ex in list((idef.ui or {}).get("examples", []) or []):
                if ex and ex not in existing.examples:
                    existing.examples.append(ex)
            return

        skeleton = FrameSkeleton(
            name=skeleton_name,
            macro_frame_family=ann.macro_frame_family,
            macro_frame_family_en=ann.macro_frame_family_en,
            macro_frame_family_zh=ann.macro_frame_family_zh,
            constraint_signature=ann.frame_constraint_signature,
            operation_type=ann.frame_operation_type,
            output_type=ann.frame_output_type,
            time_scope=ann.frame_time_scope,
            signature=signature,
            slots_schema=dict(idef.params_schema or {}),
            cypher_template=idef.cypher or "",
            result_schema=dict(idef.result_schema or {}),
            source_intents=[idef.name],
            representative_intent=idef.name,
            description=idef.description or "",
            examples=list((idef.ui or {}).get("examples", []) or []),
            notes={
                "source_frame_name": str((idef.ui or {}).get("frame_name") or "").strip(),
                "question_hint": question_hint,
            },
        )
        self._skeletons[skeleton_name] = skeleton
        self._macro_index.setdefault(skeleton.macro_frame_family, []).append(skeleton_name)

    @staticmethod
    def _pick_question_hint(idef: IntentDef) -> str:
        examples = list((idef.ui or {}).get("examples", []) or [])
        if examples:
            return str(examples[0] or "")
        return str(idef.description or "")

    def get(self, name: str) -> Optional[FrameSkeleton]:
        return self._skeletons.get(name)

    def list(self) -> List[FrameSkeleton]:
        return list(self._skeletons.values())

    def by_macro_family(self, macro_family: str) -> List[FrameSkeleton]:
        names = self._macro_index.get(macro_family, [])
        return [self._skeletons[name] for name in names if name in self._skeletons]

    def find_exact_signature(
        self,
        macro_family: str,
        constraint_signature: str,
        operation_type: str,
        output_type: str,
        time_scope: str,
    ) -> Optional[FrameSkeleton]:
        signature = f"{constraint_signature}__{operation_type}__{output_type}__{time_scope}"
        for skeleton in self.by_macro_family(macro_family):
            if skeleton.signature == signature:
                return skeleton
        return None

    def macro_family_cards(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for macro_family, names in sorted(self._macro_index.items()):
            skeletons = [self._skeletons[name] for name in names]
            example_signatures = [sk.signature for sk in skeletons[:4]]
            rows.append(
                {
                    "macro_frame_family": macro_family,
                    "macro_frame_family_en": skeletons[0].macro_frame_family_en if skeletons else macro_family,
                    "macro_frame_family_zh": skeletons[0].macro_frame_family_zh if skeletons else macro_family,
                    "skeleton_count": len(skeletons),
                    "example_signatures": example_signatures,
                }
            )
        return rows
