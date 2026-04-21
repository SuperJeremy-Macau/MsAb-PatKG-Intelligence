from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from bsab_kg_qa_en.intents.registry import IntentDef, IntentRegistry


@dataclass(frozen=True)
class FrameDef:
    name: str
    description: str
    slots_schema: Dict[str, Any]
    cypher_template: str
    result_schema: Dict[str, str]
    examples: List[str]
    metadata: Dict[str, Any]
    source_intent: str


class FrameRegistry:
    """Independent query-frame view derived from intent definitions.

    Frames are first-class runtime objects for the query-frame orchestrator:
    - frame name
    - slot schema
    - frame-level metadata for structure selection
    - executable Cypher template

    The current implementation bootstraps frames from existing intent definitions
    so the system can move to a structure-first runtime without requiring a full
    manual rewrite of every template file.
    """

    def __init__(self, intent_registry: IntentRegistry):
        self.intent_registry = intent_registry
        self._frames: Dict[str, FrameDef] = {}
        self._load_from_intents()

    def _load_from_intents(self) -> None:
        for idef in self.intent_registry.list(only_show=False):
            frame_name = str((idef.ui or {}).get("frame_name") or "").strip()
            if not frame_name:
                continue
            fdef = self._intent_to_frame(idef, frame_name)
            self._frames[fdef.name] = fdef

    def _intent_to_frame(self, idef: IntentDef, frame_name: str) -> FrameDef:
        return FrameDef(
            name=frame_name,
            description=idef.description or "",
            slots_schema=dict(idef.params_schema or {}),
            cypher_template=idef.cypher or "",
            result_schema=dict(idef.result_schema or {}),
            examples=list((idef.ui or {}).get("examples", []) or []),
            metadata=self._infer_frame_metadata(frame_name, idef),
            source_intent=idef.name,
        )

    def _infer_frame_metadata(self, frame_name: str, idef: IntentDef) -> Dict[str, Any]:
        tokens = set(frame_name.upper().split("_"))
        name = idef.name.upper()
        params = {str(k).lower() for k in (idef.params_schema or {}).keys()}

        output_axis = "unknown"
        if {"ASSIGNEE", "ASSIGNEES", "COMPANY", "COMPANIES"} & tokens:
            output_axis = "assignee"
        elif {"TARGETPAIRS", "TARGETPAIR", "TARGET", "TARGETS"} & tokens and ("PAIR" in tokens or "PAIRS" in tokens or "TARGETPAIR" in tokens or "TARGETPAIRS" in tokens):
            output_axis = "target_pair"
        elif {"PATHWAY", "PATHWAYS"} & tokens or "PATHWAYS" in name:
            output_axis = "pathway"
        elif {"FUNCTION", "FUNCTIONS"} & tokens or "FUNCTIONS" in name:
            output_axis = "function"
        elif {"ORIGIN", "ORIGINS"} & tokens:
            output_axis = "origin"
        elif {"PATENT", "PATENTS", "PUBLICATION", "PUBLICATIONS"} & tokens:
            output_axis = "patent"

        constraint_axis = "none"
        if "FUNCTION" in tokens or "FUNCTION" in name or "functional_of_target" in params:
            constraint_axis = "function"
        elif "PATHWAY" in tokens or "PATHWAY" in name or "pathway" in params:
            constraint_axis = "pathway"
        elif "TECHNOLOGY" in tokens or "TECHNOLOGYCLASS1" in tokens or "technologyclass1" in params:
            constraint_axis = "technologyclass1"
        elif "CANCER" in tokens or "cancer" in params:
            constraint_axis = "cancer"
        elif "ORIGIN" in tokens or "origin" in params:
            constraint_axis = "origin"
        elif "TARGETPAIR" in tokens or "tp_name" in params:
            constraint_axis = "target_pair"
        elif "TARGET" in tokens or any(k in params for k in {"target", "target1", "target2"}):
            constraint_axis = "target"

        operation = "lookup"
        if "FIRST" in tokens:
            operation = "first"
        elif "TOP" in tokens and "PATENT" in tokens and "COUNT" in tokens:
            operation = "top_patent_count"
        elif "TOP" in tokens and "FAMILY" in tokens and "COUNT" in tokens:
            operation = "top_family_count"
        elif "NEW" in tokens and "ENTRANTS" in tokens:
            operation = "new_entrant"
        elif "EXISTS" in tokens:
            operation = "existence"
        elif "YEARS" in tokens:
            operation = "year_list"
        elif "COUNT" in tokens:
            operation = "count"
        elif "COMBINATIONS" in tokens:
            operation = "combination_list"

        time_scope = "all_time"
        if "2024" in name or "2024" in frame_name:
            time_scope = "year_2024"
        elif any(tag in name for tag in ["_3Y", "LAST_3Y", "RECENT_YEARS", "RECENT_WINDOW"]):
            time_scope = "last_3y"
        elif "5Y" in name:
            time_scope = "last_5y"

        return {
            "output_axis": output_axis,
            "constraint_axis": constraint_axis,
            "operation": operation,
            "time_scope": time_scope,
            "slot_names": list((idef.params_schema or {}).keys()),
        }

    def get(self, frame_name: str) -> Optional[FrameDef]:
        return self._frames.get(frame_name)

    def list(self) -> List[FrameDef]:
        return list(self._frames.values())
