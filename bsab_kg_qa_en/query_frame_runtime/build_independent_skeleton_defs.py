from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bsab_kg_qa_en.frame_structure_taxonomy.taxonomy import (
    _MACRO_FAMILY_EN,
    _MACRO_FAMILY_ZH,
    infer_macro_frame_family,
)
from bsab_kg_qa_en.intents.registry import IntentRegistry

INTENT_DIR = ROOT / "bsab_kg_qa_en" / "intents" / "definitions"
OUTPUT_DIR = Path(__file__).resolve().parent / "definitions"


SELECTED_SPECS: List[Dict[str, str]] = [
    {
        "signature": "function__member_lookup__target_pair__all_time",
        "intent_name": "TARGETPAIRS_BY_FUNCTION",
        "description": "Independent frame skeleton for discovering target pairs constrained by functional category.",
    },
    {
        "signature": "pathway__member_lookup__target_pair__all_time",
        "intent_name": "TARGETPAIRS_BY_PATHWAY",
        "description": "Independent frame skeleton for discovering target pairs constrained by pathway.",
    },
    {
        "signature": "target__member_lookup__target_pair__all_time",
        "intent_name": "TARGETPAIRS_BY_TARGET",
        "description": "Independent frame skeleton for discovering target pairs containing a target.",
    },
    {
        "signature": "cancer__member_lookup__target_pair__all_time",
        "intent_name": "TARGETPAIRS_BY_CANCER",
        "description": "Independent frame skeleton for discovering target pairs under a cancer-expression constraint.",
    },
    {
        "signature": "technologyclass1__member_lookup__target_pair__all_time",
        "intent_name": "TARGETPAIRS_BY_TECHCLASS1",
        "description": "Independent frame skeleton for discovering target pairs constrained by TechnologyClass1.",
    },
    {
        "signature": "function__member_lookup__assignee__all_time",
        "intent_name": "ASSIGNEES_BY_FUNCTION",
        "description": "Independent frame skeleton for discovering assignees constrained by functional category.",
    },
    {
        "signature": "function__member_lookup__assignee__year_2024",
        "intent_name": "ASSIGNEES_BY_FUNCTION_2024",
        "description": "Independent frame skeleton for discovering 2024 assignees constrained by functional category.",
    },
    {
        "signature": "pathway__member_lookup__assignee__year_2024",
        "intent_name": "ASSIGNEES_BY_PATHWAY_2024",
        "description": "Independent frame skeleton for discovering 2024 assignees constrained by pathway.",
    },
    {
        "signature": "target__member_lookup__assignee__all_time",
        "intent_name": "ASSIGNEES_BY_TARGET",
        "description": "Independent frame skeleton for discovering assignees constrained by target.",
    },
    {
        "signature": "technologyclass1__member_lookup__assignee__all_time",
        "intent_name": "ASSIGNEES_BY_TECHCLASS1",
        "description": "Independent frame skeleton for discovering assignees constrained by TechnologyClass1.",
    },
    {
        "signature": "function__rank_by_patent_count__target_pair__all_time",
        "intent_name": "TOP_TARGETPAIRS_BY_FUNCTION_PATENT_COUNT",
        "description": "Independent frame skeleton for ranking target pairs by patent count under a functional constraint.",
    },
    {
        "signature": "function__rank_by_family_count__target_pair__all_time",
        "intent_name": "TOP_TARGETPAIRS_BY_FUNCTION_FAMILY_COUNT",
        "description": "Independent frame skeleton for ranking target pairs by family count under a functional constraint.",
    },
    {
        "signature": "pathway__rank_by_patent_count__target_pair__all_time",
        "intent_name": "TOP_TARGETPAIRS_BY_PATHWAY_PATENT_COUNT",
        "description": "Independent frame skeleton for ranking target pairs by patent count under a pathway constraint.",
    },
    {
        "signature": "pathway__rank_by_family_count__target_pair__all_time",
        "intent_name": "TOP_TARGETPAIRS_BY_PATHWAY_FAMILY_COUNT",
        "description": "Independent frame skeleton for ranking target pairs by family count under a pathway constraint.",
    },
    {
        "signature": "technologyclass1__rank_by_patent_count__target_pair__all_time",
        "intent_name": "TOP_TARGETPAIRS_BY_TECHCLASS1_PATENT_COUNT",
        "description": "Independent frame skeleton for ranking target pairs by patent count under a TechnologyClass1 constraint.",
    },
    {
        "signature": "function__emerging_targetpair_lookup__target_pair__year_2024",
        "intent_name": "NEW_TARGETPAIRS_BY_FUNCTION_2024",
        "description": "Independent frame skeleton for identifying 2024-emerging target pairs under a functional constraint.",
    },
    {
        "signature": "pathway__first_discloser__target_pair__last_3y",
        "intent_name": "NEW_TARGETPAIRS_BY_PATHWAY_3Y",
        "description": "Independent frame skeleton for identifying newly appearing target pairs in the last 3 years under a pathway constraint.",
    },
    {
        "signature": "origin+target__first_disclosure_detail__detail_record__all_time",
        "intent_name": "FIRST_DISCLOSURE_DETAILS_BY_ORIGIN_TARGET",
        "description": "Independent frame skeleton for first-disclosure detail lookup under origin + target constraints.",
    },
    {
        "signature": "origin__rank_by_diversity__origin__all_time",
        "intent_name": "ORIGIN_DIVERSITY_RANKING",
        "description": "Independent frame skeleton for origin-level diversity ranking.",
    },
    {
        "signature": "target+target_pair__existence__boolean__all_time",
        "intent_name": "TARGETPAIR_EXISTS_BY_TARGET",
        "description": "Independent frame skeleton for boolean target-pair existence checks by target.",
    },
]


def build_definition(spec: Dict[str, str], registry: IntentRegistry) -> Dict[str, object]:
    idef = registry.get(spec["intent_name"])
    if not idef:
        raise KeyError(f"Intent not found: {spec['intent_name']}")

    constraint_signature, operation_type, output_type, time_scope = spec["signature"].split("__")
    macro_frame_family = infer_macro_frame_family(
        constraint=constraint_signature,
        operation=operation_type,
        output=output_type,
        time_scope=time_scope,
    )

    return {
        "name": f"SKELETON_{spec['signature'].upper()}",
        "macro_frame_family": macro_frame_family,
        "macro_frame_family_en": _MACRO_FAMILY_EN[macro_frame_family],
        "macro_frame_family_zh": _MACRO_FAMILY_ZH[macro_frame_family],
        "constraint_signature": constraint_signature,
        "operation_type": operation_type,
        "output_type": output_type,
        "time_scope": time_scope,
        "signature": spec["signature"],
        "description": spec["description"],
        "slots_schema": dict(idef.params_schema or {}),
        "cypher_template": idef.cypher or "",
        "result_schema": dict(idef.result_schema or {}),
        "examples": list((idef.ui or {}).get("examples", []) or []),
        "notes": {
            "definition_source": "independent_query_frame_skeleton_v1",
            "seed_intent_name": spec["intent_name"],
        },
    }


def main() -> None:
    registry = IntentRegistry(str(INTENT_DIR))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for old in OUTPUT_DIR.glob("*.json"):
        old.unlink()

    manifest: List[Dict[str, str]] = []
    for spec in SELECTED_SPECS:
        data = build_definition(spec, registry)
        path = OUTPUT_DIR / f"{data['name'].lower()}.json"
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest.append(
            {
                "name": str(data["name"]),
                "signature": str(data["signature"]),
                "path": str(path),
            }
        )

    manifest_path = OUTPUT_DIR / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Generated {len(manifest)} independent skeleton definitions in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
