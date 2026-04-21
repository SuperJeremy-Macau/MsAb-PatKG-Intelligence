from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List

import pandas as pd


@dataclass(frozen=True)
class StructureAnnotation:
    frame_structure_class: str
    macro_frame_family: str
    macro_frame_family_en: str
    macro_frame_family_zh: str
    frame_constraint_signature: str
    frame_operation_type: str
    frame_output_type: str
    frame_time_scope: str
    frame_structure_label_en: str
    frame_structure_label_zh: str


_TIME_LABEL_EN = {
    "all_time": "all-time",
    "year_2024": "year-2024",
    "last_3y": "last-3y",
    "last_5y": "last-5y",
}

_TIME_LABEL_ZH = {
    "all_time": "全时段",
    "year_2024": "2024年",
    "last_3y": "近三年",
    "last_5y": "近五年",
}

_CONSTRAINT_LABEL_EN = {
    "none": "global scope",
    "function": "functional category constrained",
    "pathway": "pathway constrained",
    "technologyclass1": "technologyclass1 constrained",
    "target": "target constrained",
    "target_pair": "target-pair constrained",
    "cancer": "cancer constrained",
    "origin": "origin constrained",
    "origin+target": "origin + target constrained",
    "origin+function": "origin + function constrained",
    "origin+technologyclass1": "origin + technologyclass1 constrained",
    "cancer+double_high_expression": "cancer + double-high-expression constrained",
    "derived_top_assignee_targetpair_set": "derived top-assignee target-pair set",
    "derived_new_entrant_set": "derived new-entrant set",
    "derived_emerging_targetpair_set": "derived emerging target-pair set",
}

_CONSTRAINT_LABEL_ZH = {
    "none": "全局范围",
    "function": "功能类别约束",
    "pathway": "通路约束",
    "technologyclass1": "TechnologyClass1约束",
    "target": "靶点约束",
    "target_pair": "靶点对约束",
    "cancer": "癌种约束",
    "origin": "来源地区约束",
    "origin+target": "来源地区+靶点联合约束",
    "origin+function": "来源地区+功能类别联合约束",
    "origin+technologyclass1": "来源地区+TechnologyClass1联合约束",
    "cancer+double_high_expression": "癌种+双高表达组合约束",
    "derived_top_assignee_targetpair_set": "由高活跃企业组合派生的约束集",
    "derived_new_entrant_set": "由新进入者集合派生的约束集",
    "derived_emerging_targetpair_set": "由新兴靶点对集合派生的约束集",
}

_OP_LABEL_EN = {
    "member_lookup": "member lookup",
    "existence": "existence check",
    "year_lookup": "year lookup",
    "publication_detail_lookup": "publication/detail lookup",
    "first_discloser": "first-discloser lookup",
    "first_disclosure_detail": "first-disclosure detail lookup",
    "rank_by_patent_count": "rank by patent count",
    "rank_by_family_count": "rank by family count",
    "rank_recent_activity": "rank by recent activity",
    "rank_by_cagr": "rank by CAGR",
    "rank_by_growth": "rank by growth",
    "rank_by_diversity": "rank by diversity",
    "rank_by_yoy": "rank by YoY change",
    "emerging_targetpair_lookup": "emerging target-pair lookup",
    "new_entrant_lookup": "new-entrant lookup",
    "combination_profile": "combination profile lookup",
    "frequency_profile": "frequency profile lookup",
}

_OP_LABEL_ZH = {
    "member_lookup": "成员列表查询",
    "existence": "存在性判断",
    "year_lookup": "年份查询",
    "publication_detail_lookup": "专利明细查询",
    "first_discloser": "最早披露者查询",
    "first_disclosure_detail": "首次披露详情查询",
    "rank_by_patent_count": "按专利数排名",
    "rank_by_family_count": "按家族数排名",
    "rank_recent_activity": "按近期活跃度排名",
    "rank_by_cagr": "按复合增长率排名",
    "rank_by_growth": "按增长量排名",
    "rank_by_diversity": "按多样性排名",
    "rank_by_yoy": "按同比变化排名",
    "emerging_targetpair_lookup": "新兴靶点对查询",
    "new_entrant_lookup": "新进入者查询",
    "combination_profile": "组合画像查询",
    "frequency_profile": "频次画像查询",
}

_OUTPUT_LABEL_EN = {
    "assignee": "assignee output",
    "target_pair": "target-pair output",
    "target": "target output",
    "pathway": "pathway output",
    "function": "function output",
    "origin": "origin output",
    "detail_record": "detail-record output",
    "publication_record": "publication-record output",
    "year": "year output",
    "boolean": "boolean output",
    "other": "other output",
}

_OUTPUT_LABEL_ZH = {
    "assignee": "企业/机构输出",
    "target_pair": "靶点对输出",
    "target": "靶点输出",
    "pathway": "通路输出",
    "function": "功能类别输出",
    "origin": "来源地区输出",
    "detail_record": "详情记录输出",
    "publication_record": "公开号记录输出",
    "year": "年份输出",
    "boolean": "布尔输出",
    "other": "其他输出",
}

_MACRO_FAMILY_EN = {
    "MF01_constrained_targetpair_discovery": "Constrained target-pair discovery",
    "MF02_targetpair_ranking_by_patent_volume": "Target-pair ranking by patent volume",
    "MF03_targetpair_ranking_by_family_volume": "Target-pair ranking by family volume",
    "MF04_emerging_targetpair_identification": "Emerging target-pair identification",
    "MF05_constrained_assignee_discovery": "Constrained assignee discovery",
    "MF06_assignee_first_discloser_identification": "Assignee first-discloser identification",
    "MF07_assignee_ranking_by_patent_volume": "Assignee ranking by patent volume",
    "MF08_assignee_ranking_by_family_volume": "Assignee ranking by family volume",
    "MF09_recent_assignee_activity_ranking": "Recent assignee activity ranking",
    "MF10_new_entrant_identification": "New entrant identification",
    "MF11_targetpair_combination_profiling": "Target-pair combination profiling",
    "MF12_function_or_pathway_frequency_profiling": "Function/pathway frequency profiling",
    "MF13_origin_level_portfolio_analytics": "Origin-level portfolio analytics",
    "MF14_first_disclosure_detail_lookup": "First-disclosure detail lookup",
    "MF15_publication_and_year_lookup": "Publication/year lookup",
    "MF16_existence_and_boolean_check": "Existence and boolean check",
}

_MACRO_FAMILY_ZH = {
    "MF01_constrained_targetpair_discovery": "受约束靶点对发现",
    "MF02_targetpair_ranking_by_patent_volume": "按专利量排序的靶点对排名",
    "MF03_targetpair_ranking_by_family_volume": "按家族量排序的靶点对排名",
    "MF04_emerging_targetpair_identification": "新兴靶点对识别",
    "MF05_constrained_assignee_discovery": "受约束企业发现",
    "MF06_assignee_first_discloser_identification": "最早披露企业识别",
    "MF07_assignee_ranking_by_patent_volume": "按专利量排序的企业排名",
    "MF08_assignee_ranking_by_family_volume": "按家族量排序的企业排名",
    "MF09_recent_assignee_activity_ranking": "近期企业活跃度排名",
    "MF10_new_entrant_identification": "新进入者识别",
    "MF11_targetpair_combination_profiling": "靶点对组合画像",
    "MF12_function_or_pathway_frequency_profiling": "功能/通路频次画像",
    "MF13_origin_level_portfolio_analytics": "来源地区层面的组合分析",
    "MF14_first_disclosure_detail_lookup": "首次披露详情查询",
    "MF15_publication_and_year_lookup": "公开号与年份查询",
    "MF16_existence_and_boolean_check": "存在性与布尔判断",
}


def _norm(text: Any) -> str:
    return str(text or "").strip()


def infer_time_scope(intent: str, question: str) -> str:
    s = f"{intent} {_norm(question)}".lower()
    if "2024" in s:
        return "year_2024"
    if any(k in s for k in ["last three years", "last 3 years", "most recent three-year", "most recent three year", "recent three-year", "recent three year"]) or "_3y" in intent.lower():
        return "last_3y"
    if any(k in s for k in ["five-year", "5-year", "last five years", "last 5 years"]) or "5y" in intent.lower():
        return "last_5y"
    return "all_time"


def infer_operation(intent: str, question: str) -> str:
    s = _norm(intent).upper()
    q = _norm(question).lower()

    if "EXISTS" in s or q.startswith("does ") or q.startswith("are there any"):
        return "existence"
    if any(k in q for k in ["most patent families", "largest number of patent families", "most families", "top 10 by family count"]):
        return "rank_by_family_count"
    if any(k in q for k in ["most published patents", "most patents", "filed the most patents", "published the most patents", "top 10 by patent count"]):
        return "rank_by_patent_count"
    if "FIRST_DISCLOSURE_DETAILS" in s or ("first disclosure year" in q and "publication number" in q):
        return "first_disclosure_detail"
    if s.startswith("FIRST_") or any(k in q for k in ["earliest discloser", "disclosed them first", "first disclosed", "earliest disclosed"]):
        return "first_discloser"
    if "NEW_TARGETPAIRS" in s:
        return "emerging_targetpair_lookup"
    if "NEW_ENTRANTS" in s or "new entrants" in q or "first entered" in q:
        return "new_entrant_lookup"
    if "TOP_ORIGIN_BY_CAGR" in s:
        return "rank_by_cagr"
    if "TOP_ORIGIN_BY_GROWTH" in s or "growth" in q:
        return "rank_by_growth"
    if "TOP_ORIGIN_BY_DIVERSITY" in s or "diversity" in q:
        return "rank_by_diversity"
    if "TOP_ORIGIN_BY_YOY" in s or "year-on-year" in q or "yoy" in q:
        return "rank_by_yoy"
    if "_3Y" in s and s.startswith("TOP_"):
        return "rank_recent_activity"
    if "PATENT_COUNT" in s and s.startswith("TOP_"):
        return "rank_by_patent_count"
    if "FAMILY_COUNT" in s and s.startswith("TOP_"):
        return "rank_by_family_count"
    if "PUBLICATION" in s or "PATENTS_FOR_" in s:
        return "publication_detail_lookup"
    if "YEARS" in s or "in which years" in q:
        return "year_lookup"
    if "COMBINATIONS" in s:
        return "combination_profile"
    if s.startswith("TOP_PATHWAYS") or s.startswith("TOP_FUNCTIONS") or "appear most often" in q or "appear most frequently" in q:
        return "frequency_profile"
    return "member_lookup"


def infer_output_type(intent: str, question: str) -> str:
    q = _norm(question).lower()
    s = _norm(intent).upper()

    if q.startswith("does ") or q.startswith("are there any") or "exists" in q:
        return "boolean"
    if any(k in q for k in ["publication number", "publication numbers", "pub_no", "first disclosure year"]) and any(k in q for k in ["assignee", "company", "organizations or individuals"]):
        return "detail_record"
    if any(k in q for k in ["publication number", "publication numbers", "patent publication numbers"]):
        return "publication_record"
    if any(k in q for k in ["which company", "which companies", "assignee", "who filed", "organizations or individuals"]):
        return "assignee"
    if any(k in q for k in ["which target pairs", "what target pairs", "target-pair combinations", "exact target pairs", "specific target-pair combinations"]):
        return "target_pair"
    if any(k in q for k in ["which targets", "what targets"]):
        return "target"
    if any(k in q for k in ["which pathways", "what pathways", "pathway combinations"]):
        return "pathway"
    if any(k in q for k in ["which functional", "what functional", "functional target pairings", "functional-of-target combinations", "functional-of-target categories"]):
        return "function"
    if any(k in q for k in ["which origin", "what origin", "origin group"]):
        return "origin"
    if "in which years" in q or "which years" in q:
        return "year"

    if s.startswith("TOP_PATHWAYS") or "PATHWAY_COMBINATIONS" in s:
        return "pathway"
    if s.startswith("TOP_FUNCTIONS") or "FUNCTION_COMBINATIONS" in s:
        return "function"
    if s.startswith("TOP_ORIGIN") or "ORIGIN_DIVERSITY_RANKING" in s:
        return "origin"
    if "ASSIGNEE" in s or "COMPANY" in s:
        return "assignee"
    if "TARGETPAIR" in s or "TARGETPAIRS" in s or "TARGET_COMBINATIONS" in s:
        return "target_pair"
    if "TARGETS" in s:
        return "target"
    return "other"


def infer_constraint_signature(intent: str, question: str) -> str:
    s = _norm(intent).upper()
    q = _norm(question).lower()

    if "BY_ORIGIN_TARGET" in s or "ORIGIN_TARGET" in s:
        return "origin+target"
    if "BY_ORIGIN_FUNCTION" in s or "ORIGIN_FUNCTION" in s:
        return "origin+function"
    if "BY_ORIGIN_TECHCLASS1" in s or "ORIGIN_TECHCLASS1" in s:
        return "origin+technologyclass1"
    if "DOUBLE_HIGH_EXPRESSION" in s:
        return "cancer+double_high_expression"
    if "FOR_TOP_ASSIGNEE_TARGETPAIRS" in s:
        return "derived_top_assignee_targetpair_set"
    if "TARGETPAIRS_FOR_NEW_ENTRANTS" in s or "PATENT_PUBLICATIONS_FOR_NEW_ENTRANTS" in s or s == "NEW_ENTRANTS_2024":
        return "derived_new_entrant_set"
    if "TOP_ASSIGNEES_BY_NEW_TARGETPAIRS" in s or "NEW_TARGETPAIRS_FOR_TOP_ASSIGNEES" in s:
        return "derived_emerging_targetpair_set"

    direct: List[str] = []
    if "FUNCTION" in s:
        direct.append("function")
    if "PATHWAY" in s:
        direct.append("pathway")
    if "TECHCLASS1" in s:
        direct.append("technologyclass1")
    if "CANCER" in s or "HIGH_EXPRESSION" in s:
        direct.append("cancer")
    if "ORIGIN" in s:
        direct.append("origin")

    # Only treat target-pair / target as constraints when they are not simply outputs.
    if ("_BY_TARGETPAIR" in s or "_FOR_TARGETPAIR" in s or "TARGETPAIR_EXISTS" in s or "PATENT_EXISTS_BY_TARGETPAIR" in s or "FIRST_DISCLOSURE_YEAR_BY_TARGETPAIR" in s):
        direct.append("target_pair")
    if any(tag in s for tag in ["_BY_TARGET", "_FOR_TARGET", "ASSIGNEES_BY_TARGET", "PATENT_PUBLICATIONS_BY_TARGET", "PATENT_APPLICATION_YEARS_BY_TARGET", "TOP_ASSIGNEE_BY_TARGET", "TARGETPAIRS_BY_TARGET_"]):
        direct.append("target")

    if not direct:
        if "category" in q or "functional" in q:
            direct.append("function")
        elif "pathway" in q:
            direct.append("pathway")
        elif "technology class" in q or "technologyclass" in q:
            direct.append("technologyclass1")
        elif "origin" in q or "country" in q or "region" in q:
            direct.append("origin")
        elif "highly expressed" in q or "cancer" in q:
            direct.append("cancer")
        elif "target pair" in q and "/" in q:
            direct.append("target_pair")
        elif "involving " in q or "containing " in q:
            direct.append("target")

    if not direct:
        return "none"
    direct = sorted(set(direct))
    return "+".join(direct)


def make_annotation(intent: str, question: str) -> StructureAnnotation:
    constraint = infer_constraint_signature(intent, question)
    operation = infer_operation(intent, question)
    output = infer_output_type(intent, question)
    time_scope = infer_time_scope(intent, question)
    structure_class = f"{constraint}__{operation}__{output}__{time_scope}"
    macro_family = infer_macro_frame_family(constraint, operation, output, time_scope)
    label_en = f"{_CONSTRAINT_LABEL_EN.get(constraint, constraint)} + {_OP_LABEL_EN.get(operation, operation)} + {_OUTPUT_LABEL_EN.get(output, output)} + {_TIME_LABEL_EN.get(time_scope, time_scope)}"
    label_zh = f"{_CONSTRAINT_LABEL_ZH.get(constraint, constraint)} + {_OP_LABEL_ZH.get(operation, operation)} + {_OUTPUT_LABEL_ZH.get(output, output)} + {_TIME_LABEL_ZH.get(time_scope, time_scope)}"
    return StructureAnnotation(
        frame_structure_class=structure_class,
        macro_frame_family=macro_family,
        macro_frame_family_en=_MACRO_FAMILY_EN[macro_family],
        macro_frame_family_zh=_MACRO_FAMILY_ZH[macro_family],
        frame_constraint_signature=constraint,
        frame_operation_type=operation,
        frame_output_type=output,
        frame_time_scope=time_scope,
        frame_structure_label_en=label_en,
        frame_structure_label_zh=label_zh,
    )


def infer_macro_frame_family(constraint: str, operation: str, output: str, time_scope: str) -> str:
    if operation == "existence" or output == "boolean":
        return "MF16_existence_and_boolean_check"
    if operation in {"year_lookup", "publication_detail_lookup"}:
        return "MF15_publication_and_year_lookup"
    if operation == "first_disclosure_detail":
        return "MF14_first_disclosure_detail_lookup"
    if constraint.startswith("origin") and operation in {"rank_by_cagr", "rank_by_growth", "rank_by_diversity", "rank_by_yoy", "first_discloser", "first_disclosure_detail", "combination_profile"}:
        return "MF13_origin_level_portfolio_analytics"
    if operation == "frequency_profile":
        return "MF12_function_or_pathway_frequency_profiling"
    if operation == "combination_profile":
        return "MF11_targetpair_combination_profiling"
    if operation == "new_entrant_lookup":
        return "MF10_new_entrant_identification"
    if operation == "rank_recent_activity":
        return "MF09_recent_assignee_activity_ranking"
    if operation == "rank_by_family_count" and output == "assignee":
        return "MF08_assignee_ranking_by_family_volume"
    if operation == "rank_by_patent_count" and output == "assignee":
        return "MF07_assignee_ranking_by_patent_volume"
    if operation == "first_discloser" and output == "assignee":
        return "MF06_assignee_first_discloser_identification"
    if output == "assignee" and operation == "member_lookup":
        return "MF05_constrained_assignee_discovery"
    if operation == "emerging_targetpair_lookup":
        return "MF04_emerging_targetpair_identification"
    if operation == "rank_by_family_count" and output == "target_pair":
        return "MF03_targetpair_ranking_by_family_volume"
    if operation == "rank_by_patent_count" and output == "target_pair":
        return "MF02_targetpair_ranking_by_patent_volume"
    if output == "target_pair" and operation == "member_lookup":
        return "MF01_constrained_targetpair_discovery"
    # fallbacks
    if output == "assignee":
        return "MF05_constrained_assignee_discovery"
    if output == "target_pair":
        return "MF01_constrained_targetpair_discovery"
    return "MF11_targetpair_combination_profiling"


def annotate_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ann = make_annotation(_norm(row.get("intent_candidate")), _norm(row.get("question_en")))
    return asdict(ann)


def annotate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        payload = row.to_dict()
        payload.update(annotate_row(payload))
        rows.append(payload)
    return pd.DataFrame(rows)


def build_appendix_summary(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "macro_frame_family",
        "macro_frame_family_en",
        "macro_frame_family_zh",
        "frame_structure_class",
        "frame_constraint_signature",
        "frame_operation_type",
        "frame_output_type",
        "frame_time_scope",
        "frame_structure_label_en",
        "frame_structure_label_zh",
    ]
    agg = (
        df.groupby(base_cols, dropna=False)
        .agg(
            question_count=("id", "count"),
            scenario_codes=("scenario_code", lambda s: ", ".join(sorted({str(x) for x in s if str(x).strip()}))),
            example_intents=("intent_candidate", lambda s: " | ".join(sorted({str(x) for x in s if str(x).strip()})[:3])),
            example_question=("question_en", "first"),
        )
        .reset_index()
        .sort_values(["question_count", "frame_structure_class"], ascending=[False, True])
    )
    return agg


def build_macro_family_summary(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby(["macro_frame_family", "macro_frame_family_en", "macro_frame_family_zh"], dropna=False)
        .agg(
            question_count=("id", "count"),
            unique_fine_grained_classes=("frame_structure_class", "nunique"),
            scenario_codes=("scenario_code", lambda s: ", ".join(sorted({str(x) for x in s if str(x).strip()}))),
            example_intents=("intent_candidate", lambda s: " | ".join(sorted({str(x) for x in s if str(x).strip()})[:5])),
            example_question=("question_en", "first"),
        )
        .reset_index()
        .sort_values(["question_count", "macro_frame_family"], ascending=[False, True])
    )
    return agg


def build_dimension_method_table() -> pd.DataFrame:
    rows = [
        {
            "dimension": "macro_frame_family",
            "definition_en": "A manuscript-level macro family obtained by merging fine-grained frame structures that share the same business operation pattern.",
            "definition_zh": "将细粒度结构类按相近业务操作模式压缩后的正文级宏观结构家族。",
            "examples": "MF01_constrained_targetpair_discovery, MF07_assignee_ranking_by_patent_volume",
        },
        {
            "dimension": "frame_constraint_signature",
            "definition_en": "What semantic constraint defines the search space.",
            "definition_zh": "定义查询搜索空间的语义约束维度。",
            "examples": "function, pathway, technologyclass1, origin+target",
        },
        {
            "dimension": "frame_operation_type",
            "definition_en": "What analytical action the query performs.",
            "definition_zh": "查询执行的分析操作类型。",
            "examples": "member_lookup, rank_by_patent_count, first_discloser, new_entrant_lookup",
        },
        {
            "dimension": "frame_output_type",
            "definition_en": "What kind of object the answer is expected to return.",
            "definition_zh": "答案主要返回哪一类对象。",
            "examples": "assignee, target_pair, pathway, detail_record",
        },
        {
            "dimension": "frame_time_scope",
            "definition_en": "What temporal window constrains the analysis.",
            "definition_zh": "分析所受的时间窗口约束。",
            "examples": "all_time, year_2024, last_3y, last_5y",
        },
        {
            "dimension": "frame_structure_class",
            "definition_en": "A fine-grained operational structure composed of constraint + operation + output + time scope.",
            "definition_zh": "由约束维度 + 操作类型 + 输出对象 + 时间范围组成的细粒度结构类。",
            "examples": "function__rank_by_patent_count__target_pair__all_time",
        },
    ]
    return pd.DataFrame(rows)
