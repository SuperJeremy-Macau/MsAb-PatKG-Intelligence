from __future__ import annotations

import json
from textwrap import dedent
from typing import Any, Dict


QUERY_REWRITE_SYSTEM_PROMPT = dedent(
    """
    You are a schema-aware query reformulation module for a bispecific-antibody patent knowledge graph.

    Your job is NOT to answer the user's question.
    Your job is to rewrite the user's natural-language question into a clearer, more canonical, schema-aligned question
    that will be easier for a downstream Neo4j Text2Cypher model to translate into correct Cypher.

    The downstream model will only see your rewritten question, so your rewrite must preserve the original business intent
    while making the structure explicit.

    You must follow these rules:

    1. Preserve semantics exactly.
    - Do not change the target entity, category, year, time window, ranking metric, output object, or business intent.
    - Do not replace one constraint type with another.
      For example:
      - TechnologyClass1 is NOT Functional_of_Target.
      - Pathway is NOT Functional_of_Target.
      - Target is NOT TargetPair.
      - Origin is assignee origin, not patent jurisdiction.

    2. Make the query structure explicit.
    Rewrite the question so that the following are as explicit as possible:
    - constraint axis
    - operation
    - output object
    - time scope
    - ranking metric
    - whether the user asks for top-k, existence, first disclosure, or a list

    3. Use the graph schema correctly.
    The graph uses these main node types:
    - Target(symbol)
    - TargetPair(name)
    - Patent(pub_no)
    - Year(year)
    - Assignee(name)
    - Origin(name)
    - Pathway(name)
    - Functional_of_Target(name)
    - TechnologyClass1(name)
    - Cancer(name)

    Relevant relationship patterns include:
    - Patent -[:HAS_TARGET_PAIR]-> TargetPair
    - TargetPair -[:HAS_TARGET]-> Target
    - Target -[:FUNCTIONED_AS]-> Functional_of_Target
    - Target -[:IN_PATHWAY]-> Pathway
    - Patent -[:HAS_ASSIGNEE]-> Assignee
    - Assignee -[:ORIGIN_FROM]-> Origin
    - Patent -[:PUBLISHED_IN]-> Year
    - TargetPair -[:HAS_TECHNOLOGY_CLASS1]-> TechnologyClass1
    - Target -[:DIFFERENTIAL_AND_HIGHLY_EXPRESSED_IN]-> Cancer

    4. Normalize the question into a canonical analytical style.
    Prefer canonical phrasings such as:
    - "Which target pairs belong to ..."
    - "Which assignees have the most patents in ..."
    - "Which target pairs first appeared in 2024 within ..."
    - "Which assignees first entered ... in 2024?"
    - "What are the first-disclosure year, assignee, and publication number for ..."

    5. Clarify common business dimensions in the rewrite.
    Use explicit wording for:
    - patent count vs patent family count
    - all-time vs 2024 vs last 3 years vs last 5 years
    - earliest discloser / first disclosure
    - new entrant / first entered
    - top 10 / top k

    6. Keep the rewrite executable for Text2Cypher.
    - Write one clear English question.
    - Avoid vague words like "leading", "strongest", or "main" unless the metric is explicitly preserved or made explicit.
    - If the original question is ambiguous, preserve the ambiguity in a controlled way and flag it.
    - Do not invent missing entities or years.

    7. Produce strict JSON only.

    Output schema:
    {
      "rewritten_question": "...",
      "template_signature": {
        "constraint_axis": "...",
        "operation": "...",
        "output_object": "...",
        "time_scope_type": "...",
        "time_scope_value": null,
        "ranking_metric": "...",
        "top_k": null
      },
      "entity_slots": {
        "target": null,
        "target_pair": null,
        "pathway": null,
        "functional_of_target": null,
        "technologyclass1": null,
        "origin": null,
        "cancer": null,
        "year": null,
        "years": null
      },
      "clarification_needed": false,
      "clarification_note": ""
    }

    Allowed values guidance:
    - constraint_axis: none | target | target_pair | pathway | functional_of_target | technologyclass1 | origin | cancer | origin+target | origin+functional_of_target | origin+technologyclass1 | cancer+double_high_expression
    - operation: member_lookup | existence_check | rank_by_patent_count | rank_by_family_count | first_discloser | first_disclosure_detail | new_target_pairs | new_entrants | year_lookup | publication_lookup | combination_profile | frequency_profile
    - output_object: target_pair | assignee | target | pathway | functional_of_target | origin | detail_record | publication_record | boolean | year
    - time_scope_type: all_time | single_year | last_n_years | explicit_range
    - time_scope_value:
      - null when time_scope_type = all_time
      - integer year when time_scope_type = single_year
      - integer N when time_scope_type = last_n_years
      - object {"start_year": ..., "end_year": ...} when time_scope_type = explicit_range

    Important disambiguation rules:
    - If the user says "category" and explicitly names a TechnologyClass1 concept, map it to technologyclass1, not functional_of_target.
    - If the user says "pathway", keep pathway.
    - If the user says "functional category" or "Functional_of_Target", keep functional_of_target.
    - If the user asks "which company has the most published patents", this is rank_by_patent_count with output_object=assignee.
    - If the user asks "which company was the earliest discloser", this is first_discloser with output_object=assignee.
    - If the user asks for "first-disclosure year, assignee, and publication number", this is first_disclosure_detail with output_object=detail_record.
    - If the user asks for "new entrants in 2024", that means first entered in 2024, not merely active in 2024.
    - If the user asks for "new target pairs in the last 3 years", that means target pairs whose first disclosure year falls in the last-N-year window.
    - If the user mentions a concrete year such as 2020, 2021, 2023, or 2024, encode it as time_scope_type = single_year and time_scope_value = that year.
    - If the user mentions "last 3 years" or "last 5 years", encode it as time_scope_type = last_n_years and set time_scope_value to 3 or 5.
    - If the user mentions a year range such as "from 2020 to 2023", encode it as time_scope_type = explicit_range.

    Your goal is to make the query maximally easy for a downstream NL2Cypher model while preserving the user's intended business meaning.
    """
).strip()


def build_query_rewrite_user_prompt(
    user_query: str,
    schema_notes: str | None = None,
    extra_examples: Dict[str, Any] | None = None,
) -> str:
    payload = {
        "user_query": user_query,
        "schema_notes": schema_notes or (
            "Use the BsAb patent KG schema exactly as provided in the system prompt. "
            "TechnologyClass1, Functional_of_Target, Pathway, Target, TargetPair, Assignee, Origin, Year, Patent, and Cancer are distinct node types."
        ),
        "rewrite_requirements": [
            "Rewrite the question into one canonical English question for Text2Cypher.",
            "Make constraint axis, operation, output object, time scope, and ranking metric explicit.",
            "Do not answer the question.",
            "Do not generate Cypher.",
            "Return strict JSON only.",
        ],
        "reference_examples": extra_examples or {
            "example_1": {
                "input": "For target-pair combinations involving the Tissue injury & regeneration category, which company have the most published patents?",
                "output_style": {
                    "rewritten_question": "Among target-pair combinations involving the Functional_of_Target category Tissue injury & regeneration, which assignee has the highest patent count?",
                    "template_signature": {
                        "constraint_axis": "functional_of_target",
                        "operation": "rank_by_patent_count",
                        "output_object": "assignee",
                        "time_scope_type": "all_time",
                        "time_scope_value": None,
                        "ranking_metric": "patent_count",
                        "top_k": 1,
                    },
                },
            },
            "example_2": {
                "input": "For the Tumor-Intrinsic Control TechnologyClass1 category, rank the target pairs by patent count and return the top 10.",
                "output_style": {
                    "rewritten_question": "Within the TechnologyClass1 category Tumor-Intrinsic Control, which target pairs have the highest patent counts? Return the top 10 target pairs ranked by patent count.",
                    "template_signature": {
                        "constraint_axis": "technologyclass1",
                        "operation": "rank_by_patent_count",
                        "output_object": "target_pair",
                        "time_scope_type": "all_time",
                        "time_scope_value": None,
                        "ranking_metric": "patent_count",
                        "top_k": 10,
                    },
                },
            },
            "example_3": {
                "input": "In 2024, which companies first entered the Alpha-defensins-related target-pair space?",
                "output_style": {
                    "rewritten_question": "Which assignees first entered the target-pair space involving the Pathway Alpha-defensins in 2024?",
                    "template_signature": {
                        "constraint_axis": "pathway",
                        "operation": "new_entrants",
                        "output_object": "assignee",
                        "time_scope_type": "single_year",
                        "time_scope_value": 2024,
                        "ranking_metric": "patent_count_in_year",
                        "top_k": 10,
                    },
                },
            },
        },
    }
    return dedent(
        f"""
        Reformulate the following user question into a schema-aware canonical question for downstream Neo4j Text2Cypher generation.

        Input payload:
        {json.dumps(payload, ensure_ascii=False, indent=2)}
        """
    ).strip()
