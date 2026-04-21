# Query Rewriting for NL2Cypher

This module stores prompt templates for a `Template-Guided Query Reformulation for NL2Cypher` workflow.

## Goal

Rewrite a user's original natural-language question into a clearer, schema-aligned canonical question before sending it to:

- `Neo4j_Text2CypherRetriever`

The rewrite layer is intended to improve:

- entity-type precision
- constraint-axis precision
- operation recognition
- time-scope clarity
- ranking-metric clarity

## Main prompt objects

- `QUERY_REWRITE_SYSTEM_PROMPT`
- `build_query_rewrite_user_prompt(user_query, schema_notes=None, extra_examples=None)`
- `QueryRewriter`

## Intended runtime

1. User asks a free-form business question.
2. LLM rewrites it into a canonical question plus structure metadata.
3. The rewritten question is sent to the downstream Text2Cypher generator.
4. Cypher is generated and executed.

## CLI example

```bash
python -m bsab_kg_qa_en.query_rewriting.query_rewriter \
  "For the Tumor-Intrinsic Control TechnologyClass1 category, rank the target pairs by patent count and return the top 10." \
  --model gpt-5.4 --pretty
```

## Why this exists

This project observed that downstream Text2Cypher quality is highly sensitive to question clarity.
The rewrite step is meant to normalize vague or diverse user phrasing into a schema-aware canonical form.

## Time handling

This rewrite layer uses a generalized time representation instead of hard-coding specific benchmark years:

- `time_scope_type = all_time`
- `time_scope_type = single_year`
- `time_scope_type = last_n_years`
- `time_scope_type = explicit_range`

and a companion `time_scope_value` field such as:

- `null`
- `2023`
- `3`
- `{"start_year": 2020, "end_year": 2023}`
