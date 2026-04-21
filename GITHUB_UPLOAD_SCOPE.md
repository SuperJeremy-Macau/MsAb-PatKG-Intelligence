# GitHub Upload Scope

This repository is currently a mixed workspace: production-ish KG QA code, benchmark assets, manuscript materials, LightRAG experiments, and local runtime outputs live together.

For a clean GitHub upload, keep the repository focused on code that is needed to run or understand the main QA system.

## Keep

- `README.md`
- `bsab_kg_qa_en/app/`
- `bsab_kg_qa_en/config/`
  - keep `__init__.py`
  - keep `settings_loader.py`
  - keep sanitized `settings.yaml`
  - keep `settings.local.yaml.example`
- `bsab_kg_qa_en/core/`
  - keep active orchestrator code
  - exclude `archive/`
  - exclude backup files
- `bsab_kg_qa_en/extract/`
- `bsab_kg_qa_en/intents/`
- `bsab_kg_qa_en/kg/`
- `bsab_kg_qa_en/ner/`
  - keep runtime code
  - exclude `synonyms_work/`
- `bsab_kg_qa_en/query_frame_runtime/`
- `bsab_kg_qa_en/query_rewriting/`
- `bsab_kg_qa_en/resolvers/`
- `tools/`
  - keep scripts still needed for Neo4j export, chart export, supplementary table generation, and figure rendering
- `bsab_kg_qa_en/tests/scripts/README.md`
- selected benchmark entry scripts only if you want a public evaluation harness:
  - `bsab_kg_qa_en/tests/run_batch_eval.py`
  - `bsab_kg_qa_en/tests/run_questions_in_order.py`
  - `bsab_kg_qa_en/tests/run_three_mode_benchmark.py`
  - `bsab_kg_qa_en/tests/test_entity_extractor.py`

## Exclude

- `docs/`
  - manuscript `.docx`
  - `.pptx`
  - supplementary `.xlsx`
  - figures and generated assets
  - references / paper PDFs
- `Bsab-related-background/`
- `.lightrag_runtime*`
- `lightrag_api_report.txt`
- LightRAG-only benchmark and utility flows if they are not part of the published repo scope:
  - `bsab_kg_qa_en/tests/run_lightrag_benchmark.py`
  - `tools/ask_lightrag_neo4j.py`
  - `tools/ask_lightrag_template.py`
  - `tools/build_lightrag_index_from_neo4j.py`
  - `tools/inspect_lightrag_api.py`
  - `tools/minimal_lightrag_ingest_query.py`
- `bsab_kg_qa_en/tests/_archive_20260401/`
- `bsab_kg_qa_en/tests/_curated_20260401/` if the repo is code-first rather than result-first
- all `__pycache__/` and `.pyc`
- all `_tmp_*` test artifacts
- `bsab_kg_qa_en/core/archive/`
- old backup source files under `bsab_kg_qa_en/core/`

## Sensitive Files

- `bsab_kg_qa_en/config/settings.yaml` previously contained a real Neo4j password.
- The tracked repo copy should stay sanitized.
- Real credentials should go only in `bsab_kg_qa_en/config/settings.local.yaml`, which is ignored by Git.
- Because the password was already present in a tracked file, rotate that Neo4j password before publishing.

## Current Size Signals

- `bsab_kg_qa_en/tests/`: about 122.86 MB
- `bsab_kg_qa_en/tests/_archive_20260401/`: about 71.96 MB
- `bsab_kg_qa_en/tests/_curated_20260401/`: about 49.98 MB
- `docs/`: about 25.99 MB
- `Bsab-related-background/`: about 81.08 MB
- `.lightrag_runtime_neo4j_index/`: about 65.72 MB

These directories dominate repository size and are not required for a clean public code upload.

## Before Pushing

1. Remove already tracked non-code assets from the Git index if you do not want them on GitHub.
2. Confirm `settings.yaml` is sanitized and your real credentials only exist in `settings.local.yaml`.
3. Review whether you want to publish benchmark result datasets or only the benchmark code.
