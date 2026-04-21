# BsAb Patent Intelligence

This repository contains a streamlined code-focused subset of a larger local workspace for bispecific antibody patent intelligence.

The main application is a Neo4j-backed QA system that combines:
- graph retrieval over a BsAb patent knowledge graph
- intent-based Cypher templates
- query rewriting and query-frame orchestration
- LLM-based answer synthesis

This upload intentionally excludes manuscript assets, LightRAG experiments, benchmark result archives, runtime caches, and local research materials.

## Included

- `bsab_kg_qa_en/app/`
  Streamlit demo entrypoint
- `bsab_kg_qa_en/core/`
  orchestrators, answer synthesis, LLM wrapper
- `bsab_kg_qa_en/intents/`
  intent registry and JSON intent definitions
- `bsab_kg_qa_en/extract/`, `kg/`, `ner/`, `resolvers/`
  graph access and entity resolution
- `bsab_kg_qa_en/query_frame_runtime/`, `frames/`, `frame_structure_taxonomy/`, `query_rewriting/`
  query-frame and rewriting runtime
- `tools/`
  selected utility scripts for export and analysis

## Not Included

- `docs/`
- `Bsab-related-background/`
- `.lightrag_runtime*`
- benchmark archives and curated result folders
- local credentials
- `__pycache__` and temporary outputs

## Requirements

Python 3.10+ is recommended.

Install dependencies:

```bash
pip install -r requirements.txt
```

Current runtime dependencies used by the kept code include:
- `openai`
- `neo4j`
- `streamlit`
- `pandas`
- `openpyxl`
- `Pillow`

Some optional tool scripts may also expect a local Neo4j database populated with the BsAb graph.

## Configuration

The repository-safe default config lives at:

`bsab_kg_qa_en/config/settings.yaml`

It does not contain real credentials.

Create a local config override by copying:

`bsab_kg_qa_en/config/settings.local.yaml.example`

to:

`bsab_kg_qa_en/config/settings.local.yaml`

Then fill in:
- Neo4j URI
- Neo4j username/password
- database name if not `neo4j`
- `OPENAI_API_KEY` in your shell environment

The loader automatically merges `settings.local.yaml` over `settings.yaml`.

## Run The Demo

From the repository root:

```bash
streamlit run bsab_kg_qa_en/app/app_demo.py
```

The demo requires:
- a reachable Neo4j instance
- a populated graph matching the expected schema
- `OPENAI_API_KEY`

## Utility Scripts

The `tools/` directory contains selected export and analysis scripts kept from the original workspace.

Important limitation:
- several of these scripts still assume the original project context and may write outputs to paths that were part of the larger local workspace
- treat them as developer utilities, not polished standalone CLI tools

## Verification Status

What was checked in this upload-ready folder:
- config loading works with the sanitized config template
- package structure is internally consistent
- the main package import chain reaches the LLM layer

What was not fully runnable in the current environment:
- full app startup, because the local environment here does not have `openai` installed
- live graph queries, because they require your own Neo4j instance and credentials

## Notes

- `GITHUB_UPLOAD_SCOPE.md` documents what was kept versus excluded.
- `UPLOAD_NOTE.txt` summarizes how this folder was prepared.
- If you plan to make this a long-term public repository, the next cleanup step should be narrowing `tools/` further and adding a proper dataset/schema setup guide.
