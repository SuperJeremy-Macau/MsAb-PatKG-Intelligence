# Frame Structure Taxonomy

This folder contains the reusable structure taxonomy used to annotate dataset questions for the independent query-frame route.

Current purpose:
- classify each question into a formal `frame_structure_class`
- expose the dimensions needed by a structure-first query-frame orchestrator
- generate appendix-ready tables for manuscript use

Core dimensions:
- `frame_constraint_signature`
- `frame_operation_type`
- `frame_output_type`
- `frame_time_scope`

The combined fine-grained class is:
- `frame_structure_class = constraint_signature__operation_type__output_type__time_scope`

This taxonomy is intentionally richer than the six abstract semantic structures in Li & Ji (2022), because the BsAb patent benchmark includes analytical operations such as:
- top-k ranking by patent count
- top-k ranking by family count
- first disclosure detail lookup
- new entrant detection
- CAGR / growth / diversity analysis

These business-analysis structures cannot be represented adequately by hop topology alone.
