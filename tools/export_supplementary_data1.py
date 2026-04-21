from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from neo4j import GraphDatabase


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bsab_kg_qa_en.config import load_settings


def _jsonify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _standardized_name(props: dict[str, Any], preferred: list[str]) -> str:
    for key in preferred:
        value = props.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    for key in ("symbol", "name", "canonical_name", "display_name"):
        value = props.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _frame_from_records(records: list[dict[str, Any]], preferred_name_keys: list[str], first_columns: list[str]) -> pd.DataFrame:
    prop_keys: set[str] = set()
    for row in records:
        prop_keys.update((row.get("props") or {}).keys())

    ordered_prop_keys = sorted(prop_keys)
    rows: list[dict[str, Any]] = []
    for row in records:
        props = row.get("props") or {}
        flat: dict[str, Any] = {
            "element_id": row.get("element_id", ""),
            "standardized_name": _standardized_name(props, preferred_name_keys),
        }
        for key in ordered_prop_keys:
            flat[key] = _jsonify(props.get(key))

        for key, value in row.items():
            if key in {"element_id", "props"}:
                continue
            flat[key] = _jsonify(value)
        rows.append(flat)

    df = pd.DataFrame(rows)
    ordered = [c for c in first_columns if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    if "standardized_name" in df.columns:
        df = df.sort_values(by=["standardized_name", "element_id"], kind="stable").reset_index(drop=True)
    return df.loc[:, ordered]


def fetch_target_rows(session) -> list[dict[str, Any]]:
    query = """
    MATCH (n:Target)
    OPTIONAL MATCH (n)-[:IN_PATHWAY]->(pw:Pathway)
    OPTIONAL MATCH (n)-[:FUNCTIONED_AS]->(f:Functional_of_Target)
    OPTIONAL MATCH (n)-[:DIFFERENTIAL_AND_HIGHLY_EXPRESSED_IN]->(c:Cancer)
    RETURN elementId(n) AS element_id,
           properties(n) AS props,
           collect(DISTINCT pw.name) AS pathway_names,
           count(DISTINCT pw) AS pathway_count,
           collect(DISTINCT f.name) AS functional_categories,
           count(DISTINCT f) AS functional_category_count,
           collect(DISTINCT c.name) AS cancer_names,
           count(DISTINCT c) AS cancer_count
    """
    return [dict(r) for r in session.run(query)]


def fetch_targetpair_rows(session) -> list[dict[str, Any]]:
    query = """
    MATCH (n:TargetPair)
    OPTIONAL MATCH (n)-[:HAS_TARGET]->(t:Target)
    OPTIONAL MATCH (n)-[:HAS_TECHNOLOGY_CLASS1]->(tc)
    RETURN elementId(n) AS element_id,
           properties(n) AS props,
           collect(DISTINCT coalesce(t.symbol, t.name)) AS component_targets,
           count(DISTINCT t) AS target_count,
           collect(DISTINCT tc.name) AS technology_classes,
           count(DISTINCT tc) AS technology_class_count
    """
    return [dict(r) for r in session.run(query)]


def fetch_assignee_rows(session) -> list[dict[str, Any]]:
    query = """
    MATCH (n:Assignee)
    OPTIONAL MATCH (n)-[:ORIGIN_FROM]->(o:Origin)
    OPTIONAL MATCH (p:Patent)-[:HAS_ASSIGNEE]->(n)
    RETURN elementId(n) AS element_id,
           properties(n) AS props,
           collect(DISTINCT o.name) AS origin_names,
           count(DISTINCT o) AS origin_count,
           count(DISTINCT p) AS patent_count
    """
    return [dict(r) for r in session.run(query)]


def fetch_function_rows(session) -> list[dict[str, Any]]:
    query = """
    MATCH (n:Functional_of_Target)
    OPTIONAL MATCH (t:Target)-[:FUNCTIONED_AS]->(n)
    RETURN elementId(n) AS element_id,
           properties(n) AS props,
           count(DISTINCT t) AS target_count,
           collect(DISTINCT coalesce(t.symbol, t.name)) AS targets
    """
    return [dict(r) for r in session.run(query)]


def fetch_pathway_rows(session) -> list[dict[str, Any]]:
    query = """
    MATCH (n:Pathway)
    OPTIONAL MATCH (t:Target)-[:IN_PATHWAY]->(n)
    RETURN elementId(n) AS element_id,
           properties(n) AS props,
           count(DISTINCT t) AS target_count,
           collect(DISTINCT coalesce(t.symbol, t.name)) AS targets
    """
    return [dict(r) for r in session.run(query)]


def fetch_origin_rows(session) -> list[dict[str, Any]]:
    query = """
    MATCH (n:Origin)
    OPTIONAL MATCH (a:Assignee)-[:ORIGIN_FROM]->(n)
    RETURN elementId(n) AS element_id,
           properties(n) AS props,
           count(DISTINCT a) AS assignee_count,
           collect(DISTINCT a.name) AS assignees
    """
    return [dict(r) for r in session.run(query)]


def build_readme(exported_at: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    metadata = pd.DataFrame(
        [
            {"Item": "Dataset", "Value": "Supplementary Data 1"},
            {"Item": "Source", "Value": "Neo4j knowledge graph used in the manuscript"},
            {"Item": "Exported at (UTC)", "Value": exported_at},
            {"Item": "Scope", "Value": "Normalized entity dictionaries for Target, TargetPair, Assignee, Functional_of_Target, Pathway, and Origin"},
        ]
    )

    overview = pd.DataFrame(
        [
            {
                "Sheet name": "Target",
                "Content": "Normalized target nodes exported from Neo4j",
                "Row definition": "One row per Target node",
                "Key columns": "element_id, standardized_name, symbol, aliases, pathway_count, functional_category_count, cancer_count",
                "Notes": "standardized_name preferentially uses symbol; related pathway/function/cancer lists are provided as JSON strings",
            },
            {
                "Sheet name": "TargetPair",
                "Content": "Normalized target-pair nodes, including a small number of trispecific and other multispecific combinations",
                "Row definition": "One row per TargetPair node",
                "Key columns": "element_id, standardized_name, name, component_targets, technology_classes",
                "Notes": "component_targets are derived from HAS_TARGET links; target_count may exceed 2 for multispecific constructs",
            },
            {
                "Sheet name": "Assignee",
                "Content": "Normalized assignee nodes at the standardized entity level",
                "Row definition": "One row per Assignee node",
                "Key columns": "element_id, standardized_name, name, aliases, origin_names, patent_count",
                "Notes": "origin_names are derived from ORIGIN_FROM links; patent_count is the number of connected Patent nodes",
            },
            {
                "Sheet name": "Functional_of_Target",
                "Content": "Manually curated functional categories for target roles in multispecific-antibody design",
                "Row definition": "One row per Functional_of_Target node",
                "Key columns": "element_id, standardized_name, name, target_count, targets",
                "Notes": "targets lists the normalized Target nodes linked through FUNCTIONED_AS",
            },
            {
                "Sheet name": "Pathway",
                "Content": "Pathway nodes linked to normalized targets",
                "Row definition": "One row per Pathway node",
                "Key columns": "element_id, standardized_name, name, stable_id, target_count, targets",
                "Notes": "all available node properties are retained; targets are linked through IN_PATHWAY",
            },
            {
                "Sheet name": "Origin",
                "Content": "Normalized assignee-origin nodes",
                "Row definition": "One row per Origin node",
                "Key columns": "element_id, standardized_name, name, assignee_count, assignees",
                "Notes": "origin refers to the assignee headquarters origin rather than patent jurisdiction",
            },
        ]
    )
    return metadata, overview


def autosize_sheet(writer: pd.ExcelWriter, sheet_name: str, dataframe: pd.DataFrame) -> None:
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(dataframe.columns):
        max_len = max([len(str(col))] + [len(str(v)) for v in dataframe[col].head(200).tolist()])
        ws.set_column(idx, idx, min(max(max_len + 2, 14), 60))


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Supplementary Data 1 from Neo4j.")
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--output", default="docs/Supplementary Data 1.xlsx")
    args = parser.parse_args()

    cfg = load_settings(args.settings)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    try:
        with driver.session(database=neo["database"]) as session:
            target_df = _frame_from_records(
                fetch_target_rows(session),
                preferred_name_keys=["symbol", "name"],
                first_columns=["element_id", "standardized_name", "symbol", "name", "alias", "aliases", "pathway_count", "functional_category_count", "cancer_count", "pathway_names", "functional_categories", "cancer_names"],
            )
            targetpair_df = _frame_from_records(
                fetch_targetpair_rows(session),
                preferred_name_keys=["name"],
                first_columns=["element_id", "standardized_name", "name", "alias", "aliases", "target_count", "component_targets", "technology_class_count", "technology_classes"],
            )
            assignee_df = _frame_from_records(
                fetch_assignee_rows(session),
                preferred_name_keys=["name"],
                first_columns=["element_id", "standardized_name", "name", "alias", "aliases", "origin_count", "origin_names", "patent_count"],
            )
            function_df = _frame_from_records(
                fetch_function_rows(session),
                preferred_name_keys=["name"],
                first_columns=["element_id", "standardized_name", "name", "alias", "aliases", "target_count", "targets"],
            )
            pathway_df = _frame_from_records(
                fetch_pathway_rows(session),
                preferred_name_keys=["name", "stable_id"],
                first_columns=["element_id", "standardized_name", "name", "stable_id", "databaseName", "speciesName", "alias", "aliases", "target_count", "targets"],
            )
            origin_df = _frame_from_records(
                fetch_origin_rows(session),
                preferred_name_keys=["name"],
                first_columns=["element_id", "standardized_name", "name", "alias", "aliases", "assignee_count", "assignees"],
            )
    finally:
        driver.close()

    exported_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    metadata_df, readme_df = build_readme(exported_at)

    output_path = ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        metadata_df.to_excel(writer, sheet_name="README", startrow=0, index=False)
        readme_df.to_excel(writer, sheet_name="README", startrow=len(metadata_df) + 3, index=False)

        target_df.to_excel(writer, sheet_name="Target", index=False)
        targetpair_df.to_excel(writer, sheet_name="TargetPair", index=False)
        assignee_df.to_excel(writer, sheet_name="Assignee", index=False)
        function_df.to_excel(writer, sheet_name="Functional_of_Target", index=False)
        pathway_df.to_excel(writer, sheet_name="Pathway", index=False)
        origin_df.to_excel(writer, sheet_name="Origin", index=False)

        autosize_sheet(writer, "README", readme_df)
        autosize_sheet(writer, "Target", target_df)
        autosize_sheet(writer, "TargetPair", targetpair_df)
        autosize_sheet(writer, "Assignee", assignee_df)
        autosize_sheet(writer, "Functional_of_Target", function_df)
        autosize_sheet(writer, "Pathway", pathway_df)
        autosize_sheet(writer, "Origin", origin_df)

    print(f"Supplementary Data 1 exported to: {output_path}")
    print(f"Target rows: {len(target_df)}")
    print(f"TargetPair rows: {len(targetpair_df)}")
    print(f"Assignee rows: {len(assignee_df)}")
    print(f"Functional_of_Target rows: {len(function_df)}")
    print(f"Pathway rows: {len(pathway_df)}")
    print(f"Origin rows: {len(origin_df)}")


if __name__ == "__main__":
    main()
