from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Neo4j entity catalog for extractor fuzzy match.")
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--output", default="bsab_kg_qa_en/tests/entity_catalog.json")
    args = parser.parse_args()

    from bsab_kg_qa_en.config import load_settings
    from bsab_kg_qa_en.extract import NodeCatalog
    from bsab_kg_qa_en.kg import Neo4jRunner

    cfg = load_settings(args.settings)
    neo = cfg["neo4j"]

    runner = Neo4jRunner(
        uri=neo["uri"],
        user=neo["user"],
        password=neo["password"],
        database=neo["database"],
        max_rows=int(neo.get("max_rows", 50)),
    )

    try:
        catalog = NodeCatalog.from_neo4j(runner)
    finally:
        runner.close()

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "categories": {k: len(v) for k, v in catalog.values.items()},
        "values": catalog.values,
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Catalog exported to: {args.output}")
    print("Category counts:")
    for k, n in payload["categories"].items():
        print(f"- {k}: {n}")


if __name__ == "__main__":
    main()
