from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from bsab_kg_qa_en.kg.neo4j_runner import Neo4jRunner


@dataclass
class NodeCatalog:
    """
    Lightweight in-memory node name catalog for fuzzy extraction.
    Each category stores canonical values + alias indexes.
    """

    values: Dict[str, List[str]] = field(default_factory=dict)

    @staticmethod
    def _to_list(v: object) -> List[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        s = str(v).strip()
        return [s] if s else []

    @classmethod
    def _collect_names_with_aliases(cls, runner: "Neo4jRunner", label: str, prop: str) -> List[str]:
        rows = runner.run(
            f"""
            MATCH (n:{label})
            WHERE n.{prop} IS NOT NULL
            RETURN toString(n.{prop}) AS canonical, n.aliases AS aliases, n.alias AS alias
            LIMIT 20000
            """,
            enforce_limit=False,
        )
        out: List[str] = []
        seen = set()
        for r in rows:
            canonical = str(r.get("canonical") or "").strip()
            if canonical and canonical not in seen:
                out.append(canonical)
                seen.add(canonical)

            aliases = cls._to_list(r.get("aliases")) + cls._to_list(r.get("alias"))
            for a in aliases:
                if a and a not in seen:
                    out.append(a)
                    seen.add(a)
        return out

    @classmethod
    def from_neo4j(cls, runner: Neo4jRunner) -> "NodeCatalog":
        catalog = cls(values={})
        specs = {
            "assignee": ("Assignee", "name"),
            "functional_of_target": ("Functional_of_Target", "name"),
            "technologyclass1": ("TechnologyClass1", "name"),
            "cancer": ("Cancer", "name"),
            "pathway": ("Pathway", "name"),
            "target": ("Target", "symbol"),
            "target_pair": ("TargetPair", "name"),
            "origin": ("Origin", "name"),
        }
        non_empty_categories = 0
        for category, (label, prop) in specs.items():
            try:
                vals = cls._collect_names_with_aliases(runner, label=label, prop=prop)
            except Exception:
                vals = []
            catalog.values[category] = vals
            if vals:
                non_empty_categories += 1

        if non_empty_categories == 0:
            raise RuntimeError("Failed to build NodeCatalog: all categories are empty.")
        return catalog

    def get(self, category: str) -> List[str]:
        return self.values.get(category, [])
