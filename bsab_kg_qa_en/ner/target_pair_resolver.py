# bsab_kg_qa_en/ner/target_pair_resolver.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
import re

from bsab_kg_qa_en.kg.neo4j_runner import Neo4jRunner


@dataclass
class TargetPairResolution:
    tp_name: Optional[str]
    raw: Optional[str]
    matched_by: str
    confidence: float
    targets: Optional[Tuple[str, str]] = None


class TargetPairResolver:
    """
    Human-like entity resolution for TargetPair:
    1) Extract a pair-like surface form from user input (do not hallucinate)
    2) Split into two target mentions if possible
    3) Optionally backtrace TargetPair.name via exact Target.symbol match
    4) Never invent a canonical TargetPair when backtrace fails
    """

    def __init__(self, runner: Neo4jRunner, prop_target_symbol: str = "symbol", prop_tp_name: str = "name"):
        self.runner = runner
        self.prop_target_symbol = prop_target_symbol
        self.prop_tp_name = prop_tp_name

    @staticmethod
    def _extract_pair_text(question: str) -> Optional[str]:
        m = re.search(
            r"([A-Za-z0-9\-_]+)\s*(?:/|\+| and | AND | x | X | × | plus | PLUS )\s*([A-Za-z0-9\-_]+)",
            question,
        )
        if not m:
            return None
        return f"{m.group(1)}/{m.group(2)}"

    @staticmethod
    def _split_pair(pair: str) -> Optional[Tuple[str, str]]:
        parts = [
            p.strip()
            for p in re.split(r"\s*(?:/|\+|\band\b|\bplus\b|\bx\b|×)\s*", pair.strip(), flags=re.IGNORECASE)
            if p.strip()
        ]
        if len(parts) == 2:
            return parts[0], parts[1]
        return None

    def resolve(self, question: str) -> TargetPairResolution:
        raw = self._extract_pair_text(question)
        if not raw:
            return TargetPairResolution(tp_name=None, raw=None, matched_by="none", confidence=0.0)

        targets = self._split_pair(raw)
        if targets:
            t1, t2 = targets
            cypher = f"""
            MATCH (t1:Target {{{self.prop_target_symbol}:$t1}})
            MATCH (t2:Target {{{self.prop_target_symbol}:$t2}})
            MATCH (tp:TargetPair)-[:HAS_TARGET]->(t1)
            MATCH (tp)-[:HAS_TARGET]->(t2)
            RETURN tp.{self.prop_tp_name} AS name
            LIMIT 1;
            """
            rows = self.runner.run(cypher, {"t1": t1, "t2": t2})
            if rows:
                return TargetPairResolution(
                    tp_name=rows[0]["name"],
                    raw=raw,
                    matched_by="target_backtrace",
                    confidence=0.95,
                    targets=(t1, t2),
                )

        return TargetPairResolution(
            tp_name=None,
            raw=raw,
            matched_by="surface_pair",
            confidence=0.45,
            targets=targets,
        )
