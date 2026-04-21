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
    1) Extract a pair string from user input (do not hallucinate)
    2) Split into two target symbols if possible
    3) Backtrace TargetPair.name via Target.symbol in KG
    4) Fallback: normalize separators and match by TargetPair.name
    """

    def __init__(self, runner: Neo4jRunner, prop_target_symbol: str = "symbol", prop_tp_name: str = "name"):
        self.runner = runner
        self.prop_target_symbol = prop_target_symbol
        self.prop_tp_name = prop_tp_name

    @staticmethod
    def _extract_pair_text(question: str) -> Optional[str]:
        m = re.search(r"([A-Za-z0-9\-_]+)\s*[/-]\s*([A-Za-z0-9\-_]+)", question)
        if not m:
            return None
        return f"{m.group(1)}/{m.group(2)}"

    @staticmethod
    def _split_pair(pair: str) -> Optional[Tuple[str, str]]:
        s = pair.replace(" ", "")
        for sep in ["x", "X", "×", "-", "–", "—"]:
            s = s.replace(sep, "/")
        parts = [p for p in s.split("/") if p]
        if len(parts) == 2:
            return parts[0], parts[1]
        return None

    @staticmethod
    def _normalize_to_slash(pair: str) -> str:
        return pair.replace(" ", "")

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
            tp_name=self._normalize_to_slash(raw),
            raw=raw,
            matched_by="separator_normalize",
            confidence=0.6,
            targets=targets,
        )
