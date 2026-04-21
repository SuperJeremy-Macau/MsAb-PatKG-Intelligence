# bsab_kg_qa_en/kg/neo4j_runner.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from neo4j import GraphDatabase
import re


class Neo4jRunner:
    def __init__(self, uri: str, user: str, password: str, database: str, max_rows: int = 50):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._database = database
        self._max_rows = max_rows

    def close(self) -> None:
        self._driver.close()

    @staticmethod
    def _has_limit(cypher: str) -> bool:
        return re.search(r"\bLIMIT\b", cypher, flags=re.IGNORECASE) is not None

    @staticmethod
    def _is_read_only(cypher: str) -> bool:
        upper = cypher.upper()
        banned = ["CREATE ", "MERGE ", "DELETE ", "SET ", "DROP ", "CALL ", "LOAD CSV", "APOC."]
        return not any(b in upper for b in banned)

    def run(self, cypher: str, params: Optional[Dict[str, Any]] = None, enforce_limit: bool = True) -> List[Dict[str, Any]]:
        if not self._is_read_only(cypher):
            raise ValueError("Cypher rejected: non read-only keywords detected.")

        q = cypher.strip()
        if enforce_limit and not self._has_limit(q):
            q = f"{q.rstrip(';')}\nLIMIT {self._max_rows};"

        with self._driver.session(database=self._database) as session:
            rs = session.run(q, params or {})
            return [dict(r) for r in rs]
