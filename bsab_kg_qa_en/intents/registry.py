# bsab_kg_qa_en/intents/registry.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class IntentDef:
    name: str
    description: str
    params_schema: Dict[str, Any]
    cypher: str
    result_schema: Dict[str, str]
    ui: Dict[str, Any]


class IntentRegistry:
    def __init__(self, definitions_dir: str):
        self._dir = definitions_dir
        self._intents: Dict[str, IntentDef] = {}
        self._load_all()

    def _load_all(self) -> None:
        if not os.path.isdir(self._dir):
            raise FileNotFoundError(f"Intent definitions dir not found: {self._dir}")

        for fn in os.listdir(self._dir):
            if not fn.endswith(".json"):
                continue
            path = os.path.join(self._dir, fn)
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            idef = IntentDef(
                name=data["name"],
                description=data.get("description", ""),
                params_schema=data.get("params_schema", {}),
                cypher=data["cypher"],
                result_schema=data.get("result_schema", {}),
                ui=data.get("ui", {"show": True, "examples": []}),
            )
            self._intents[idef.name] = idef

    def get(self, name: str) -> Optional[IntentDef]:
        return self._intents.get(name)

    def list(self, only_show: bool = True) -> List[IntentDef]:
        if not only_show:
            return list(self._intents.values())
        return [i for i in self._intents.values() if i.ui.get("show", True)]
