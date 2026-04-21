from __future__ import annotations

import json
import re
import time
from typing import Dict, Optional

from bsab_kg_qa_en.extract import EntityExtractor, ExtractMatch, NodeCatalog


class NERService:
    """Shared entity extraction service (catalog + fuzzy match + optional LLM NER)."""

    def __init__(self, runner, llm):
        self.runner = runner
        self.llm = llm
        self.extractor: Optional[EntityExtractor] = None
        self._llm_ner_cache: Dict[str, Dict[str, ExtractMatch]] = {}
        self.init_extractor(max_retries=3)

    def init_extractor(self, max_retries: int = 2) -> None:
        self.extractor = None
        for i in range(max_retries):
            try:
                catalog = NodeCatalog.from_neo4j(self.runner)
                self.extractor = EntityExtractor(catalog)
                return
            except Exception:
                if i < max_retries - 1:
                    time.sleep(0.3)
        self.extractor = None

    def match_first(self, question: str, category: str) -> Optional[ExtractMatch]:
        if self.extractor is None:
            self.init_extractor(max_retries=1)
        if not self.extractor:
            return None
        return self.extractor.match_first_in_question(question, category)

    def _extract_entities_via_llm(self, question: str) -> Dict[str, ExtractMatch]:
        if self.extractor is None:
            self.init_extractor(max_retries=1)
        if not self.extractor:
            return {}
        if question in self._llm_ner_cache:
            return self._llm_ner_cache[question]

        categories = sorted(self.extractor.catalog.values.keys())
        system_prompt = f"""
Extract entity mentions from a BsAb patent question.
Return strict JSON:
{{"mentions": {{"<category>": ["<mention>"]}}}}
Allowed categories: {json.dumps(categories, ensure_ascii=False)}
No hallucination.
"""
        user_prompt = f"Question: {question}"
        text = self.llm.chat(system_prompt.strip(), user_prompt, temperature=0.0)
        text = re.sub(r"^```json\\s*|\\s*```$", "", text.strip(), flags=re.MULTILINE).strip()

        hits: Dict[str, ExtractMatch] = {}
        try:
            obj = json.loads(text)
            mentions = obj.get("mentions") if isinstance(obj, dict) else None
            if isinstance(mentions, dict):
                for category, raw_list in mentions.items():
                    if category not in categories or not isinstance(raw_list, list):
                        continue
                    best: Optional[ExtractMatch] = None
                    for raw in raw_list:
                        m = self.extractor.match(str(raw), category)
                        if m and (best is None or m.score > best.score):
                            best = m
                    if best:
                        hits[category] = best
        except Exception:
            pass

        self._llm_ner_cache[question] = hits
        return hits

    def extract_entity(self, question: str, category: str) -> tuple[Optional[str], Optional[ExtractMatch]]:
        if not category:
            return None, None

        hit = self.match_first(question, category)
        if hit:
            return hit.value, hit

        llm_hits = self._extract_entities_via_llm(question)
        llm_hit = llm_hits.get(category)
        if llm_hit:
            return llm_hit.value, llm_hit

        return None, None
