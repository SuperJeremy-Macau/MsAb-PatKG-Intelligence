from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import re
from typing import Dict, Iterable, List, Optional, Sequence

from bsab_kg_qa_en.extract.node_catalog import NodeCatalog


def _normalize(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9\u4e00-\u9fa5]+", "", (text or "").lower())
    return cleaned.strip()


def _acronym(text: str) -> str:
    words = re.split(r"[^A-Za-z0-9]+", text)
    return "".join(w[0] for w in words if w).lower()


def _tokenize(text: str) -> List[str]:
    parts = re.findall(r"[A-Za-z0-9\-]+|[\u4e00-\u9fa5]{1,8}", text or "")
    return [p.strip() for p in parts if p.strip()]


@dataclass
class ExtractMatch:
    category: str
    value: str
    score: float
    matched_by: str


class EntityExtractor:
    """
    Fuzzy extractor for node-like entities in user questions.

    Matching layers:
    1) exact normalized match
    2) acronym match (e.g., "BMS" -> "Bristol Myers Squibb")
    3) sequence similarity fallback on question chunks
    """

    def __init__(self, catalog: NodeCatalog, min_score: float = 0.72):
        self.catalog = catalog
        self.min_score = min_score
        self._index: Dict[str, List[dict]] = {}
        self._build_index()

    def _build_index(self) -> None:
        self._index = {}
        for category, vals in self.catalog.values.items():
            bucket: List[dict] = []
            seen = set()
            for v in vals:
                norm = _normalize(v)
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                bucket.append({"value": v, "norm": norm, "acronym": _acronym(v), "len": len(norm)})
            self._index[category] = bucket

    def refresh(self, catalog: NodeCatalog) -> None:
        self.catalog = catalog
        self._build_index()

    def _build_question_chunks(self, question: str) -> List[str]:
        chunks: List[str] = []
        whole = (question or "").strip()
        if whole:
            chunks.append(whole)

        for piece in re.split(r"[,;，。？！!?\n]", whole):
            p = piece.strip()
            if p and p not in chunks:
                chunks.append(p)

        tokens = _tokenize(whole)
        max_n = min(6, len(tokens))
        for n in range(max_n, 0, -1):
            for i in range(0, len(tokens) - n + 1):
                span = " ".join(tokens[i : i + n]).strip()
                if len(_normalize(span)) < 2:
                    continue
                if span not in chunks:
                    chunks.append(span)
        return chunks

    def _best_match_from_chunks(self, chunks: Sequence[str], category: str) -> Optional[ExtractMatch]:
        candidates = self._index.get(category, [])
        if not candidates or not chunks:
            return None

        # 1) exact normalized containment against all chunks
        chunk_norms = [(_normalize(ch), ch) for ch in chunks if ch]
        chunk_norms = [(n, c) for n, c in chunk_norms if n]
        for text_norm, _raw in chunk_norms:
            for c in candidates:
                c_norm = c["norm"]
                if text_norm == c_norm:
                    return ExtractMatch(category=category, value=c["value"], score=1.0, matched_by="exact")
                if min(len(text_norm), len(c_norm)) >= 4 and (text_norm in c_norm or c_norm in text_norm):
                    return ExtractMatch(category=category, value=c["value"], score=0.96, matched_by="containment")

        # 2) acronym containment
        lowered_tokens = {
            t.lower()
            for chunk in chunks
            for t in re.findall(r"[A-Za-z]{2,12}", chunk)
        }
        for c in candidates:
            acro = c["acronym"]
            if acro and acro in lowered_tokens:
                return ExtractMatch(category=category, value=c["value"], score=0.90, matched_by="acronym")

        # 3) similarity fallback on short-listed chunk/candidate lengths
        best_score = 0.0
        best_candidate = None
        for text_norm, _raw in chunk_norms:
            tlen = len(text_norm)
            if tlen < 3:
                continue
            for c in candidates:
                clen = c["len"]
                # cheap pruning to keep runtime bounded
                if abs(clen - tlen) > max(8, int(0.7 * max(clen, tlen))):
                    continue
                score = SequenceMatcher(None, text_norm, c["norm"]).ratio()
                if score > best_score:
                    best_score = score
                    best_candidate = c

        if best_candidate is not None and best_score >= self.min_score:
            return ExtractMatch(
                category=category,
                value=best_candidate["value"],
                score=float(best_score),
                matched_by="similarity",
            )
        return None

    def match(self, text: str, category: str) -> Optional[ExtractMatch]:
        chunks = [text] if text else []
        return self._best_match_from_chunks(chunks, category)

    def match_first_in_question(self, question: str, category: str) -> Optional[ExtractMatch]:
        chunks = self._build_question_chunks(question)
        return self._best_match_from_chunks(chunks, category)

    def match_many(self, question: str, categories: Iterable[str]) -> Dict[str, ExtractMatch]:
        chunks = self._build_question_chunks(question)
        out: Dict[str, ExtractMatch] = {}
        for c in categories:
            hit = self._best_match_from_chunks(chunks, c)
            if hit:
                out[c] = hit
        return out
