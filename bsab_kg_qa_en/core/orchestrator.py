# bsab_kg_qa_en/core/orchestrator.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import json
import os
import re
import time
from datetime import datetime

from bsab_kg_qa_en.core.answer_synthesizer import AnswerSynthesizer
from bsab_kg_qa_en.core.llm_provider import LLMProvider
from bsab_kg_qa_en.extract import EntityExtractor, ExtractMatch, NodeCatalog
from bsab_kg_qa_en.intents.registry import IntentRegistry
from bsab_kg_qa_en.kg.neo4j_runner import Neo4jRunner
from bsab_kg_qa_en.ner import NERService, TargetPairResolver
from bsab_kg_qa_en.query_rewriting import QueryRewriter


@dataclass
class AnswerBundle:
    mode: str
    answer: str
    debug: Dict[str, Any]


_OFFICIAL_T2C_PROMPT_BASE = """
Task: Generate a Cypher statement for querying a Neo4j graph database from a user input.

Schema:
{schema}

Examples (optional):
{examples}

Input:
{query_text}

Rules:
- Generate exactly one read-only query.
- Use only MATCH/OPTIONAL MATCH/WHERE/WITH/RETURN/ORDER BY/LIMIT.
- Do not use CREATE/MERGE/DELETE/DETACH/SET/DROP/CALL/APOC/LOAD CSV.
- Always include LIMIT <= 50.
- Do not include triple backticks or any extra text.

Cypher query:
"""

_OFFICIAL_T2C_PROMPT_COUNT = """
Task: Generate a Cypher statement for querying a Neo4j graph database from a user input.

Schema:
{schema}

Examples (optional):
{examples}

Input:
{query_text}

Rules:
- Generate exactly one read-only query.
- Prefer precise counting/aggregation for questions asking "how many", "count", "number".
- Bind concrete entities with exact equality filters whenever possible.
- Use only MATCH/OPTIONAL MATCH/WHERE/WITH/RETURN/ORDER BY/LIMIT.
- Do not use CREATE/MERGE/DELETE/DETACH/SET/DROP/CALL/APOC/LOAD CSV.
- Always include LIMIT <= 50.
- Do not include triple backticks or any extra text.

Cypher query:
"""

_OFFICIAL_T2C_PROMPT_TREND = """
Task: Generate a Cypher statement for querying a Neo4j graph database from a user input.

Schema:
{schema}

Examples (optional):
{examples}

Input:
{query_text}

Rules:
- Generate exactly one read-only query.
- For year/ranking/trend questions, include explicit temporal and grouping logic.
- Use only MATCH/OPTIONAL MATCH/WHERE/WITH/RETURN/ORDER BY/LIMIT.
- Do not use CREATE/MERGE/DELETE/DETACH/SET/DROP/CALL/APOC/LOAD CSV.
- Always include LIMIT <= 50.
- Do not include triple backticks or any extra text.

Cypher query:
"""


def _build_official_text2cypher_retriever(
    runner: Neo4jRunner,
    llm: LLMProvider,
    schema_text: str,
    custom_prompt: str,
) -> Tuple[Optional[Any], Optional[str]]:
    try:
        from neo4j_graphrag.llm import OpenAILLM
        from neo4j_graphrag.retrievers import Text2CypherRetriever

        model_name = getattr(llm, "_model", None) or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL")
        if not base_url:
            client = getattr(llm, "_client", None)
            base = getattr(client, "base_url", None)
            base_url = str(base) if base else None

        llm_kwargs: Dict[str, Any] = {}
        if api_key:
            llm_kwargs["api_key"] = api_key
        if base_url:
            llm_kwargs["base_url"] = base_url

        official_llm = OpenAILLM(
            model_name=model_name,
            model_params={"temperature": 0.0},
            **llm_kwargs,
        )
        retriever = Text2CypherRetriever(
            driver=runner._driver,  # pylint: disable=protected-access
            llm=official_llm,
            neo4j_schema=schema_text,
            custom_prompt=custom_prompt,
            neo4j_database=runner._database,  # pylint: disable=protected-access
        )
        return retriever, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


class LLMOnlyOrchestrator:
    """Isolated LLM-only path."""

    def __init__(self, llm: LLMProvider, temperature_no_kg: float = 0.2):
        self.synth = AnswerSynthesizer(llm, temperature_answer=0.2, temperature_no_kg=temperature_no_kg)

    def answer(self, question: str, mode: str = "no_kg") -> AnswerBundle:
        ans = self.synth.answer_no_kg(question)
        return AnswerBundle(mode="no_kg", answer=ans, debug={"mode": "no_kg"})


class _GraphBaseOrchestrator:
    _ALLOWED_LABELS = {
        "Patent", "Family", "Pathway", "Assignee", "TargetPair", "Target",
        "Functional_of_Target", "TechnologyClass1", "Year", "Cancer", "Origin",
    }
    _ALLOWED_RELS = {
        "HAS_ASSIGNEE", "HAS_TARGET_PAIR", "HAS_PATENT", "PUBLISHED_IN",
        "IN_PATHWAY", "CO_ASSIGNEE_WITH", "DIFFERENTIAL_AND_HIGHLY_EXPRESSED_IN",
        "HAS_TARGET", "ORIGIN_FROM", "COMBINATION_WITH", "FUNCTIONED_AS",
        "HAS_TECHNOLOGY_CLASS1",
    }
    _BANNED_TOKENS = [
        "CREATE", "MERGE", "DELETE", "DETACH", "SET", "DROP", "CALL", "APOC", "LOAD CSV",
        "FOREACH", "REMOVE", "GRANT", "DENY", "REVOKE",
    ]

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
    ):
        self.runner = runner
        self.registry = registry
        self.llm = llm
        self.props = props
        self.temperature_intent = temperature_intent

        self.synth = AnswerSynthesizer(
            llm,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        self.tp_resolver = TargetPairResolver(
            runner,
            prop_target_symbol=self.props.get("target_symbol", "symbol"),
            prop_tp_name=self.props.get("targetpair_name", "name"),
        )

        self.ner = NERService(self.runner, self.llm)
        self.extractor: Optional[EntityExtractor] = self.ner.extractor
        self._llm_ner_cache: Dict[str, Dict[str, ExtractMatch]] = {}
        self._last_nl2cypher_raw: Optional[str] = None

    def _init_extractor(self, max_retries: int = 2) -> None:
        self.ner.init_extractor(max_retries=max_retries)
        self.extractor = self.ner.extractor

    # ---------------- common helpers ----------------
    def _extract_year(self, question: str) -> Optional[int]:
        matches = re.findall(r"\b(?:19|20)\d{2}\b", question or "")
        return int(matches[-1]) if matches else None

    def _extract_int_value(self, question: str) -> Optional[int]:
        m = re.search(r"\b(\d{1,5})\b", question or "")
        return int(m.group(1)) if m else None

    def _extract_year_range(self, question: str) -> Tuple[Optional[int], Optional[int]]:
        q = (question or "").lower()
        years = re.findall(r"\b(?:19|20)\d{2}\b", question or "")

        # Relative ranges: "last 3 years", "past 5 years", "近三年/近5年"
        m_rel_en = re.search(r"\b(?:last|past)\s+(\d{1,2})\s+years?\b", q)
        m_rel_zh = re.search(r"近\s*([一二两三四五六七八九十\d]{1,3})\s*年", question or "")
        if m_rel_en or m_rel_zh:
            n = None
            if m_rel_en:
                try:
                    n = int(m_rel_en.group(1))
                except Exception:
                    n = None
            elif m_rel_zh:
                raw = m_rel_zh.group(1)
                zh_num = {
                    "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
                    "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
                }
                if raw.isdigit():
                    n = int(raw)
                elif raw in zh_num:
                    n = zh_num[raw]
            if n and 1 <= n <= 20:
                end_year = datetime.now().year - 1
                start_year = end_year - n + 1
                return start_year, end_year

        if not years:
            return None, None

        # "between 2020 and 2023", "from 2020 to 2023"
        if (
            ("between" in q and "and" in q)
            or ("from" in q and "to" in q)
            or ("from" in q and "-" in q)
            or ("to" in q and "-" in q)
        ) and len(years) >= 2:
            y1, y2 = int(years[0]), int(years[1])
            return (y1, y2) if y1 <= y2 else (y2, y1)

        if len(years) == 1:
            y = int(years[0])

            # "before 2025", "prior to 2025", "< 2025"
            if any(k in q for k in ["before", "prior to", "earlier than", "older than", "< "]):
                return 1900, y - 1
            # "after 2025", "since 2025", "> 2025"
            if any(k in q for k in ["after", "since", "later than", "newer than", "> "]):
                return y + 1, 2100
            # "until 2025", "up to 2025", "through 2025"
            if any(k in q for k in ["until", "up to", "through", "by "]):
                return 1900, y

            return y, y

        y1, y2 = int(years[0]), int(years[1])
        return (y1, y2) if y1 <= y2 else (y2, y1)

    def _question_has_temporal_constraint(self, question: str) -> bool:
        q = (question or "").lower()
        if re.search(r"\b(before|after|since|until|prior to|earlier than|later than|up to|through)\b", q):
            return True
        if re.search(r"\b(last|past)\s+\d{1,2}\s+years?\b", q):
            return True
        if re.search(r"\bbetween\s+(?:19|20)\d{2}\s+and\s+(?:19|20)\d{2}\b", q):
            return True
        if re.search(r"\bfrom\s+(?:19|20)\d{2}\s+to\s+(?:19|20)\d{2}\b", q):
            return True
        if re.search(r"\bstartyear\b|\bendyear\b", q):
            return True
        return False

    def _normalize_tp_to_slash(self, text: str) -> Optional[str]:
        m = re.search(r"([A-Za-z0-9\-_]+)\s*[/\-]\s*([A-Za-z0-9\-_]+)", text or "")
        if not m:
            return None
        return f"{m.group(1)}/{m.group(2)}"

    def _validate_cypher_strict(self, cypher: str) -> None:
        if not cypher or not isinstance(cypher, str):
            raise ValueError("Empty cypher")

        c = cypher.strip()
        if not c.lower().startswith("match") and not c.lower().startswith("with"):
            raise ValueError("Cypher must start with MATCH or WITH")

        up = c.upper()
        for tok in self._BANNED_TOKENS:
            if tok in up:
                raise ValueError(f"Cypher rejected: banned token detected: {tok}")

        for lab in re.findall(r"\([A-Za-z_][A-Za-z0-9_]*\s*:\s*([A-Za-z_][A-Za-z0-9_]*)", c):
            if lab not in self._ALLOWED_LABELS:
                raise ValueError(f"Cypher rejected: unknown label :{lab}")

        for rel in re.findall(r"\[:([A-Za-z_][A-Za-z0-9_]*)\]", c):
            if rel not in self._ALLOWED_RELS:
                raise ValueError(f"Cypher rejected: unknown relationship :{rel}")

        if re.search(r"\bLIMIT\b", c, flags=re.IGNORECASE) is None:
            raise ValueError("Cypher rejected: missing LIMIT")

    def _build_schema_text(self) -> str:
        return (
            "Core paths:\n"
            "(Patent)-[:HAS_ASSIGNEE]->(Assignee)-[:ORIGIN_FROM]->(Origin)\n"
            "(Patent)-[:HAS_TARGET_PAIR]->(TargetPair)-[:HAS_TARGET]->(Target)\n"
            "(TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(TechnologyClass1)\n"
            "(Patent)-[:PUBLISHED_IN]->(Year)\n"
            "(Target)-[:IN_PATHWAY]->(Pathway)\n"
            "(Target)-[:DIFFERENTIAL_AND_HIGHLY_EXPRESSED_IN]->(Cancer)\n"
            "(Target)-[:FUNCTIONED_AS]->(Functional_of_Target)\n"
            "\n"
            "Properties:\n"
            f"Assignee.{self.props.get('assignee_name', 'name')}\n"
            f"TargetPair.{self.props.get('targetpair_name', 'name')}\n"
            "TechnologyClass1.name\n"
            f"Target.{self.props.get('target_symbol', 'symbol')}\n"
            f"Year.{self.props.get('year_value', 'year')}\n"
            f"Origin.{self.props.get('origin_name', 'name')}\n"
            f"Patent.{self.props.get('patent_pub_no', 'pub_no')}\n"
            "\n"
            "Allowed labels: " + ", ".join(sorted(self._ALLOWED_LABELS)) + "\n"
            "Allowed rels: " + ", ".join(sorted(self._ALLOWED_RELS)) + "\n"
        )

    # ---------------- entity extraction ----------------
    def _guess_category_from_param(self, key: str) -> Optional[str]:
        k = (key or "").strip().lower()
        if not k:
            return None
        if k in {"tp_name", "target_pair", "targetpair", "pair", "targetpair_name"}:
            return "target_pair"
        if k in {"assignee", "company", "company_name", "assignee_name"} or "assignee" in k or "company" in k:
            return "assignee"
        if k in {"pathway", "pw", "pathway_name"}:
            return "pathway"
        if k in {"cancer", "tumor", "oncology"}:
            return "cancer"
        if k in {"function", "functional_of_target", "functional", "target_function"}:
            return "functional_of_target"
        if k in {
            "technologyclass1", "technology_class1", "technologyclass",
            "technology_class", "techclass", "tech_class", "techclass1",
        }:
            return "technologyclass1"
        if k in {"origin", "country", "region"}:
            return "origin"
        if k in {"target", "target1", "target2", "target_name"} or (k.startswith("target") and "pair" not in k):
            return "target"
        return None

    def _extract_entities_via_llm(self, question: str) -> Dict[str, ExtractMatch]:
        return self.ner._extract_entities_via_llm(question)

    def _extract_entity_param(self, question: str, key: str) -> Tuple[Optional[str], Optional[ExtractMatch]]:
        category = self._guess_category_from_param(key)
        val, match = self.ner.extract_entity(question, category or "")
        self.extractor = self.ner.extractor
        return val, match

    def _extract_numeric_param(self, question: str, key: str) -> Optional[int]:
        k = (key or "").lower()
        if k == "year":
            return self._extract_year(question)
        if k in {"years", "min_count", "top_k", "limit", "count", "start_year", "end_year"}:
            return self._extract_int_value(question)
        return None

    def _clarify_missing(self, missing: str, intent_name: str) -> str:
        m = (missing or "").lower()
        if m in {"tp_name", "target_pair", "targetpair"}:
            return "Please specify a concrete target pair (for example PD-1/VEGFA)."
        if m in {"year", "start_year", "end_year"}:
            return "Please specify concrete year information (for example 2024)."
        if m in {"cancer"}:
            return "Please specify a concrete cancer type (for example lung cancer)."
        if m in {"assignee", "company", "assignee_name", "company_name"}:
            return "Please specify a concrete assignee/company name."
        if m in {"target", "target1", "target2"}:
            return "Please specify a concrete target symbol."
        if m in {"origin", "country", "region"}:
            return "Please specify a concrete origin/country."
        return f"Please provide the required parameter for intent {intent_name}: {missing}."

    # ---------------- intent ----------------
    def _intent_route_tokens(self, text: str) -> List[str]:
        stop = {
            "the", "a", "an", "of", "in", "on", "for", "to", "and", "or", "by", "with", "from",
            "that", "which", "what", "who", "whom", "is", "are", "was", "were", "be", "been",
            "this", "these", "those", "their", "there", "into", "across", "among", "within",
            "please", "provide", "return", "show", "list", "all", "any", "do", "does", "did",
            "have", "has", "had", "more", "most", "top", "last", "recent", "recently", "year",
            "years", "pair", "pairs", "target", "targets", "patent", "patents",
        }
        tokens = re.findall(r"[a-z0-9]+", (text or "").lower())
        return [tok for tok in tokens if tok not in stop and len(tok) > 1]

    def _shortlist_intents(self, question: str, limit: int = 18) -> List[Any]:
        intents = self.registry.list(only_show=False)
        if len(intents) <= limit:
            return intents

        q = (question or "").lower()
        q_tokens = set(self._intent_route_tokens(question))
        q_years = set(re.findall(r"(?:19|20)\d{2}", q))
        scored: List[Tuple[float, Any]] = []
        for idef in intents:
            corpus_parts = [idef.name, idef.description] + list((idef.ui or {}).get("examples", [])[:3])
            corpus = " ".join([str(x or "") for x in corpus_parts])
            corpus_lower = corpus.lower()
            c_tokens = set(self._intent_route_tokens(corpus))
            overlap = len(q_tokens & c_tokens)
            score = overlap * 5.0

            if q_years and any(y in corpus_lower for y in q_years):
                score += 8.0
            if "last three years" in q or "last 3 years" in q or "???" in question:
                if any(tag in idef.name for tag in ["_3Y", "LAST_3Y", "RECENT_YEARS", "RECENT_WINDOW"]):
                    score += 12.0
            if "2024" in q and "2024" in idef.name:
                score += 12.0
            if "new entrant" in q and "NEW_ENTRANT" in idef.name:
                score += 12.0
            if any(k in q for k in ["first disclosed", "first disclosure", "first appeared", "first appearance", "earliest"]):
                if "FIRST" in idef.name:
                    score += 10.0
            if "pathway" in q and "PATHWAY" in idef.name:
                score += 8.0
            if ("functional" in q or "category" in q) and "FUNCTION" in idef.name:
                score += 8.0
            if any(k in q for k in ["highly expressed", "high expression", "cancer"]):
                if ("CANCER" in idef.name) or ("HIGH_EXPRESSION" in idef.name):
                    score += 8.0
            if any(k in q for k in ["origin", "country", "region"]):
                if "ORIGIN" in idef.name:
                    score += 8.0
            if any(k in q for k in ["assignee", "company", "enterprise"]):
                if "ASSIGNEE" in idef.name:
                    score += 6.0

            scored.append((score, idef))

        scored.sort(key=lambda x: (-x[0], x[1].name))
        top = [idef for score, idef in scored[:limit] if score > 0]
        return top or [idef for _, idef in scored[:limit]]

    def classify_intent(self, question: str) -> Dict[str, Any]:
        heur = self._heuristic_intent(question)
        if heur and self.registry.get(heur):
            return {"intent": heur}

        candidates = self._shortlist_intents(question)
        supported = [i.name for i in candidates]
        cards = [
            {
                "name": idef.name,
                "description": idef.description,
                "examples": list((idef.ui or {}).get("examples", [])[:2]),
            }
            for idef in candidates
        ]
        system_prompt = """
You are an intent classifier for a BsAb patent KG QA system.

Choose exactly one intent from the candidate list.
Use the candidate descriptions and examples to decide the best semantic match.
Prefer the intent whose entity type, time constraint, aggregation target, and ranking logic best match the question.
Return strict JSON only: {"intent":"..."}
""".strip()
        user_prompt = (
            f"Question:\n{question}\n\n"
            f"Candidate intents:\n{json.dumps(cards, ensure_ascii=False, indent=2)}\n\n"
            f"Allowed intent names:\n{json.dumps(supported, ensure_ascii=False)}"
        )
        text = self.llm.chat(system_prompt, user_prompt, temperature=self.temperature_intent)
        text = re.sub(r"^```json\s*|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                chosen = str(obj.get("intent") or "").strip()
                if chosen in supported:
                    return {"intent": chosen}
        except Exception:
            pass
        return {"intent": supported[0] if supported else "UNKNOWN"}

    def _heuristic_intent(self, question: str) -> Optional[str]:
        q = (question or "").lower()

        if "which target pairs contain targets in the" in q and "category" in q:
            return "TARGETPAIRS_BY_FUNCTION"
        if "among target pairs related to the" in q and "category" in q and "most patents" in q:
            return "TOP_TARGETPAIRS_BY_FUNCTION_PATENT_COUNT"
        if "among target pairs related to the" in q and "category" in q and "most patent families" in q:
            return "TOP_TARGETPAIRS_BY_FUNCTION_FAMILY_COUNT"
        if "among target pairs related to the" in q and "category" in q and ("last three years" in q or "last 3 years" in q) and "first disclosed" in q:
            return "NEW_TARGETPAIRS_BY_FUNCTION_3Y"
        if "among target pairs related to the" in q and "category" in q and "first disclosed in 2024" in q:
            return "NEW_TARGETPAIRS_BY_FUNCTION_2024"
        if "functional category associated with" in q or "same functional category as" in q:
            return "TARGETPAIRS_BY_TARGET_FUNCTION"
        if ("functional-of-target categories are most prevalent" in q) or ("functional-of-target categories appear most frequently" in q):
            return "TOP_FUNCTIONS_2024" if "2024" in q else "TOP_FUNCTIONS_LAST_3Y"

        if "which target pairs contain targets in the" in q and "pathway" in q and "category" not in q:
            return "TARGETPAIRS_BY_PATHWAY"
        if "among target pairs related to the" in q and "pathway" in q and "most patents" in q:
            return "TOP_TARGETPAIRS_BY_PATHWAY_PATENT_COUNT"
        if "among target pairs related to the" in q and "pathway" in q and "most patent families" in q:
            return "TOP_TARGETPAIRS_BY_PATHWAY_FAMILY_COUNT"
        if "among target pairs related to the" in q and "pathway" in q and ("last three years" in q or "last 3 years" in q) and "first disclosed" in q:
            return "NEW_TARGETPAIRS_BY_PATHWAY_3Y"
        if "among target pairs related to the" in q and "pathway" in q and "first disclosed in 2024" in q:
            return "NEW_TARGETPAIRS_BY_PATHWAY_2024"
        if "pathway associated with" in q:
            return "TARGETPAIRS_BY_TARGET_PATHWAY"
        if ("which pathways appear most often overall" in q) or ("which pathways appear most frequently" in q):
            return "TOP_PATHWAYS_2024" if "2024" in q else "TOP_PATHWAYS_LAST_3Y"

        if "which targets are highly expressed in" in q:
            return "HIGH_EXPRESSION_TARGETS_BY_CANCER"
        if "what target pairs involve them" in q and "highly expressed in" in q:
            return "TARGETPAIRS_BY_CANCER"
        if "two targets that are both highly expressed in" in q:
            return "DOUBLE_HIGH_EXPRESSION_TARGETPAIRS_BY_CANCER"
        if "what are the patent publication numbers, publication years, and assignees" in q and "both highly expressed in" in q:
            return "PATENTS_FOR_DOUBLE_HIGH_EXPRESSION_TARGETPAIRS"
        if "what pathway combinations do they represent" in q and "both highly expressed in" in q:
            return "PATHWAY_COMBINATIONS_FOR_DOUBLE_HIGH_EXPRESSION_TARGETPAIRS"
        if "what functional-of-target combinations do they represent" in q and "both highly expressed in" in q:
            return "FUNCTION_COMBINATIONS_FOR_DOUBLE_HIGH_EXPRESSION_TARGETPAIRS"

        if q.startswith("does ") and "published patents" in q:
            return "PATENT_EXISTS_BY_TARGETPAIR"
        if "in which years were patents related to" in q and "published" in q:
            return "PATENT_YEARS_BY_TARGETPAIR"
        if "who filed patents related to" in q:
            return "ASSIGNEES_BY_TARGETPAIR"
        if "first disclosure year for" in q:
            return "FIRST_DISCLOSURE_YEAR_BY_TARGETPAIR"
        if "are there any target-pair combinations that include" in q:
            return "TARGETPAIR_EXISTS_BY_TARGET"
        if "what are the specific target-pair combinations that include" in q:
            return "TARGETPAIRS_BY_TARGET"
        if "what are the corresponding patent publication numbers" in q:
            return "PATENT_PUBLICATIONS_BY_TARGET"
        if "in which years were patents involving" in q and "filed" in q:
            return "PATENT_APPLICATION_YEARS_BY_TARGET"
        if "which assignees filed patents involving" in q:
            return "ASSIGNEES_BY_TARGET"
        if "which assignee filed the largest number" in q and "involving" in q:
            return "TOP_ASSIGNEE_BY_TARGET"

        if "novel target-pair combinations over the last three years" in q:
            return "TOP_ASSIGNEES_BY_NEW_TARGETPAIRS_3Y"
        if "novel target-pair combinations in 2024" in q or "introduced the greatest number of novel target-pair combinations in 2024" in q:
            return "TOP_ASSIGNEES_BY_NEW_TARGETPAIRS_2024"
        if "highly active in novel target-pair filings over the last three years" in q:
            return "NEW_TARGETPAIRS_FOR_TOP_ASSIGNEES"
        if "what functional-of-target pairings do they correspond to" in q:
            return "FUNCTION_COMBINATIONS_FOR_TOP_ASSIGNEE_TARGETPAIRS"
        if "what exact target pairs do they include" in q:
            return "TARGET_COMBINATIONS_FOR_TOP_ASSIGNEE_TARGETPAIRS"
        if "which involve targets that are highly expressed in cancer" in q:
            return "CANCER_HIGH_EXPRESSION_FOR_TOP_ASSIGNEE_TARGETPAIRS"
        if "new entrants in 2024" in q and "first time that year" in q:
            return "NEW_ENTRANTS_2024"
        if "what patent publication numbers were disclosed by those 2024 new entrants" in q:
            return "PATENT_PUBLICATIONS_FOR_NEW_ENTRANTS_2024"
        if "what target-pair combinations are associated with those 2024 new entrants" in q:
            return "TARGETPAIRS_FOR_NEW_ENTRANTS_2024"

        if "which companies have entered target-pair combinations involving the" in q and "category" in q:
            return "ASSIGNEES_BY_FUNCTION"
        if "which companies entered target pairs containing" in q and "targets in 2024" in q:
            return "ASSIGNEES_BY_FUNCTION_2024"
        if "within target-pair combinations related to the" in q and "category" in q and "which company disclosed them first" in q:
            return "FIRST_ASSIGNEE_BY_FUNCTION"
        if "within target-pair combinations related to the" in q and "category" in q and "last three years" in q and "published the most patents" in q:
            return "TOP_ASSIGNEES_BY_FUNCTION_3Y"
        if "within target-pair combinations related to the" in q and "category" in q and "2024" in q and "new entrants" in q:
            return "NEW_ENTRANTS_BY_FUNCTION_2024"
        if "within target-pair combinations related to the" in q and "category" in q and "filed the most patents" in q:
            return "TOP_ASSIGNEES_BY_FUNCTION_PATENT_COUNT"
        if "within target-pair combinations related to the" in q and "category" in q and "filed the most patent families" in q:
            return "TOP_ASSIGNEES_BY_FUNCTION_FAMILY_COUNT"

        if "which companies have entered target pairs involving the" in q and "pathway" in q:
            return "ASSIGNEES_BY_PATHWAY"
        if "which companies entered target-pair combinations containing" in q and "targets in 2024" in q:
            return "ASSIGNEES_BY_PATHWAY_2024"
        if "which companies entered target-pair combinations involving targets that belong to the" in q and "pathway in 2024" in q:
            return "ASSIGNEES_BY_PATHWAY_CATEGORY_2024"
        if "within target-pair combinations related to the" in q and "pathway" in q and "which company disclosed them first" in q:
            return "FIRST_ASSIGNEE_BY_PATHWAY"
        if "within target-pair combinations related to the" in q and "pathway" in q and "last three years" in q and "published the most patents" in q:
            return "TOP_ASSIGNEES_BY_PATHWAY_3Y"
        if "within target-pair combinations related to the" in q and "pathway" in q and "2024" in q and "new entrants" in q:
            return "NEW_ENTRANTS_BY_PATHWAY_2024"
        if "within target-pair combinations related to the" in q and "pathway" in q and "filed the most patents" in q:
            return "TOP_ASSIGNEES_BY_PATHWAY_PATENT_COUNT"
        if "within target-pair combinations related to the" in q and "pathway" in q and "filed the most patent families" in q:
            return "TOP_ASSIGNEES_BY_PATHWAY_FAMILY_COUNT"

        if "highest five-year compound annual growth rate" in q:
            return "TOP_ORIGIN_BY_CAGR_5Y"
        if "how have patent filings from organizations or individuals in" in q and "over the last three years" in q:
            return "TOP_ORIGIN_BY_GROWTH_3Y"
        if "year-over-year growth rate in 2024" in q:
            return "TOP_ORIGIN_BY_YOY_2024"
        if "lead in target-pair combinations containing" in q:
            return "TOP_ORIGIN_BY_TARGET"
        if "earliest filing" in q and "patent publication number" in q:
            return "FIRST_DISCLOSURE_DETAILS_BY_ORIGIN_TARGET"
        if "what functional-of-target combinations are involved in those patents" in q:
            return "FUNCTION_COMBINATIONS_BY_ORIGIN_TARGET"
        if "what target-pair combinations are involved in those patents" in q:
            return "TARGET_COMBINATIONS_BY_ORIGIN_TARGET"
        if "lead in target-pair combinations involving" in q and "targets" in q:
            return "TOP_ORIGIN_BY_FUNCTION"
        if "what target-pair combinations are covered" in q and "involve" in q:
            return "TARGET_COMBINATIONS_BY_ORIGIN_FUNCTION"
        if "largest number of first-in-class target-pair combinations across all years" in q:
            return "TOP_ORIGIN_BY_FIRST_DISCLOSURE_ALL_YEARS"
        if "largest number of first-in-class target-pair combinations" in q and "2024" in q:
            return "TOP_ORIGIN_BY_FIRST_DISCLOSURE_2024"
        if "greatest diversity of target pairs in their patent filings" in q:
            return "TOP_ORIGIN_BY_DIVERSITY"
        if "ranked by target-pair diversity" in q:
            return "ORIGIN_DIVERSITY_RANKING"

        if ("patent count" in q or "how many" in q) and "target pair" in q and "before" in q:
            return "TARGETPAIR_COUNT_BEFORE_YEAR"
        if ("patent count" in q or "how many" in q) and "target pair" in q and "after" in q:
            return "TARGETPAIR_COUNT_AFTER_YEAR"
        if "newly introduced" in q and "target pair" in q and "after" in q and "for " in q:
            return "NEW_TARGETPAIRS_BY_ASSIGNEE_AFTER_YEAR"
        if "no filings after" in q and "target pairs" in q and "for " in q:
            return "HISTORICAL_TARGETPAIRS_NO_FILINGS_AFTER_YEAR_BY_ASSIGNEE"
        if "partner targets" in q and "newly emerging" in q and "after" in q and "for " in q:
            return "PARTNER_TARGETS_NEW_AFTER_YEAR_BY_TARGET"
        if "new entrant" in q and "companies" in q and "after" in q and ("for uk" in q or "for " in q):
            return "NEW_ENTRANT_COMPANIES_BY_ORIGIN_AFTER_YEAR"
        if "top 10 partner targets for" in q and "co-occurrence" in q:
            return "TOP_PARTNER_TARGETS_BY_TARGET"
        if "top 20 patents" in q and "publication" in q and ("for target pair" in q or "target pair" in q):
            return "TOP_PATENTS_BY_TARGETPAIR"
        if ("top 10 target pairs" in q and "patent count" in q) or ("highest number" in q and "target pair" in q and "overall" in q):
            return "TOP_TARGETPAIRS_BY_PATENT_COUNT"
        if "top 10 countries" in q and ("patent publication count" in q or "patent count" in q):
            return "TOP_ORIGINS_BY_PATENT_COUNT"
        if "late entrants" in q and "target pair" in q and "after" in q:
            return "LATE_ENTRANT_ASSIGNEES_BY_TARGETPAIR_AFTER_YEAR"
        if "assignee diversity" in q and "target pairs" in q and "after" in q:
            return "TARGETPAIRS_ASSIGNEE_DIVERSITY_GROWTH_AFTER_YEAR"
        if "sustained 3-year downward trend" in q and "involving" in q:
            return "COMPANIES_DOWNWARD_TREND_BY_TARGET"
        if "what rank does" in q and "among all assignees" in q:
            return "COMPANY_RANK_BY_PATENT_COUNT"
        if "most common filing/priority country" in q and "patents" in q:
            return "MOST_COMMON_ORIGIN_BY_ASSIGNEE"
        if ("publication country" in q or "in region" in q) and "how many" in q:
            return "PATENT_COUNT_BY_ASSIGNEE_AND_ORIGIN"
        if "top technology class" in q and "for" in q:
            if "target pair" in q:
                return "PATENT_COUNT_BY_TARGETPAIR_AND_FUNCTION"
            return "TOP_FUNCTIONS_BY_ASSIGNEE"
        if ("technology class" in q and "how many" in q) or ("technology class" in q and "patent count" in q):
            if "target pair" in q:
                return "PATENT_COUNT_BY_TARGETPAIR_AND_FUNCTION"
            return "PATENT_COUNT_BY_ASSIGNEE_AND_FUNCTION"

        if "how many" in q and "assigned to" in q and "in the dataset" in q:
            return "PATENT_COUNT_BY_ASSIGNEE"
        if "how many" in q and "published by" in q and re.search(r"\b(?:19|20)\d{2}\b", q):
            return "PATENT_COUNT_BY_ASSIGNEE_AND_YEAR"
        if "first publication year" in q and "for" in q:
            return "FIRST_PUBLICATION_YEAR_BY_ASSIGNEE"
        if any(x in q for x in ["most recent publication year", "latest publication year"]) and "for" in q:
            return "LATEST_PUBLICATION_YEAR_BY_ASSIGNEE"
        if "how many" in q and ("startyear" in q or "endyear" in q or "-" in q) and "does" in q:
            return "PATENT_COUNT_BY_ASSIGNEE_YEAR_RANGE"
        if "which year" in q and "publish the highest number" in q:
            return "PEAK_PUBLICATION_YEAR_BY_ASSIGNEE"
        if "distinct target pairs" in q and "assigned to" in q:
            return "DISTINCT_TARGETPAIR_COUNT_BY_ASSIGNEE"
        if "most frequent target pair" in q and "assigned to" in q:
            return "TOP_TARGETPAIR_BY_ASSIGNEE"
        if "how many patents" in q and "involving target pair" in q and "does" in q:
            return "PATENT_COUNT_BY_ASSIGNEE_AND_TARGETPAIR"
        if "how many bsab patents" in q and "have that involve" in q:
            return "PATENT_COUNT_BY_ASSIGNEE_AND_TARGET"
        if "top 10 targets appearing in" in q:
            return "TOP_TARGETS_BY_ASSIGNEE"
        if "yearly counts" in q and "for" in q:
            return "YEARLY_PATENT_COUNTS_BY_ASSIGNEE"
        if "total number of distinct target pairs" in q:
            return "DISTINCT_TARGETPAIR_COUNT_GLOBAL"
        if "top 10 targets by frequency" in q:
            return "TOP_TARGETS_GLOBAL"
        if "average number of patents per target pair" in q:
            return "AVERAGE_PATENTS_PER_TARGETPAIR"
        if "highest number of distinct assignees" in q:
            return "TARGETPAIR_WITH_MAX_ASSIGNEE_DIVERSITY"
        if "first publication year for target pair" in q:
            return "FIRST_PUBLICATION_BY_TARGETPAIR"
        if "most recent publication year for target pair" in q or "latest publication year for target pair" in q:
            return "LATEST_PUBLICATION_BY_TARGETPAIR"
        if "top 10 assignees" in q or "most patents" in q:
            return "TOP_ASSIGNEES_BY_PATENT_COUNT"
        return None

    def _run_intent_query(self, intent_name: str, question: str) -> Dict[str, Any]:
        extraction_debug: Dict[str, Any] = {"hits": {}, "missing": []}
        idef = self.registry.get(intent_name)
        if not idef:
            return {
                "intent": intent_name,
                "question": question,
                "params": {},
                "cypher": None,
                "rows": 0,
                "graph_results": [],
                "entity_extraction": extraction_debug,
            }

        params: Dict[str, Any] = {}
        start_year, end_year = self._extract_year_range(question)
        for key, schema in idef.params_schema.items():
            if key == "start_year" and start_year is not None:
                params[key] = start_year
                extraction_debug["hits"][key] = {"value": start_year, "matched_by": "regex_year_range", "score": 1.0, "category": "numeric"}
                continue
            if key == "end_year" and end_year is not None:
                params[key] = end_year
                extraction_debug["hits"][key] = {"value": end_year, "matched_by": "regex_year_range", "score": 1.0, "category": "numeric"}
                continue

            if key == "tp_name":
                tp = self.tp_resolver.resolve(question).tp_name
                if not tp:
                    v, m = self._extract_entity_param(question, key)
                    tp = self._normalize_tp_to_slash(v or "") if v else None
                    if not tp and v:
                        tp = v
                    if m:
                        extraction_debug["hits"][key] = {
                            "value": m.value,
                            "score": round(m.score, 4),
                            "matched_by": m.matched_by,
                            "category": m.category,
                        }
                if tp:
                    params[key] = tp
                    continue

            num = self._extract_numeric_param(question, key)
            if num is not None:
                params[key] = num
                extraction_debug["hits"][key] = {"value": num, "matched_by": "regex_numeric", "score": 1.0, "category": "numeric"}
                continue

            val, match = self._extract_entity_param(question, key)
            if val is not None:
                params[key] = val
                if match:
                    extraction_debug["hits"][key] = {
                        "value": match.value,
                        "score": round(match.score, 4),
                        "matched_by": match.matched_by,
                        "category": match.category,
                    }
                continue

            if schema.get("required", False):
                extraction_debug["missing"].append(key)
                return {
                    "intent": intent_name,
                    "question": question,
                    "params": params,
                    "cypher": idef.cypher,
                    "rows": 0,
                    "graph_results": [],
                    "missing": key,
                    "entity_extraction": extraction_debug,
                }

            if key == "years":
                params[key] = 3
            elif key == "min_count":
                params[key] = 50
            else:
                params[key] = None

        graph_results = self.runner.run(idef.cypher, params)
        return {
            "intent": intent_name,
            "question": question,
            "params": params,
            "cypher": idef.cypher,
            "rows": len(graph_results),
            "graph_results": graph_results,
            "entity_extraction": extraction_debug,
        }


class AutoCypherOrchestrator(_GraphBaseOrchestrator):
    """LLM + auto-generated Cypher only; no fallback to preset intents."""

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
    ):
        super().__init__(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        self._official_retrievers: Dict[str, Any] = {}
        self._official_init_errors: Dict[str, str] = {}

    def _get_official_retriever(self, custom_prompt: str) -> Optional[Any]:
        if custom_prompt in self._official_retrievers:
            return self._official_retrievers[custom_prompt]
        if custom_prompt in self._official_init_errors:
            return None
        retriever, err = _build_official_text2cypher_retriever(
            runner=self.runner,
            llm=self.llm,
            schema_text=self._build_schema_text(),
            custom_prompt=custom_prompt,
        )
        if retriever is None:
            self._official_init_errors[custom_prompt] = err or "official_text2cypher_init_failed"
            return None
        self._official_retrievers[custom_prompt] = retriever
        return retriever

    def _select_official_prompts(self, question: str, max_prompts: int = 2) -> List[str]:
        q = (question or "").lower()
        prompts = [_OFFICIAL_T2C_PROMPT_BASE]
        if any(k in q for k in ["how many", "count", "number of"]):
            prompts.append(_OFFICIAL_T2C_PROMPT_COUNT)
        if any(k in q for k in ["top", "rank", "most", "least", "trend", "yearly", "over time"]):
            prompts.append(_OFFICIAL_T2C_PROMPT_TREND)
        uniq: List[str] = []
        for p in prompts:
            if p not in uniq:
                uniq.append(p)
        return uniq[:max_prompts]

    def _official_candidates(self, question: str, max_prompts: int = 2) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            from neo4j_graphrag.generation.prompts import Text2CypherTemplate
            from neo4j_graphrag.retrievers.text2cypher import extract_cypher
        except Exception:
            return out

        for prompt_text in self._select_official_prompts(question, max_prompts=max_prompts):
            retriever = self._get_official_retriever(prompt_text)
            if retriever is None:
                continue
            try:
                prompt_template = Text2CypherTemplate(template=retriever.custom_prompt)
                prompt = prompt_template.format(
                    schema=retriever.neo4j_schema,
                    examples="",
                    query_text=question,
                )
                llm_result = retriever.llm.invoke(prompt)
                cypher = extract_cypher((llm_result.content or "").strip())
                if cypher:
                    out.append({"cypher": cypher, "params": {}, "source": "official_text2cypher"})
            except Exception:
                continue
        return out

    def _heuristic_candidates(self, question: str) -> List[Dict[str, Any]]:
        q = (question or "").lower()
        out: List[Dict[str, Any]] = []

        assignee, _ = self._extract_entity_param(question, "assignee")
        tp_name = self.tp_resolver.resolve(question).tp_name
        if not tp_name:
            v, _ = self._extract_entity_param(question, "tp_name")
            tp_name = self._normalize_tp_to_slash(v or "") if v else None

        year = self._extract_year(question)
        start_year, end_year = self._extract_year_range(question)
        has_temporal_constraint = self._question_has_temporal_constraint(question)

        if assignee and "how many" in q:
            if (start_year is not None and end_year is not None) and has_temporal_constraint:
                out.append({
                    "cypher": "MATCH (a:Assignee {name:$assignee})<-[:HAS_ASSIGNEE]-(p:Patent)-[:PUBLISHED_IN]->(y:Year) WHERE y.year >= $start_year AND y.year <= $end_year RETURN a.name AS assignee, $start_year AS start_year, $end_year AS end_year, count(DISTINCT p) AS patent_count LIMIT 1;",
                    "params": {"assignee": assignee, "start_year": start_year, "end_year": end_year},
                    "source": "heuristic",
                })
            if year is not None and "published" in q:
                out.append({
                    "cypher": "MATCH (a:Assignee {name:$assignee})<-[:HAS_ASSIGNEE]-(p:Patent)-[:PUBLISHED_IN]->(y:Year {year:$year}) RETURN a.name AS assignee, y.year AS year, count(DISTINCT p) AS patent_count LIMIT 1;",
                    "params": {"assignee": assignee, "year": year},
                    "source": "heuristic",
                })
            out.append({
                "cypher": "MATCH (a:Assignee {name:$assignee})<-[:HAS_ASSIGNEE]-(p:Patent) RETURN a.name AS assignee, count(DISTINCT p) AS patent_count LIMIT 1;",
                "params": {"assignee": assignee},
                "source": "heuristic",
            })

        if tp_name and "how many" in q:
            out.append({
                "cypher": "MATCH (tp:TargetPair {name:$tp_name})<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN tp.name AS target_pair, count(DISTINCT p) AS patent_count LIMIT 1;",
                "params": {"tp_name": tp_name},
                "source": "heuristic",
            })

        if tp_name and any(k in q for k in ["first publication year", "earliest"]):
            out.append({
                "cypher": "MATCH (tp:TargetPair {name:$tp_name})<-[:HAS_TARGET_PAIR]-(p:Patent)-[:PUBLISHED_IN]->(y:Year) RETURN tp.name AS target_pair, min(y.year) AS first_year, count(DISTINCT p) AS patent_count LIMIT 1;",
                "params": {"tp_name": tp_name},
                "source": "heuristic",
            })

        if tp_name and any(k in q for k in ["latest", "most recent"]):
            out.append({
                "cypher": "MATCH (tp:TargetPair {name:$tp_name})<-[:HAS_TARGET_PAIR]-(p:Patent)-[:PUBLISHED_IN]->(y:Year) RETURN tp.name AS target_pair, max(y.year) AS latest_year, count(DISTINCT p) AS patent_count LIMIT 1;",
                "params": {"tp_name": tp_name},
                "source": "heuristic",
            })

        return out
    def _generate_candidate_cyphers(self, question: str, k: int = 4) -> List[Dict[str, Any]]:
        schema_text = self._build_schema_text()
        system_prompt = f"""
Generate {k} candidate read-only Cypher queries for the question.
Be concise, avoid over-constraining to one template.

Schema:
{schema_text}

Output strict JSON list:
[
  {{"cypher":"...","params":{{...}},"rationale":"short"}}
]

Rules:
- Use only MATCH/OPTIONAL MATCH/WHERE/WITH/RETURN/ORDER BY/LIMIT
- Must include LIMIT <= 50
- No CREATE/MERGE/DELETE/SET/CALL/APOC/LOAD CSV
"""
        user_prompt = f"Question: {question}"
        text = self.llm.chat(system_prompt.strip(), user_prompt, temperature=0.1)
        text = re.sub(r"^```json\s*|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()

        out: List[Dict[str, Any]] = []
        out.extend(self._heuristic_candidates(question))
        out.extend(self._official_candidates(question, max_prompts=max(1, min(3, k))))
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                obj = obj.get("candidates", [])
            if isinstance(obj, list):
                for it in obj[:k]:
                    if isinstance(it, dict):
                        cypher = str(it.get("cypher") or "").strip()
                        params = it.get("params") if isinstance(it.get("params"), dict) else {}
                        if cypher:
                            out.append({"cypher": cypher, "params": params, "source": "candidate"})
        except Exception:
            pass

        if not out:
            # Single-shot fallback inside auto path (still no intent fallback)
            system_prompt_single = f"""
Generate one read-only Cypher query for the question.
Schema:
{schema_text}
Output strict JSON: {{"cypher":"...","params":{{...}}}}
Always include LIMIT <= 50.
"""
            text2 = self.llm.chat(system_prompt_single.strip(), user_prompt, temperature=0.0)
            text2 = re.sub(r"^```json\s*|\s*```$", "", text2.strip(), flags=re.MULTILINE).strip()
            self._last_nl2cypher_raw = text2
            try:
                obj2 = json.loads(text2)
                cypher = str(obj2.get("cypher") or "").strip()
                params = obj2.get("params") if isinstance(obj2.get("params"), dict) else {}
                if cypher:
                    out.append({"cypher": cypher, "params": params, "source": "single"})
            except Exception:
                pass

        # de-dup by normalized cypher + params
        uniq: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for it in out:
            cypher = str(it.get("cypher") or "").strip()
            if not cypher:
                continue
            params = it.get("params") if isinstance(it.get("params"), dict) else {}
            key = re.sub(r"\s+", " ", cypher.strip().rstrip(";")).lower() + "::" + json.dumps(params, ensure_ascii=False, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            uniq.append({"cypher": cypher, "params": params, "source": it.get("source", "candidate")})
        return uniq

    def _score_candidate(
        self,
        question: str,
        cypher: str,
        params: Dict[str, Any],
        rows: List[Dict[str, Any]],
        source: str = "candidate",
    ) -> float:
        score = 0.0
        q = (question or "").lower()
        c = (cypher or "").lower()

        if rows:
            score += 2.0
        if len(rows) > 3:
            score += 0.5

        # preference: aggregation for count questions
        if "how many" in q and ("count(" in c):
            score += 1.0
        if any(k in q for k in ["before", "after", "since", "until", "between", "from", "to", "prior to"]):
            if "published_in" in c and "y.year" in c and "where" in c:
                score += 1.2
            elif "published_in" in c and "y.year" in c:
                score += 0.6
        if any(k in q for k in ["year", "trend", "over time"]) and "year" in c:
            score += 0.5
        if any(k in q for k in ["company", "assignee"]) and "assignee" in c:
            score += 0.5
        if any(k in q for k in ["target pair", "targetpair"]) and "targetpair" in c:
            score += 0.5

        # reward parameterized entity filters
        if "$" in cypher:
            score += 0.3
        if params:
            score += 0.2
        if source == "official_text2cypher":
            score += 0.15
        return score

    def _best_cypher(self, question: str) -> Optional[Dict[str, Any]]:
        candidates = self._generate_candidate_cyphers(question)
        best: Optional[Dict[str, Any]] = None

        for item in candidates:
            cypher = item.get("cypher", "")
            params = item.get("params", {}) or {}
            if re.search(r"\bLIMIT\b", cypher, flags=re.IGNORECASE) is None:
                cypher = cypher.rstrip(";") + "\nLIMIT 50;"
            try:
                self._validate_cypher_strict(cypher)
                rows = self.runner.run(cypher, params)
                src = str(item.get("source", "candidate"))
                s = self._score_candidate(question, cypher, params, rows, source=src)
                pack = {
                    "cypher": cypher,
                    "params": params,
                    "rows": rows,
                    "score": s,
                    "source": src,
                }
                if best is None or pack["score"] > best["score"]:
                    best = pack
            except Exception:
                continue
        return best

    def answer(self, question: str, mode: str = "auto_cypher") -> AnswerBundle:
        best = self._best_cypher(question)
        debug: Dict[str, Any] = {"mode": "auto_cypher"}
        if not best:
            return AnswerBundle(
                mode="auto_cypher",
                answer="Unable to recognize the query and generate a safe Cypher.",
                debug=debug,
            )

        debug.update(
            {
                "cypher": best["cypher"],
                "params": best["params"],
                "rows": len(best["rows"]),
                "graph_results": best["rows"],
                "candidate_score": best["score"],
                "candidate_source": best["source"],
            }
        )
        ans = self.synth.answer_with_graph(question, "AUTO_CYPHER", best["rows"])
        return AnswerBundle(mode="auto_cypher", answer=ans, debug=debug)


class Neo4j_Text2CypherRetriever(_GraphBaseOrchestrator):
    """Neo4j official Text2Cypher stack baseline (single-shot)."""

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
    ):
        super().__init__(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        self._official_retriever: Any = None
        self._official_init_error: Optional[str] = None

    def _init_official_text2cypher(self) -> None:
        if self._official_retriever is not None or self._official_init_error:
            return
        retriever, err = _build_official_text2cypher_retriever(
            runner=self.runner,
            llm=self.llm,
            schema_text=self._build_schema_text(),
            custom_prompt=_OFFICIAL_T2C_PROMPT_BASE,
        )
        if retriever is None:
            self._official_init_error = err or "official_text2cypher_init_failed"
            return
        self._official_retriever = retriever

    def _generate_once(self, question: str) -> Optional[Dict[str, Any]]:
        self._init_official_text2cypher()
        if self._official_retriever is None:
            return None

        try:
            from neo4j_graphrag.generation.prompts import Text2CypherTemplate
            from neo4j_graphrag.retrievers.text2cypher import extract_cypher

            prompt_template = Text2CypherTemplate(template=self._official_retriever.custom_prompt)
            prompt = prompt_template.format(
                schema=self._official_retriever.neo4j_schema,
                examples="",
                query_text=question,
            )
            llm_result = self._official_retriever.llm.invoke(prompt)
            cypher = extract_cypher((llm_result.content or "").strip())

            if cypher:
                if re.search(r"\bLIMIT\b", cypher, flags=re.IGNORECASE) is None:
                    cypher = cypher.rstrip(";") + "\nLIMIT 50;"
                return {"cypher": cypher, "params": {}}
        except Exception:
            return None
        return None

    def answer(self, question: str, mode: str = "neo4j_text2cypher_retriever") -> AnswerBundle:
        pack = self._generate_once(question)
        debug: Dict[str, Any] = {
            "mode": "neo4j_text2cypher_retriever",
            "text2cypher_backend": "neo4j_graphrag_official",
        }
        if self._official_init_error:
            debug["official_init_error"] = self._official_init_error
        if not pack:
            return AnswerBundle(mode="neo4j_text2cypher_retriever", answer="Unable to recognize the query and generate a safe Cypher.", debug=debug)

        cypher = pack["cypher"]
        params = pack.get("params") or {}
        try:
            self._validate_cypher_strict(cypher)
            rows = self.runner.run(cypher, params)
        except Exception:
            return AnswerBundle(mode="neo4j_text2cypher_retriever", answer="Unable to recognize the query and generate a safe Cypher.", debug=debug)

        debug.update({"cypher": cypher, "params": params, "rows": len(rows), "graph_results": rows})
        ans = self.synth.answer_with_graph(question, "NEO4J_TEXT2CYPHER_RETRIEVER", rows)
        return AnswerBundle(mode="neo4j_text2cypher_retriever", answer=ans, debug=debug)


AutoCypherV2Orchestrator = Neo4j_Text2CypherRetriever


class QueryRewritingNeo4jText2CypherRetriever(Neo4j_Text2CypherRetriever):
    """Rewrite the user question first, then run official Neo4j Text2Cypher once."""

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
        rewrite_model: Optional[str] = None,
    ):
        super().__init__(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        client = getattr(llm, "_client", None)
        base_url = str(getattr(client, "base_url", "") or "https://api.openai.com/v1")
        rewrite_model_name = rewrite_model or getattr(llm, "_model", None) or os.getenv("OPENAI_MODEL", "gpt-5.4")
        self.query_rewriter = QueryRewriter(
            base_url=base_url,
            api_key_env="OPENAI_API_KEY",
            model=rewrite_model_name,
            temperature=0.0,
            timeout_s=180.0,
            max_retries=3,
            retry_backoff_s=2.0,
        )
        self.rewrite_model = rewrite_model_name

    def _run_cypher_with_retry(self, cypher: str, params: Dict[str, Any], attempts: int = 3) -> List[Dict[str, Any]]:
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return self.runner.run(cypher, params)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < attempts - 1:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                break
        raise last_exc or RuntimeError("Unknown Neo4j execution failure")

    def answer(self, question: str, mode: str = "query_rewriting_neo4j_text2cypher_retriever") -> AnswerBundle:
        debug: Dict[str, Any] = {
            "mode": "query_rewriting_neo4j_text2cypher_retriever",
            "text2cypher_backend": "neo4j_graphrag_official",
            "rewrite_model": self.rewrite_model,
            "text2cypher_model": getattr(self.llm, "_model", ""),
            "original_question": question,
        }
        if self._official_init_error:
            debug["official_init_error"] = self._official_init_error

        try:
            rewrite = self.query_rewriter.rewrite(question)
            rewritten_question = rewrite.rewritten_question or question
            debug.update(
                {
                    "rewritten_question": rewritten_question,
                    "rewrite_template_signature": rewrite.template_signature,
                    "rewrite_entity_slots": rewrite.entity_slots,
                    "rewrite_clarification_needed": rewrite.clarification_needed,
                    "rewrite_clarification_note": rewrite.clarification_note,
                    "rewrite_raw_response": rewrite.raw_response,
                }
            )
        except Exception as exc:  # noqa: BLE001
            debug["rewrite_error"] = f"{type(exc).__name__}: {exc}"
            return AnswerBundle(
                mode="query_rewriting_neo4j_text2cypher_retriever",
                answer="Unable to rewrite the query into a schema-aligned form for Text2Cypher.",
                debug=debug,
            )

        pack = self._generate_once(rewritten_question)
        if not pack:
            return AnswerBundle(
                mode="query_rewriting_neo4j_text2cypher_retriever",
                answer="Unable to recognize the rewritten query and generate a safe Cypher.",
                debug=debug,
            )

        cypher = pack["cypher"]
        params = pack.get("params") or {}
        try:
            self._validate_cypher_strict(cypher)
            rows = self._run_cypher_with_retry(cypher, params)
        except Exception as exc:  # noqa: BLE001
            debug["execution_error"] = f"{type(exc).__name__}: {exc}"
            return AnswerBundle(
                mode="query_rewriting_neo4j_text2cypher_retriever",
                answer="Unable to recognize the rewritten query and generate a safe Cypher.",
                debug=debug,
            )

        debug.update({"cypher": cypher, "params": params, "rows": len(rows), "graph_results": rows})
        ans = self.synth.answer_with_graph(question, "QUERY_REWRITING_NEO4J_TEXT2CYPHER_RETRIEVER", rows)
        return AnswerBundle(mode="query_rewriting_neo4j_text2cypher_retriever", answer=ans, debug=debug)


class HybridIntentCypherOrchestrator(_GraphBaseOrchestrator):
    """LLM + preset intent + Cypher fallback path."""

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        enable_nl2cypher_fallback: bool = True,
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
    ):
        super().__init__(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        self.enable_nl2cypher_fallback = enable_nl2cypher_fallback
        self._auto_delegate = AutoCypherOrchestrator(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )

    def _mentions_entity(self, question: str, category: str) -> bool:
        if self.extractor is None:
            self._init_extractor(max_retries=1)
        if not self.extractor:
            return False
        try:
            hit = self.ner.match_first(question, category)
            if not hit:
                return False
            # Reduce false positives from broad fuzzy matches in long questions.
            if hit.matched_by in {"exact", "containment", "acronym"}:
                return True
            return float(hit.score) >= 0.92
        except Exception:
            return False

    def _intent_looks_mismatched(self, question: str, intent_name: str) -> Tuple[bool, str]:
        q = (question or "").lower()
        idef = self.registry.get(intent_name)
        cypher = (idef.cypher if idef else "") or ""
        params_schema = idef.params_schema if idef else {}

        has_time_kw = self._question_has_temporal_constraint(question)
        asks_targetpair = ("target pair" in q) or bool(re.search(r"[a-z0-9\-_]+/[a-z0-9\-_]+", q))
        asks_compare = any(k in q for k in ["compare", "vs", "versus", "before and after", "increased", "decreased", "declining", "growth", "new entrant"])
        mentions_assignee = self._mentions_entity(question, "assignee")
        mentions_origin = self._mentions_entity(question, "origin")
        mentions_target = self._mentions_entity(question, "target")
        asks_assignee_constraint = any(k in q for k in [" assignee", "company", "assigned to", "published by", "for "])
        asks_origin_constraint = any(k in q for k in ["country", "origin", "region", "domestic", "foreign"])

        cypher_has_year = "PUBLISHED_IN" in cypher or "y.year" in cypher
        intent_has_year_param = ("year" in params_schema) or ("start_year" in params_schema) or ("end_year" in params_schema) or ("years" in params_schema)
        intent_has_tp_param = ("tp_name" in params_schema)
        intent_has_assignee = ("assignee" in params_schema) or ("Assignee" in cypher) or ("HAS_ASSIGNEE" in cypher)
        intent_has_origin = ("origin" in params_schema) or ("Origin" in cypher) or ("ORIGIN_FROM" in cypher)
        intent_has_target = any(k in params_schema for k in ["target", "target1", "target2"]) or ("Target " in cypher) or ("Target)" in cypher) or ("HAS_TARGET" in cypher)

        # TargetPair question but selected intent cannot bind/operate on target-pair semantics.
        if asks_targetpair and not (intent_has_tp_param or "TargetPair" in cypher):
            return True, "question_mentions_targetpair_but_intent_lacks_targetpair_semantics"

        # Entity constraints in question should appear in selected intent semantics.
        if asks_assignee_constraint and mentions_assignee and not intent_has_assignee:
            return True, "question_mentions_assignee_but_intent_lacks_assignee_constraint"
        if asks_origin_constraint and mentions_origin and not intent_has_origin:
            return True, "question_mentions_origin_but_intent_lacks_origin_constraint"
        if (mentions_target and not asks_targetpair) and not intent_has_target:
            return True, "question_mentions_target_but_intent_lacks_target_constraint"

        # Explicit temporal constraint but selected intent does not have year semantics.
        if has_time_kw and not (cypher_has_year and intent_has_year_param):
            return True, "question_mentions_time_constraint_but_intent_lacks_year_semantics"

        # Comparative trend-like questions are usually not single-point fact intents.
        if asks_compare and intent_name in {
            "PATENT_COUNT_BY_ASSIGNEE",
            "PATENT_COUNT_BY_ASSIGNEE_AND_YEAR",
            "PATENT_COUNT_BY_ASSIGNEE_YEAR_RANGE",
            "PATENT_COUNT_BY_ASSIGNEE_AND_TARGETPAIR",
            "TOP_TARGETPAIR_BY_ASSIGNEE",
            "COMPANIES_BY_TARGETPAIR",
        }:
            return True, "question_is_comparative_or_trend_like_but_intent_is_single_point_fact"

        return False, ""

    def answer(self, question: str, mode: str = "kg") -> AnswerBundle:
        parsed = self.classify_intent(question)
        intent_name = str(parsed.get("intent") or "UNKNOWN")
        heur_intent = self._heuristic_intent(question)
        force_heuristic_intents = {
            "TARGETPAIR_COUNT_BEFORE_YEAR",
            "TARGETPAIR_COUNT_AFTER_YEAR",
            "NEW_TARGETPAIRS_BY_ASSIGNEE_AFTER_YEAR",
            "HISTORICAL_TARGETPAIRS_NO_FILINGS_AFTER_YEAR_BY_ASSIGNEE",
            "PARTNER_TARGETS_NEW_AFTER_YEAR_BY_TARGET",
            "NEW_ENTRANT_COMPANIES_BY_ORIGIN_AFTER_YEAR",
            "TOP_PARTNER_TARGETS_BY_TARGET",
            "TOP_PATENTS_BY_TARGETPAIR",
            "TOP_TARGETPAIRS_BY_PATENT_COUNT",
            "TOP_ORIGINS_BY_PATENT_COUNT",
            "LATE_ENTRANT_ASSIGNEES_BY_TARGETPAIR_AFTER_YEAR",
            "TARGETPAIRS_ASSIGNEE_DIVERSITY_GROWTH_AFTER_YEAR",
            "COMPANIES_DOWNWARD_TREND_BY_TARGET",
            "COMPANY_RANK_BY_PATENT_COUNT",
            "MOST_COMMON_ORIGIN_BY_ASSIGNEE",
            "PATENT_COUNT_BY_ASSIGNEE_AND_ORIGIN",
            "TOP_FUNCTIONS_BY_ASSIGNEE",
            "PATENT_COUNT_BY_ASSIGNEE_AND_FUNCTION",
            "PATENT_COUNT_BY_TARGETPAIR_AND_FUNCTION",
        }
        if intent_name == "UNKNOWN":
            intent_name = heur_intent or "UNKNOWN"
        elif heur_intent in force_heuristic_intents:
            intent_name = heur_intent

        debug: Dict[str, Any] = {
            "mode": "hybrid_intent_cypher",
            "parsed": parsed,
            "intent": intent_name,
        }

        mismatch, mismatch_reason = self._intent_looks_mismatched(question, intent_name)
        if mismatch and self.enable_nl2cypher_fallback:
            auto_bundle = self._auto_delegate.answer(question)
            auto_debug = auto_bundle.debug if isinstance(auto_bundle.debug, dict) else {}
            debug["fallback"] = "auto_cypher"
            debug["fallback_reason"] = mismatch_reason
            debug["fallback_debug"] = auto_debug
            debug["cypher"] = auto_debug.get("cypher")
            debug["params"] = auto_debug.get("params")
            debug["rows"] = auto_debug.get("rows")
            debug["graph_results"] = auto_debug.get("graph_results")
            debug["final_intent"] = auto_debug.get("intent") or auto_debug.get("intent_name") or "AUTO_CYPHER"
            return AnswerBundle(mode="hybrid_intent_cypher", answer=auto_bundle.answer, debug=debug)

        result = self._run_intent_query(intent_name, question)
        debug.update(
            {
                "params": result.get("params"),
                "entity_extraction": result.get("entity_extraction"),
                "cypher": result.get("cypher"),
                "rows": result.get("rows"),
                "graph_results": result.get("graph_results"),
            }
        )

        if result.get("missing"):
            if self.enable_nl2cypher_fallback:
                auto_bundle = self._auto_delegate.answer(question)
                auto_debug = auto_bundle.debug if isinstance(auto_bundle.debug, dict) else {}
                debug["fallback"] = "auto_cypher"
                debug["fallback_reason"] = f"missing_required_param:{result['missing']}"
                debug["fallback_debug"] = auto_debug
                debug["cypher"] = auto_debug.get("cypher") or debug.get("cypher")
                debug["params"] = auto_debug.get("params") or debug.get("params")
                debug["rows"] = auto_debug.get("rows") if auto_debug.get("rows") is not None else debug.get("rows")
                debug["graph_results"] = auto_debug.get("graph_results") or debug.get("graph_results")
                debug["final_intent"] = auto_debug.get("intent") or auto_debug.get("intent_name") or debug.get("intent")
                return AnswerBundle(mode="hybrid_intent_cypher", answer=auto_bundle.answer, debug=debug)
            ans = self._clarify_missing(str(result["missing"]), intent_name)
            return AnswerBundle(mode="hybrid_intent_cypher", answer=ans, debug=debug)

        if result.get("cypher"):
            ans = self.synth.answer_with_graph(question, intent_name, result.get("graph_results") or [])
            return AnswerBundle(mode="hybrid_intent_cypher", answer=ans, debug=debug)

        if self.enable_nl2cypher_fallback:
            auto_bundle = self._auto_delegate.answer(question)
            auto_debug = auto_bundle.debug if isinstance(auto_bundle.debug, dict) else {}
            debug["fallback"] = "auto_cypher"
            debug["fallback_debug"] = auto_debug
            debug["cypher"] = auto_debug.get("cypher") or debug.get("cypher")
            debug["params"] = auto_debug.get("params") or debug.get("params")
            debug["rows"] = auto_debug.get("rows") if auto_debug.get("rows") is not None else debug.get("rows")
            debug["graph_results"] = auto_debug.get("graph_results") or debug.get("graph_results")
            debug["final_intent"] = auto_debug.get("intent") or auto_debug.get("intent_name") or debug.get("intent")
            if auto_bundle.answer.startswith("Unable to recognize"):
                return AnswerBundle(
                    mode="hybrid_intent_cypher",
                    answer="Unable to map the question to a supported preset intent and failed to generate safe Cypher.",
                    debug=debug,
                )
            return AnswerBundle(mode="hybrid_intent_cypher", answer=auto_bundle.answer, debug=debug)

        return AnswerBundle(
            mode="hybrid_intent_cypher",
            answer="Unable to map the question to a supported preset intent.",
            debug=debug,
        )


class QueryRewritingHybridIntentCypherOrchestrator(HybridIntentCypherOrchestrator):
    """Rewrite the user question first, then route through HybridIntent."""

    def __init__(
        self,
        runner: Neo4jRunner,
        registry: IntentRegistry,
        llm: LLMProvider,
        props: Dict[str, str],
        enable_nl2cypher_fallback: bool = True,
        temperature_intent: float = 0.0,
        temperature_answer: float = 0.2,
        temperature_no_kg: float = 0.2,
        rewrite_model: Optional[str] = None,
    ):
        super().__init__(
            runner=runner,
            registry=registry,
            llm=llm,
            props=props,
            enable_nl2cypher_fallback=enable_nl2cypher_fallback,
            temperature_intent=temperature_intent,
            temperature_answer=temperature_answer,
            temperature_no_kg=temperature_no_kg,
        )
        client = getattr(llm, "_client", None)
        base_url = str(getattr(client, "base_url", "") or "https://api.openai.com/v1")
        rewrite_model_name = rewrite_model or getattr(llm, "_model", None) or os.getenv("OPENAI_MODEL", "gpt-5.4")
        self.query_rewriter = QueryRewriter(
            base_url=base_url,
            api_key_env="OPENAI_API_KEY",
            model=rewrite_model_name,
            temperature=0.0,
            timeout_s=180.0,
            max_retries=3,
            retry_backoff_s=2.0,
        )
        self.rewrite_model = rewrite_model_name

    def answer(self, question: str, mode: str = "query_rewriting_hybrid_intent_cypher") -> AnswerBundle:
        debug: Dict[str, Any] = {
            "mode": "query_rewriting_hybrid_intent_cypher",
            "rewrite_model": self.rewrite_model,
            "hybrid_model": getattr(self.llm, "_model", ""),
            "original_question": question,
        }
        try:
            rewrite = self.query_rewriter.rewrite(question)
            rewritten_question = rewrite.rewritten_question or question
            debug.update(
                {
                    "rewritten_question": rewritten_question,
                    "rewrite_template_signature": rewrite.template_signature,
                    "rewrite_entity_slots": rewrite.entity_slots,
                    "rewrite_clarification_needed": rewrite.clarification_needed,
                    "rewrite_clarification_note": rewrite.clarification_note,
                    "rewrite_raw_response": rewrite.raw_response,
                }
            )
        except Exception as exc:  # noqa: BLE001
            debug["rewrite_error"] = f"{type(exc).__name__}: {exc}"
            return AnswerBundle(
                mode="query_rewriting_hybrid_intent_cypher",
                answer="Unable to rewrite the query into a schema-aligned form for HybridIntent.",
                debug=debug,
            )

        inner = super().answer(rewritten_question, mode="kg")
        inner_debug = inner.debug if isinstance(inner.debug, dict) else {}
        debug.update(inner_debug)

        graph_results = inner_debug.get("graph_results")
        intent_name = inner_debug.get("final_intent") or inner_debug.get("intent") or "QUERY_REWRITING_HYBRID_INTENT"
        if isinstance(graph_results, list) and inner_debug.get("cypher"):
            try:
                ans = self.synth.answer_with_graph(question, intent_name, graph_results)
                return AnswerBundle(mode="query_rewriting_hybrid_intent_cypher", answer=ans, debug=debug)
            except Exception:
                pass
        return AnswerBundle(mode="query_rewriting_hybrid_intent_cypher", answer=inner.answer, debug=debug)


# Backward-compatible alias.
Orchestrator = HybridIntentCypherOrchestrator










