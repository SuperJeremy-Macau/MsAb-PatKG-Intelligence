from __future__ import annotations

from typing import Dict, List
import json
import re

from bsab_kg_qa_en.extract import NodeCatalog
from bsab_kg_qa_en.interaction.state_models import CandidateOption


class EntityLookup:
    """Read-only entity candidate lookup backed by live Neo4j values plus optional LLM reranking."""

    CATEGORY_SPECS: Dict[str, tuple[str, str]] = {
        "assignee": ("Assignee", "name"),
        "cancer": ("Cancer", "name"),
        "functional_of_target": ("Functional_of_Target", "name"),
        "origin": ("Origin", "name"),
        "pathway": ("Pathway", "name"),
        "target": ("Target", "symbol"),
        "target_pair": ("TargetPair", "name"),
        "technologyclass1": ("TechnologyClass1", "name"),
    }

    CANCER_GLOSSES: Dict[str, list[str]] = {
        "ACC": ["adrenocortical carcinoma", "adrenal cancer"],
        "BLCA": ["bladder cancer", "bladder carcinoma", "urothelial carcinoma"],
        "BRCA": ["breast cancer", "breast carcinoma", "mammary carcinoma"],
        "CESC": ["cervical cancer", "cervical squamous cell carcinoma", "endocervical adenocarcinoma"],
        "CHOL": ["cholangiocarcinoma", "bile duct cancer"],
        "COAD": ["colon adenocarcinoma", "colon cancer"],
        "DLBC": ["diffuse large b-cell lymphoma", "large b cell lymphoma"],
        "ESCA": ["esophageal carcinoma", "esophageal cancer"],
        "GBM": ["glioblastoma", "glioblastoma multiforme", "brain cancer"],
        "HNSC": ["head and neck squamous cell carcinoma", "head and neck cancer"],
        "KICH": ["kidney chromophobe", "kidney cancer"],
        "KIRC": ["kidney renal clear cell carcinoma", "renal clear cell carcinoma", "kidney cancer"],
        "KIRP": ["kidney renal papillary cell carcinoma", "renal papillary carcinoma", "kidney cancer"],
        "LAML": ["acute myeloid leukemia", "aml", "leukemia"],
        "LGG": ["lower grade glioma", "glioma", "brain tumor"],
        "LIHC": ["liver hepatocellular carcinoma", "liver cancer", "hepatocellular carcinoma"],
        "LUAD": ["lung adenocarcinoma", "lung cancer", "adenocarcinoma of the lung"],
        "LUSC": ["lung squamous cell carcinoma", "lung cancer", "squamous lung cancer"],
        "MESO": ["mesothelioma", "malignant mesothelioma"],
        "OV": ["ovarian cancer", "ovarian carcinoma"],
        "PAAD": ["pancreatic adenocarcinoma", "pancreatic cancer"],
        "PCPG": ["pheochromocytoma", "paraganglioma"],
        "PRAD": ["prostate adenocarcinoma", "prostate cancer"],
        "READ": ["rectum adenocarcinoma", "rectal cancer"],
        "SARC": ["sarcoma", "soft tissue sarcoma"],
        "SKCM": ["skin cutaneous melanoma", "melanoma", "skin cancer"],
        "STAD": ["stomach adenocarcinoma", "stomach cancer", "gastric cancer"],
        "TGCT": ["testicular germ cell tumor", "testicular cancer"],
        "THCA": ["thyroid carcinoma", "thyroid cancer"],
        "UCEC": ["uterine corpus endometrial carcinoma", "endometrial cancer", "uterine cancer"],
        "UCS": ["uterine carcinosarcoma", "uterine sarcoma"],
    }

    CANCER_GENERIC_TOKENS = {
        "cancer", "carcinoma", "adenocarcinoma", "tumor", "tumour", "cell",
        "cells", "squamous", "renal", "corpus", "malignant", "acute",
    }

    ASSIGNEE_SUFFIX_TOKENS = {
        "inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation",
        "co", "company", "gmbh", "sa", "spa", "plc", "bv", "ag", "nv",
        "holdings", "holding", "biotech", "biopharma", "biopharmaceuticals",
        "pharma", "pharmaceuticals",
    }

    FUNCTIONAL_TOKEN_GLOSSES: Dict[str, str] = {
        "tme": "tumor microenvironment",
        "nk": "natural killer",
        "treg": "regulatory t cell",
        "ecm": "extracellular matrix",
    }

    PATHWAY_TOKEN_GLOSSES: Dict[str, str] = {
        "adme": "absorption distribution metabolism excretion",
        "mdr": "multidrug resistance",
        "nfe2l2": "nrf2",
        "nrf2": "nfe2l2",
    }

    TECHNOLOGYCLASS_TOKEN_GLOSSES: Dict[str, str] = {
        "tme": "tumor microenvironment",
        "pk": "pharmacokinetics",
        "pd": "pharmacodynamics",
        "piggbacking": "piggybacking",
        "combiantion": "combination",
    }

    ORIGIN_GLOSSES: Dict[str, list[str]] = {
        "China": ["china", "chinese"],
        "EU": ["europe", "european union", "european"],
        "JP": ["japan", "japanese"],
        "KR": ["korea", "south korea", "korean"],
        "UK": ["united kingdom", "britain", "british", "england"],
        "US": ["united states", "usa", "u s", "america", "american"],
        "Other": ["other region", "other regions", "others"],
        "Personal": ["individual", "person", "personal assignee"],
    }

    def __init__(self, runner, llm=None):
        self.runner = runner
        self.llm = llm
        self.catalog_error: str | None = None
        self._live_entry_cache: Dict[str, list[dict]] = {}
        try:
            self.catalog = NodeCatalog.from_neo4j(runner)
        except Exception as exc:  # noqa: BLE001
            self.catalog_error = f"{type(exc).__name__}: {exc}"
            self.catalog = NodeCatalog(values={category: [] for category in self.CATEGORY_SPECS})

    def search(self, category: str, raw_text: str | None, limit: int = 8) -> List[CandidateOption]:
        text = (raw_text or "").strip()
        if not text:
            return []

        direct = self._neo4j_candidates(category, text, limit=max(limit * 4, 12))
        if direct:
            llm_candidates = self._llm_rerank_candidates(category, text, direct, limit=limit)
            if llm_candidates:
                return llm_candidates
            return direct[:limit]

        entries = self._get_live_entries(category)
        llm_candidates = self._llm_rerank_candidates(category, text, entries, limit=limit)
        if llm_candidates:
            return llm_candidates

        return self._catalog_candidates(category, text, limit=limit)

    def _neo4j_candidates(self, category: str, raw_text: str, limit: int = 8) -> List[CandidateOption]:
        entries = self._get_live_entries(category)
        if not entries:
            return []

        q = raw_text.lower().strip()
        scored: list[tuple[float, dict]] = []
        for entry in entries:
            canonical = entry["canonical"]
            aliases = entry["aliases"]
            glosses = entry.get("glosses", [])
            lowered_canonical = canonical.lower()
            lowered_aliases = [alias.lower() for alias in aliases]
            lowered_glosses = [gloss.lower() for gloss in glosses]
            normalized_canonical = entry.get("normalized_canonical", "")
            normalized_aliases = entry.get("normalized_aliases", [])
            compact_canonical = entry.get("compact_canonical", "")
            compact_aliases = entry.get("compact_aliases", [])

            score = 0.0
            matched_on = "canonical"
            if lowered_canonical == q:
                score = 1.0
            elif q in lowered_canonical:
                score = 0.92
            else:
                alias_hit = next((alias for alias in lowered_aliases if alias == q), None)
                if alias_hit:
                    score = 0.95
                    matched_on = "alias"
                else:
                    alias_hit = next((alias for alias in lowered_aliases if q in alias), None)
                    if alias_hit:
                        score = 0.88
                        matched_on = "alias"
                    else:
                        gloss_hit = next((gloss for gloss in lowered_glosses if gloss == q), None)
                        if gloss_hit:
                            score = 0.9
                            matched_on = "gloss"
                        else:
                            gloss_hit = next((gloss for gloss in lowered_glosses if q in gloss), None)
                            if gloss_hit:
                                score = 0.84
                                matched_on = "gloss"
                            elif lowered_canonical.startswith(q[:3]) if len(q) >= 3 else False:
                                score = 0.65
            if category == "assignee" and score <= 0:
                assignee_score, assignee_match = self._score_assignee_semantics(
                    raw_text,
                    normalized_canonical,
                    normalized_aliases,
                    compact_canonical,
                )
                if assignee_score > 0:
                    score = assignee_score
                    matched_on = assignee_match
            if category == "target" and score <= 0:
                target_score, target_match = self._score_target_semantics(
                    raw_text,
                    normalized_canonical,
                    normalized_aliases,
                    compact_canonical,
                    compact_aliases,
                    glosses,
                )
                if target_score > 0:
                    score = target_score
                    matched_on = target_match
            if category == "target_pair" and score <= 0:
                target_pair_score, target_pair_match = self._score_target_pair_semantics(raw_text, entry)
                if target_pair_score > 0:
                    score = target_pair_score
                    matched_on = target_pair_match
            if category == "functional_of_target" and score <= 0:
                functional_score, functional_match = self._score_functional_target_semantics(raw_text, entry)
                if functional_score > 0:
                    score = functional_score
                    matched_on = functional_match
            if category == "pathway" and score <= 0:
                pathway_score, pathway_match = self._score_pathway_semantics(raw_text, entry)
                if pathway_score > 0:
                    score = pathway_score
                    matched_on = pathway_match
            if category == "technologyclass1" and score <= 0:
                tech_score, tech_match = self._score_technologyclass_semantics(raw_text, entry)
                if tech_score > 0:
                    score = tech_score
                    matched_on = tech_match
            if category == "origin" and score <= 0:
                origin_score, origin_match = self._score_origin_semantics(raw_text, entry)
                if origin_score > 0:
                    score = origin_score
                    matched_on = origin_match
            if category == "cancer" and score <= 0:
                semantic_score = self._score_cancer_semantics(q, entry)
                if semantic_score > 0:
                    score = semantic_score
                    matched_on = "semantic"

            if score <= 0:
                continue
            scored.append((score, {"canonical": canonical, "matched_on": matched_on, "entry": entry}))

        if category == "target_pair":
            for score, item in self._target_pair_component_recall(raw_text, entries):
                scored.append((score, item))

        scored.sort(key=lambda item: (-item[0], item[1]["canonical"]))
        out: List[CandidateOption] = []
        seen = set()
        for score, item in scored[:limit]:
            canonical = item["canonical"]
            if canonical in seen:
                continue
            reason = self._match_reason(item["entry"], item["matched_on"], raw_text)
            out.append(
                CandidateOption(
                    value=canonical,
                    label=canonical,
                    category=category,
                    source=f"neo4j_{item['matched_on']}",
                    score=score,
                    metadata={"matched_on": item["matched_on"], "reason": reason},
                )
            )
            seen.add(canonical)
        return out

    def _catalog_candidates(self, category: str, raw_text: str, limit: int = 8) -> List[CandidateOption]:
        q = raw_text.lower()
        values = self.catalog.get(category)
        matches = [value for value in values if q in value.lower()]
        if not matches and len(q) >= 3:
            matches = [value for value in values if value.lower().startswith(q[:3])]

        out: List[CandidateOption] = []
        for value in matches[:limit]:
            out.append(
                CandidateOption(
                    value=value,
                    label=value,
                    category=category,
                    source="catalog_contains",
                    score=0.5,
                )
            )
        return out

    def _get_live_entries(self, category: str) -> list[dict]:
        if category in self._live_entry_cache:
            return self._live_entry_cache[category]

        spec = self.CATEGORY_SPECS.get(category)
        if not spec:
            self._live_entry_cache[category] = []
            return []

        label, prop = spec
        if category == "cancer":
            cypher = f"""
            MATCH (n:{label})
            WHERE n.code IS NOT NULL
            RETURN DISTINCT
                toString(n.code) AS canonical,
                n.aliases AS aliases,
                toString(n.name) AS display_name
            LIMIT 2000
            """
        elif category == "target":
            cypher = f"""
            MATCH (n:{label})
            WHERE n.{prop} IS NOT NULL
            RETURN DISTINCT
                toString(n.{prop}) AS canonical,
                n.aliases AS aliases,
                toString(n.ensembl_id) AS ensembl_id
            LIMIT 2000
            """
        elif category == "target_pair":
            cypher = f"""
            MATCH (n:{label})
            WHERE n.{prop} IS NOT NULL
            OPTIONAL MATCH (n)-[:HAS_TARGET]->(t:Target)
            RETURN DISTINCT
                toString(n.{prop}) AS canonical,
                n.aliases AS aliases,
                collect(DISTINCT toString(t.symbol)) AS component_targets
            LIMIT 2000
            """
        else:
            cypher = f"""
            MATCH (n:{label})
            WHERE n.{prop} IS NOT NULL
            RETURN DISTINCT toString(n.{prop}) AS canonical, n.aliases AS aliases
            LIMIT 2000
            """
        try:
            rows = self.runner.run(cypher, enforce_limit=False)
        except Exception:
            self._live_entry_cache[category] = []
            return []

        entries: list[dict] = []
        seen = set()
        for row in rows:
            canonical = str(row.get("canonical") or "").strip()
            if not canonical or canonical in seen:
                continue
            aliases = self._to_list(row.get("aliases"))
            cleaned_aliases = []
            alias_seen = set()
            for alias in aliases:
                alias = str(alias).strip()
                if alias and alias.lower() != canonical.lower() and alias.lower() not in alias_seen:
                    cleaned_aliases.append(alias)
                    alias_seen.add(alias.lower())
            glosses = self._glosses_for(category, canonical, row)
            normalized_canonical = self._normalize_assignee_name(canonical) if category == "assignee" else ""
            normalized_aliases = [self._normalize_assignee_name(alias) for alias in cleaned_aliases] if category == "assignee" else []
            if category == "target":
                normalized_canonical = self._normalize_target_text(canonical)
                normalized_aliases = [self._normalize_target_text(alias) for alias in cleaned_aliases]
                compact_canonical = self._compact_alnum(canonical)
                compact_aliases = [self._compact_alnum(alias) for alias in cleaned_aliases]
            elif category == "target_pair":
                normalized_canonical = self._normalize_target_pair_text(canonical)
                normalized_aliases = [self._normalize_target_pair_text(alias) for alias in cleaned_aliases]
                compact_canonical = self._compact_alnum(canonical)
                compact_aliases = [self._compact_alnum(alias) for alias in cleaned_aliases]
            else:
                compact_canonical = self._compact_alnum(canonical) if category == "assignee" else ""
                compact_aliases = [self._compact_alnum(alias) for alias in cleaned_aliases] if category == "assignee" else []
            entries.append(
                {
                    "category": category,
                    "canonical": canonical,
                    "display_name": str(row.get("display_name") or "").strip() if category == "cancer" else canonical,
                    "aliases": cleaned_aliases,
                    "glosses": glosses,
                    "component_targets": self._to_list(row.get("component_targets")) if category == "target_pair" else [],
                    "normalized_canonical": normalized_canonical,
                    "normalized_aliases": [alias for alias in normalized_aliases if alias],
                    "compact_canonical": compact_canonical,
                    "compact_aliases": [alias for alias in compact_aliases if alias],
                }
            )
            seen.add(canonical)

        self._live_entry_cache[category] = entries
        return entries

    def _llm_rerank_candidates(
        self,
        category: str,
        raw_text: str,
        entries: list[dict] | list[CandidateOption],
        limit: int = 8,
    ) -> List[CandidateOption]:
        if self.llm is None or not entries:
            return []

        candidate_pool = self._coerce_candidate_entries(category, entries)[: min(len(entries), 24)]
        if not candidate_pool:
            return []

        prompt_candidates = [self._format_prompt_candidate(category, entry) for entry in candidate_pool]
        system = (
            "You normalize user entity mentions to canonical graph entities. "
            "Choose only from the provided candidate entities. "
            "Return strict JSON only in the format "
            '{"matches":[{"canonical":"...", "score":0.0, "reason":"..."}]}. '
            "If no candidates are plausible, return an empty matches array."
        )
        category_guidance = self._llm_category_guidance(category)
        user = (
            f"Category: {category}\n"
            f"User mention: {raw_text}\n"
            f"Candidate entities from Neo4j:\n{json.dumps(prompt_candidates, ensure_ascii=False)}\n\n"
            f"{category_guidance}\n"
            "Pick the most plausible canonical entities for this mention. "
            "Use alias or gloss evidence when available, but never invent a canonical outside this list."
        )
        try:
            data = self.llm.respond_json(system=system, user=user, reasoning_effort="medium", verbosity="low")
        except Exception:
            return []

        matches = data.get("matches")
        if not isinstance(matches, list):
            return []

        canonical_to_entry = {entry["canonical"]: entry for entry in candidate_pool}
        out: List[CandidateOption] = []
        seen = set()
        for item in matches:
            if not isinstance(item, dict):
                continue
            canonical = str(item.get("canonical") or "").strip()
            if not canonical or canonical not in canonical_to_entry or canonical in seen:
                continue
            score = item.get("score")
            try:
                numeric_score = float(score) if score is not None else 0.72
            except Exception:
                numeric_score = 0.72
            entry = canonical_to_entry[canonical]
            out.append(
                CandidateOption(
                    value=canonical,
                    label=canonical,
                    category=category,
                    source="llm_live_candidates",
                    score=numeric_score,
                    metadata={
                        "reason": str(item.get("reason") or ""),
                        "matched_on": "llm_rerank",
                        "aliases": entry.get("aliases", [])[:5],
                    },
                )
            )
            seen.add(canonical)
            if len(out) >= limit:
                break
        return out

    def _coerce_candidate_entries(self, category: str, entries: list[dict] | list[CandidateOption]) -> list[dict]:
        if not entries:
            return []
        first = entries[0]
        if isinstance(first, CandidateOption):
            candidate_values = [item.value for item in entries if isinstance(item, CandidateOption)]
            by_canonical = {entry["canonical"]: entry for entry in self._get_live_entries(category)}
            return [by_canonical[value] for value in candidate_values if value in by_canonical]
        return [entry for entry in entries if isinstance(entry, dict) and entry.get("canonical")]

    def _format_prompt_candidate(self, category: str, entry: dict) -> dict:
        payload = {
            "canonical": entry["canonical"],
            "aliases": entry.get("aliases", [])[:5],
        }
        display_name = entry.get("display_name", "")
        if display_name and display_name != entry["canonical"]:
            payload["display_name"] = display_name
        if category == "assignee":
            payload["normalized_name"] = entry.get("normalized_canonical", "")
        if category == "target":
            payload["normalized_symbol"] = entry.get("normalized_canonical", "")
        if category == "target_pair":
            payload["targets"] = entry.get("component_targets", [])[:2]
        glosses = entry.get("glosses", [])[:5]
        if glosses:
            payload["glosses"] = glosses
        return payload

    @staticmethod
    def _llm_category_guidance(category: str) -> str:
        if category == "assignee":
            return (
                "For assignees, match the user's company mention to the closest real company in the list. "
                "Corporate suffixes like Inc, LLC, Ltd, GmbH are weak evidence only. "
                "Do not merge different companies just because they are related or share a brand family."
            )
        if category == "cancer":
            return (
                "For cancers, natural-language disease mentions may map to graph abbreviations if the glosses clearly support it."
            )
        if category == "target":
            return (
                "For targets, resolve symbol variants and long-form protein or gene names only against the provided candidates. "
                "Treat punctuation differences like PD-1 vs PD1 as weak formatting differences, not different targets."
            )
        if category == "target_pair":
            return (
                "For target pairs, resolve only against the provided candidate pairs. "
                "Treat slash, plus, and 'and' as formatting variants, and consider reversed target order as potentially the same pair concept."
            )
        if category == "functional_of_target":
            return (
                "For functional target classes, match natural-language biology phrases to the closest canonical functional class in the list. "
                "Underscore-separated labels are just display formatting; rely on the meaning of the phrase."
            )
        if category == "pathway":
            return (
                "For pathways, match natural-language pathway mentions to the closest canonical pathway name in the provided list. "
                "Treat punctuation, parentheses, and acronym expansions like ADME or NRF2/NFE2L2 as formatting or naming variants."
            )
        if category == "technologyclass1":
            return (
                "For technology classes, match natural-language mechanism descriptions to the closest canonical class label in the list. "
                "Treat hyphens, slashes, TME, PK/PD, and minor spelling noise as naming variants."
            )
        if category == "origin":
            return (
                "For origin, match short region codes like US, UK, EU, JP, KR to their natural-language country or region names."
            )
        return "Resolve the mention only against the provided canonical entities."

    def _glosses_for(self, category: str, canonical: str, row: dict | None = None) -> list[str]:
        if category == "cancer":
            return self.CANCER_GLOSSES.get(canonical, [])
        if category == "target":
            values = []
            ensembl_id = str((row or {}).get("ensembl_id") or "").strip()
            if ensembl_id and ensembl_id.lower() != canonical.lower():
                values.append(ensembl_id)
            seen = set()
            out = []
            for value in values:
                lowered = value.lower()
                if lowered in seen:
                    continue
                out.append(value)
                seen.add(lowered)
            return out
        if category == "target_pair":
            components = self._to_list((row or {}).get("component_targets"))
            if len(components) >= 2:
                return ["/".join(components[:2])]
            return []
        if category == "functional_of_target":
            return self._functional_glosses(canonical)
        if category == "pathway":
            return self._pathway_glosses(canonical)
        if category == "technologyclass1":
            return self._technologyclass_glosses(canonical)
        if category == "origin":
            return self.ORIGIN_GLOSSES.get(canonical, [])
        return []

    def _score_assignee_semantics(
        self,
        raw_text: str,
        normalized_canonical: str,
        normalized_aliases: list[str],
        compact_canonical: str,
    ) -> tuple[float, str]:
        normalized_query = self._normalize_assignee_name(raw_text)
        compact_query = self._compact_alnum(raw_text)
        if not normalized_query and not compact_query:
            return 0.0, "canonical"

        if normalized_query and normalized_query == normalized_canonical:
            return 0.97, "normalized"
        if normalized_query and normalized_query in normalized_aliases:
            return 0.95, "normalized_alias"
        if compact_query and compact_query == compact_canonical:
            return 0.94, "compact"
        if normalized_query and normalized_canonical and (
            normalized_query in normalized_canonical or normalized_canonical in normalized_query
        ):
            return 0.82, "normalized"
        return 0.0, "canonical"

    def _score_target_semantics(
        self,
        raw_text: str,
        normalized_canonical: str,
        normalized_aliases: list[str],
        compact_canonical: str,
        compact_aliases: list[str],
        glosses: list[str],
    ) -> tuple[float, str]:
        normalized_query = self._normalize_target_text(raw_text)
        compact_query = self._compact_alnum(raw_text)
        if not normalized_query and not compact_query:
            return 0.0, "canonical"

        if normalized_query and normalized_query == normalized_canonical:
            return 0.97, "normalized"
        if normalized_query and normalized_query in normalized_aliases:
            return 0.95, "normalized_alias"
        if compact_query and compact_query == compact_canonical:
            return 0.94, "compact"
        if compact_query and compact_query in compact_aliases:
            return 0.93, "compact_alias"

        normalized_glosses = [self._normalize_target_text(gloss) for gloss in glosses if self._normalize_target_text(gloss)]
        if normalized_query and normalized_query in normalized_glosses:
            return 0.92, "gloss"
        if normalized_query:
            for gloss in normalized_glosses:
                if normalized_query in gloss or gloss in normalized_query:
                    return 0.84, "gloss"
        return 0.0, "canonical"

    def _target_pair_component_recall(self, raw_text: str, entries: list[dict]) -> list[tuple[float, dict]]:
        parts = self._split_pair_text(raw_text)
        if not parts:
            return []

        left_candidates = self.search("target", parts[0], limit=4)
        right_candidates = self.search("target", parts[1], limit=4)
        if not left_candidates or not right_candidates:
            return []

        left_scores = {candidate.value: float(candidate.score or 0.75) for candidate in left_candidates}
        right_scores = {candidate.value: float(candidate.score or 0.75) for candidate in right_candidates}

        recalled: list[tuple[float, dict]] = []
        for entry in entries:
            components = entry.get("component_targets", [])
            if len(components) < 2:
                continue
            first, second = components[0], components[1]
            direct = first in left_scores and second in right_scores
            reverse = first in right_scores and second in left_scores
            if not direct and not reverse:
                continue
            if direct:
                score = min(0.96, (left_scores[first] + right_scores[second]) / 2.0)
            else:
                score = min(0.94, (right_scores[first] + left_scores[second]) / 2.0)
            recalled.append(
                (
                    score,
                    {
                        "canonical": entry["canonical"],
                        "matched_on": "component_recall",
                        "entry": entry,
                    },
                )
            )
        return recalled

    def _score_target_pair_semantics(self, raw_text: str, entry: dict) -> tuple[float, str]:
        normalized_query = self._normalize_target_pair_text(raw_text)
        compact_query = self._compact_alnum(raw_text)
        if not normalized_query and not compact_query:
            return 0.0, "canonical"

        normalized_canonical = entry.get("normalized_canonical", "")
        normalized_aliases = entry.get("normalized_aliases", [])
        compact_canonical = entry.get("compact_canonical", "")
        compact_aliases = entry.get("compact_aliases", [])

        if normalized_query and normalized_query == normalized_canonical:
            return 0.97, "normalized"
        if normalized_query and normalized_query in normalized_aliases:
            return 0.95, "normalized_alias"
        if compact_query and compact_query == compact_canonical:
            return 0.94, "compact"
        if compact_query and compact_query in compact_aliases:
            return 0.93, "compact_alias"

        query_parts = self._split_pair_text(raw_text)
        component_targets = entry.get("component_targets", [])
        if query_parts and len(component_targets) >= 2:
            normalized_query_parts = {self._normalize_target_text(part) for part in query_parts}
            normalized_component_parts = {self._normalize_target_text(part) for part in component_targets[:2]}
            if normalized_query_parts == normalized_component_parts:
                return 0.92, "component_set"
        return 0.0, "canonical"

    def _score_functional_target_semantics(self, raw_text: str, entry: dict) -> tuple[float, str]:
        normalized_query = self._normalize_phrase(raw_text)
        if not normalized_query:
            return 0.0, "canonical"

        normalized_canonical = self._normalize_phrase(entry.get("canonical", ""))
        if normalized_query == normalized_canonical:
            return 0.94, "normalized"
        if normalized_query and (
            normalized_query in normalized_canonical or normalized_canonical in normalized_query
        ):
            return 0.84, "normalized"

        glosses = entry.get("glosses", [])
        normalized_glosses = [self._normalize_phrase(gloss) for gloss in glosses if self._normalize_phrase(gloss)]
        if normalized_query in normalized_glosses:
            return 0.93, "gloss"
        for gloss in normalized_glosses:
            if normalized_query in gloss or gloss in normalized_query:
                return 0.86, "gloss"

        query_tokens = set(normalized_query.split())
        for gloss in normalized_glosses:
            gloss_tokens = set(gloss.split())
            if not query_tokens or not gloss_tokens:
                continue
            overlap = len(query_tokens & gloss_tokens) / max(len(query_tokens), len(gloss_tokens))
            if overlap >= 0.75:
                return 0.8, "semantic"
            if overlap >= 0.5:
                return 0.72, "semantic"
        return 0.0, "canonical"

    def _score_pathway_semantics(self, raw_text: str, entry: dict) -> tuple[float, str]:
        normalized_query = self._normalize_phrase(raw_text)
        if not normalized_query:
            return 0.0, "canonical"

        normalized_canonical = self._normalize_phrase(entry.get("canonical", ""))
        if normalized_query == normalized_canonical:
            return 0.94, "normalized"
        if normalized_query and (
            normalized_query in normalized_canonical or normalized_canonical in normalized_query
        ):
            return 0.85, "normalized"

        glosses = entry.get("glosses", [])
        normalized_glosses = [self._normalize_phrase(gloss) for gloss in glosses if self._normalize_phrase(gloss)]
        if normalized_query in normalized_glosses:
            return 0.93, "gloss"
        for gloss in normalized_glosses:
            if normalized_query in gloss or gloss in normalized_query:
                return 0.87, "gloss"

        query_tokens = set(normalized_query.split())
        for gloss in normalized_glosses:
            gloss_tokens = set(gloss.split())
            if not query_tokens or not gloss_tokens:
                continue
            overlap = len(query_tokens & gloss_tokens) / max(len(query_tokens), len(gloss_tokens))
            if overlap >= 0.8:
                return 0.82, "semantic"
            if overlap >= 0.6:
                return 0.74, "semantic"
        return 0.0, "canonical"

    def _score_technologyclass_semantics(self, raw_text: str, entry: dict) -> tuple[float, str]:
        normalized_query = self._normalize_phrase(raw_text)
        if not normalized_query:
            return 0.0, "canonical"

        normalized_canonical = self._normalize_phrase(entry.get("canonical", ""))
        if normalized_query == normalized_canonical:
            return 0.94, "normalized"
        if normalized_query and (
            normalized_query in normalized_canonical or normalized_canonical in normalized_query
        ):
            return 0.85, "normalized"

        glosses = entry.get("glosses", [])
        normalized_glosses = [self._normalize_phrase(gloss) for gloss in glosses if self._normalize_phrase(gloss)]
        if normalized_query in normalized_glosses:
            return 0.93, "gloss"
        for gloss in normalized_glosses:
            if normalized_query in gloss or gloss in normalized_query:
                return 0.87, "gloss"
        return 0.0, "canonical"

    def _score_origin_semantics(self, raw_text: str, entry: dict) -> tuple[float, str]:
        normalized_query = self._normalize_phrase(raw_text)
        if not normalized_query:
            return 0.0, "canonical"

        canonical = str(entry.get("canonical", "")).strip()
        if normalized_query == self._normalize_phrase(canonical):
            return 0.96, "normalized"

        glosses = entry.get("glosses", [])
        normalized_glosses = [self._normalize_phrase(gloss) for gloss in glosses if self._normalize_phrase(gloss)]
        if normalized_query in normalized_glosses:
            return 0.94, "gloss"
        for gloss in normalized_glosses:
            if normalized_query == gloss or normalized_query in gloss or gloss in normalized_query:
                return 0.88, "gloss"
        return 0.0, "canonical"

    def _score_cancer_semantics(self, query: str, entry: dict) -> float:
        normalized_query = self._normalize_phrase(query)
        if not normalized_query:
            return 0.0

        best = 0.0
        query_tokens_all = set(normalized_query.split())
        query_tokens_focus = {t for t in query_tokens_all if t not in self.CANCER_GENERIC_TOKENS}
        for gloss in entry.get("glosses", []):
            normalized_gloss = self._normalize_phrase(gloss)
            if not normalized_gloss:
                continue
            if normalized_query == normalized_gloss:
                best = max(best, 0.93)
            elif normalized_query in normalized_gloss or normalized_gloss in normalized_query:
                best = max(best, 0.86)
            else:
                gloss_tokens_all = set(normalized_gloss.split())
                gloss_tokens_focus = {t for t in gloss_tokens_all if t not in self.CANCER_GENERIC_TOKENS}
                if query_tokens_focus and gloss_tokens_focus:
                    overlap_focus = query_tokens_focus & gloss_tokens_focus
                    if overlap_focus:
                        overlap = len(overlap_focus) / max(len(query_tokens_focus), len(gloss_tokens_focus))
                        if overlap >= 0.66:
                            best = max(best, 0.8)
                        elif overlap >= 0.5:
                            best = max(best, 0.74)
        return best

    def _match_reason(self, entry: dict, matched_on: str, raw_text: str) -> str:
        canonical = entry["canonical"]
        if matched_on == "canonical":
            return f"`{raw_text}` exactly matches KG entity `{canonical}`."
        if matched_on == "alias":
            return f"`{raw_text}` matches an alias recorded for KG entity `{canonical}`."
        if entry.get("category") == "target" and matched_on in {"normalized", "normalized_alias", "compact", "compact_alias"}:
            return f"`{raw_text}` matches the normalized target symbol or alias for KG entity `{canonical}`."
        if entry.get("category") == "target_pair" and matched_on in {"normalized", "normalized_alias", "compact", "compact_alias"}:
            return f"`{raw_text}` matches the normalized target-pair form for KG entity `{canonical}`."
        if entry.get("category") == "target_pair" and matched_on == "component_set":
            return f"`{raw_text}` contains the same component targets as KG entity `{canonical}`."
        if entry.get("category") == "target_pair" and matched_on == "component_recall":
            components = entry.get("component_targets", [])
            if len(components) >= 2:
                return f"`{raw_text}` is consistent with component targets `{components[0]}` and `{components[1]}` for KG entity `{canonical}`."
        if entry.get("category") == "functional_of_target" and matched_on in {"normalized", "gloss", "semantic"}:
            glosses = entry.get("glosses", [])
            if glosses:
                return f"`{raw_text}` aligns with functional class phrase `{glosses[0]}`, mapped to KG entity `{canonical}`."
        if entry.get("category") == "pathway" and matched_on in {"normalized", "gloss", "semantic"}:
            glosses = entry.get("glosses", [])
            if glosses:
                return f"`{raw_text}` aligns with pathway phrase `{glosses[0]}`, mapped to KG entity `{canonical}`."
        if entry.get("category") == "technologyclass1" and matched_on in {"normalized", "gloss"}:
            glosses = entry.get("glosses", [])
            if glosses:
                return f"`{raw_text}` aligns with technology class phrase `{glosses[0]}`, mapped to KG entity `{canonical}`."
        if entry.get("category") == "origin" and matched_on in {"normalized", "gloss"}:
            glosses = entry.get("glosses", [])
            if glosses:
                return f"`{raw_text}` aligns with origin phrase `{glosses[0]}`, mapped to KG entity `{canonical}`."
        if matched_on in {"normalized", "normalized_alias", "compact", "compact_alias"}:
            return f"`{raw_text}` matches the normalized company form of KG entity `{canonical}`."
        if matched_on == "gloss":
            glosses = entry.get("glosses", [])
            if glosses:
                if entry.get("category") == "target":
                    return f"`{raw_text}` aligns with the target descriptor `{glosses[0]}`, mapped to KG entity `{canonical}`."
                return f"`{raw_text}` is close to the disease phrase `{glosses[0]}`, mapped to KG entity `{canonical}`."
        if matched_on == "semantic":
            glosses = entry.get("glosses", [])
            if glosses:
                return f"`{raw_text}` semantically aligns with `{glosses[0]}`, so `{canonical}` is a likely KG cancer code."
        return f"`{canonical}` is a plausible KG entity for `{raw_text}`."

    @staticmethod
    def _normalize_phrase(text: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()
        return re.sub(r"\s+", " ", cleaned)

    def _normalize_assignee_name(self, text: str) -> str:
        phrase = self._normalize_phrase(text)
        if not phrase:
            return ""
        tokens = [tok for tok in phrase.split() if tok not in self.ASSIGNEE_SUFFIX_TOKENS]
        if not tokens:
            tokens = phrase.split()
        return " ".join(tokens)

    @staticmethod
    def _normalize_target_text(text: str) -> str:
        phrase = EntityLookup._normalize_phrase(text)
        return phrase.replace(" ", "")

    @staticmethod
    def _split_pair_text(text: str) -> tuple[str, str] | None:
        separator_pattern = r"\s*(?:/|\+|\band\b|\bplus\b|\bx\b|\u00D7)\s*"
        parts = [
            part.strip()
            for part in re.split(separator_pattern, text or "", flags=re.IGNORECASE)
            if part.strip()
        ]
        if len(parts) == 2:
            return parts[0], parts[1]
        return None

    @classmethod
    def _normalize_target_pair_text(cls, text: str) -> str:
        parts = cls._split_pair_text(text)
        if parts:
            return "/".join(cls._normalize_target_text(part) for part in parts)
        return cls._normalize_target_text(text)

    @classmethod
    def _functional_glosses(cls, canonical: str) -> list[str]:
        text = str(canonical or "").strip()
        if not text:
            return []
        base = text.replace("_", " ")
        variants = [base]
        if base.lower().endswith(" target"):
            variants.append(base[:-7].strip())

        expanded_tokens = []
        for token in base.split():
            expanded_tokens.append(cls.FUNCTIONAL_TOKEN_GLOSSES.get(token.lower(), token))
        expanded = " ".join(expanded_tokens).strip()
        if expanded and expanded.lower() != base.lower():
            variants.append(expanded)
            if expanded.lower().endswith(" target"):
                variants.append(expanded[:-7].strip())

        out = []
        seen = set()
        for value in variants:
            lowered = value.lower()
            if value and lowered not in seen:
                out.append(value)
                seen.add(lowered)
        return out

    @classmethod
    def _pathway_glosses(cls, canonical: str) -> list[str]:
        text = str(canonical or "").strip()
        if not text:
            return []

        variants = [text]
        base = re.sub(r"[()/,-]+", " ", text)
        base = re.sub(r"\s+", " ", base).strip()
        if base and base.lower() != text.lower():
            variants.append(base)

        tokens = []
        for token in base.split():
            lowered = token.lower()
            tokens.append(cls.PATHWAY_TOKEN_GLOSSES.get(lowered, token))
        expanded = " ".join(tokens).strip()
        if expanded and expanded.lower() != base.lower():
            variants.append(expanded)

        if text.lower().endswith(" pathway"):
            variants.append(text[:-8].strip())
        if base.lower().endswith(" pathway"):
            variants.append(base[:-8].strip())
        if expanded.lower().endswith(" pathway"):
            variants.append(expanded[:-8].strip())

        out = []
        seen = set()
        for value in variants:
            cleaned = re.sub(r"\s+", " ", value).strip()
            lowered = cleaned.lower()
            if cleaned and lowered not in seen:
                out.append(cleaned)
                seen.add(lowered)
        return out

    @classmethod
    def _technologyclass_glosses(cls, canonical: str) -> list[str]:
        text = str(canonical or "").strip()
        if not text:
            return []

        normalized = (
            text.replace("‑", "-")
            .replace("–", "-")
            .replace("/", " / ")
        )
        normalized = re.sub(r"\s+", " ", normalized).strip()
        variants = [normalized]

        tokens = []
        for token in normalized.split():
            lowered = token.lower()
            tokens.append(cls.TECHNOLOGYCLASS_TOKEN_GLOSSES.get(lowered, token))
        expanded = " ".join(tokens).strip()
        expanded = expanded.replace(" / ", "/")
        if expanded and expanded.lower() != normalized.lower():
            variants.append(expanded)

        cleaned_variants = []
        for value in variants:
            current = value
            current = current.replace("functional combiantion", "functional combination")
            current = current.replace("Piggbacking", "Piggybacking")
            current = re.sub(r"\s+", " ", current).strip()
            if current:
                cleaned_variants.append(current)

        out = []
        seen = set()
        for value in cleaned_variants:
            lowered = value.lower()
            if lowered not in seen:
                out.append(value)
                seen.add(lowered)
        return out

    @staticmethod
    def _compact_alnum(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (text or "").lower())

    @staticmethod
    def _to_list(v: object) -> List[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        s = str(v).strip()
        return [s] if s else []
