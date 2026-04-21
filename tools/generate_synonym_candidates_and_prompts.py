from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
from neo4j import GraphDatabase

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


CATEGORY_SPECS: Dict[str, Tuple[str, str]] = {
    "assignee": ("Assignee", "name"),
    "target": ("Target", "symbol"),
    "target_pair": ("TargetPair", "name"),
    "pathway": ("Pathway", "name"),
    "functional_of_target": ("Functional_of_Target", "name"),
    "cancer": ("Cancer", "name"),
    "origin": ("Origin", "name"),
}


ASSIGNEE_SUFFIXES = [
    "inc",
    "inc.",
    "ltd",
    "ltd.",
    "llc",
    "corp",
    "corp.",
    "co",
    "co.",
    "company",
    "plc",
    "sa",
    "nv",
    "gmbh",
    "ag",
    "kg",
    "co ltd",
    "co., ltd",
]


def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _safe(s: Any) -> str:
    return _norm_spaces(str(s or "").strip())


def _dedup_keep_order(vals: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for v in vals:
        s = _safe(v)
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def _split_target_pair(tp: str) -> List[str]:
    # support A/B or A-B
    if "/" in tp:
        parts = [_safe(x) for x in tp.split("/")]
        return [p for p in parts if p]
    if "-" in tp and tp.count("-") == 1:
        parts = [_safe(x) for x in tp.split("-")]
        return [p for p in parts if p]
    return [tp]


def fetch_entities(uri: str, user: str, password: str, database: str) -> Dict[str, Dict[str, Any]]:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    out: Dict[str, Dict[str, Any]] = {}
    try:
        with driver.session(database=database) as s:
            for category, (label, prop) in CATEGORY_SPECS.items():
                q = (
                    f"MATCH (n:{label}) "
                    f"WHERE n.{prop} IS NOT NULL "
                    f"RETURN toString(n.{prop}) AS canonical, n.aliases AS aliases, n.alias AS alias "
                    f"LIMIT 30000"
                )
                rows = list(s.run(q))
                by_canonical: Dict[str, Dict[str, Any]] = {}
                for r in rows:
                    canonical = _safe(r.get("canonical"))
                    if not canonical:
                        continue
                    rec = by_canonical.setdefault(
                        canonical,
                        {
                            "canonical": canonical,
                            "category": category,
                            "aliases": [],
                            "source": f"neo4j:{label}.{prop}",
                        },
                    )
                    aliases = []
                    raw_aliases = r.get("aliases")
                    raw_alias = r.get("alias")
                    if isinstance(raw_aliases, list):
                        aliases.extend([_safe(x) for x in raw_aliases if _safe(x)])
                    elif _safe(raw_aliases):
                        aliases.append(_safe(raw_aliases))
                    if isinstance(raw_alias, list):
                        aliases.extend([_safe(x) for x in raw_alias if _safe(x)])
                    elif _safe(raw_alias):
                        aliases.append(_safe(raw_alias))
                    rec["aliases"].extend(aliases)

                for k, v in by_canonical.items():
                    v["aliases"] = _dedup_keep_order(v.get("aliases", []))
                    by_canonical[k] = v
                out[category] = by_canonical
    finally:
        driver.close()
    return out


def heuristic_aliases(category: str, canonical: str) -> List[Tuple[str, str, float]]:
    c = _safe(canonical)
    if not c:
        return []
    aliases: List[Tuple[str, str, float]] = []

    def add(alias: str, rule: str, conf: float) -> None:
        a = _safe(alias)
        if not a:
            return
        if a.lower() == c.lower():
            return
        aliases.append((a, rule, conf))

    # common punctuation/case variants
    add(c.upper(), "rule_upper", 0.84)
    add(c.lower(), "rule_lower", 0.80)

    if category == "assignee":
        x = re.sub(r"[,\.\(\)]", " ", c)
        x = _norm_spaces(x)
        add(x, "rule_remove_punct", 0.88)
        words = x.split()
        # strip legal suffixes
        if words:
            trimmed = words[:]
            while trimmed and trimmed[-1].lower() in ASSIGNEE_SUFFIXES:
                trimmed.pop()
            if trimmed:
                add(" ".join(trimmed), "rule_strip_legal_suffix", 0.94)
        # ampersand normalization
        add(x.replace("&", "and"), "rule_amp_to_and", 0.90)
        add(x.replace("and", "&"), "rule_and_to_amp", 0.86)

    elif category == "target":
        add(c.replace("-", ""), "rule_drop_hyphen", 0.96)
        add(c.replace("-", " "), "rule_hyphen_to_space", 0.90)
        add(c.replace("/", ""), "rule_drop_slash", 0.88)
        add(c.replace("/", "-"), "rule_slash_to_hyphen", 0.86)
        # PD-L1 -> PDL1 style
        compact = re.sub(r"[^A-Za-z0-9]", "", c)
        add(compact, "rule_compact_alnum", 0.95)

    elif category == "target_pair":
        parts = _split_target_pair(c)
        if len(parts) == 2:
            a, b = parts[0], parts[1]
            add(f"{a}-{b}", "rule_pair_slash_to_hyphen", 0.92)
            add(f"{a} / {b}", "rule_pair_spaced_slash", 0.93)
            add(f"{a}{b}", "rule_pair_concat", 0.70)
            # reverse order as low-confidence candidate only
            add(f"{b}/{a}", "rule_pair_reverse_order", 0.40)

    elif category in {"pathway", "functional_of_target", "cancer", "origin"}:
        add(c.replace("_", " "), "rule_underscore_to_space", 0.95)
        add(c.replace("-", " "), "rule_hyphen_to_space", 0.90)
        add(re.sub(r"\s+", "", c), "rule_remove_spaces", 0.70)

    return aliases


def build_candidates(entities: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, List[Dict[str, Any]]]], pd.DataFrame]:
    nested: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    flat_rows: List[Dict[str, Any]] = []

    for category, by_canonical in entities.items():
        nested[category] = {}
        for canonical, rec in by_canonical.items():
            candidates: List[Dict[str, Any]] = []
            # existing aliases from graph
            for a in rec.get("aliases", []):
                candidates.append(
                    {
                        "alias": a,
                        "source": "neo4j_alias_property",
                        "confidence": 0.99,
                    }
                )
            # generated aliases
            for alias, rule, conf in heuristic_aliases(category, canonical):
                candidates.append({"alias": alias, "source": rule, "confidence": conf})

            # de-dup alias
            seen = set()
            dedup: List[Dict[str, Any]] = []
            for c in candidates:
                a = _safe(c["alias"])
                if not a:
                    continue
                if len(a) < 2:
                    continue
                k = a.lower()
                if k == canonical.lower():
                    continue
                if k in seen:
                    continue
                seen.add(k)
                dedup.append(c)

            nested[category][canonical] = dedup
            for c in dedup:
                flat_rows.append(
                    {
                        "category": category,
                        "canonical": canonical,
                        "alias": c["alias"],
                        "source": c["source"],
                        "confidence": c["confidence"],
                        "keep": "",
                        "notes": "",
                    }
                )

    flat_df = pd.DataFrame(flat_rows).sort_values(["category", "canonical", "confidence"], ascending=[True, True, False])
    return nested, flat_df


def write_prompt_batches(
    entities: Dict[str, Dict[str, Any]],
    out_dir: str,
    batch_size: int = 20,
) -> None:
    prompts_dir = os.path.join(out_dir, "websearch_prompts")
    os.makedirs(prompts_dir, exist_ok=True)

    template = """You are helping build an entity synonym dictionary for a Neo4j KG QA system.

Task:
1) For each canonical entity below, find high-quality aliases/synonyms/abbreviations from the allowed sources.
2) Return ONLY JSON array. Each item:
{{
  "canonical": "...",
  "alias": "...",
  "category": "{category}",
  "source_url": "...",
  "evidence": "...",
  "confidence": 0.0-1.0
}}

Strict rules:
- Do not invent aliases.
- Keep biomedical/organization names exact.
- Prefer precision over recall.
- If no reliable alias is found, skip it.
- Do not include duplicates.
- Output only JSON.

Allowed sources:
{allowed_sources}

Canonical entities (batch {batch_no}):
{entities_block}
"""

    allowed_map = {
        "target": "- GeneCards (genecards.org)\n- HGNC (genenames.org)\n- UniProt (uniprot.org)\n- NCBI Gene (ncbi.nlm.nih.gov/gene)",
        "target_pair": "- GeneCards (for each component target)\n- HGNC\n- UniProt\n(Note: treat pair alias as format variants of validated target aliases)",
        "pathway": "- Reactome (reactome.org)\n- KEGG (kegg.jp)\n- WikiPathways (wikipathways.org)",
        "functional_of_target": "- Internal category naming + biomedical glossary references\n- Avoid over-expansion if no authoritative synonym",
        "cancer": "- NCI Thesaurus (ncit.nci.nih.gov)\n- OncoTree (oncotree.mskcc.org)\n- MeSH",
        "assignee": "- Official company site\n- SEC/company registry pages\n- WIPO/USPTO assignee name variants (if available)",
        "origin": "- Do not search. Use normalization only (e.g., US/USA, UK/United Kingdom).",
    }

    for category, by_canonical in entities.items():
        canonicals = sorted(by_canonical.keys())
        if not canonicals:
            continue
        category_dir = os.path.join(prompts_dir, category)
        os.makedirs(category_dir, exist_ok=True)

        batches = [canonicals[i : i + batch_size] for i in range(0, len(canonicals), batch_size)]
        for i, b in enumerate(batches, start=1):
            entities_block = "\n".join(f"- {x}" for x in b)
            content = template.format(
                category=category,
                allowed_sources=allowed_map.get(category, "- Official authoritative source only"),
                batch_no=i,
                entities_block=entities_block,
            )
            p = os.path.join(category_dir, f"{category}_batch_{i:02d}.md")
            with open(p, "w", encoding="utf-8") as f:
                f.write(content)

    with open(os.path.join(prompts_dir, "README.md"), "w", encoding="utf-8") as f:
        f.write(
            "# Web Search Prompt Batches\n\n"
            "Each subfolder contains copy-paste prompts for ChatGPT Web Search.\n"
            "Run batch by batch, then merge returned JSON into `websearch_results/`.\n"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Neo4j entities and generate synonym candidates + websearch prompts.")
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()

    from bsab_kg_qa_en.config import load_settings

    cfg = load_settings(args.settings)
    n = cfg["neo4j"]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or os.path.join("bsab_kg_qa_en", "ner", "synonyms_work", f"synonyms_{ts}")
    os.makedirs(out_dir, exist_ok=True)

    entities = fetch_entities(
        uri=n["uri"],
        user=n["user"],
        password=n["password"],
        database=n["database"],
    )
    nested_candidates, flat_df = build_candidates(entities)

    # 1) entity canonical export
    canonical_export = {
        k: {
            "count": len(v),
            "canonicals": sorted(v.keys()),
        }
        for k, v in entities.items()
    }
    with open(os.path.join(out_dir, "entity_canonicals.json"), "w", encoding="utf-8") as f:
        json.dump(canonical_export, f, ensure_ascii=False, indent=2)

    # 2) candidates export (nested + flat)
    with open(os.path.join(out_dir, "synonym_candidates_nested.json"), "w", encoding="utf-8") as f:
        json.dump(nested_candidates, f, ensure_ascii=False, indent=2)

    flat_csv = os.path.join(out_dir, "synonym_candidates_review.csv")
    flat_xlsx = os.path.join(out_dir, "synonym_candidates_review.xlsx")
    flat_df.to_csv(flat_csv, index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(flat_xlsx, engine="xlsxwriter") as writer:
        flat_df.to_excel(writer, index=False, sheet_name="review")

    # 3) seed synonym dictionary (high-confidence only)
    seed: Dict[str, Dict[str, List[str]]] = defaultdict(dict)
    for category, by_canonical in nested_candidates.items():
        for canonical, cands in by_canonical.items():
            keep = [x["alias"] for x in cands if float(x.get("confidence", 0.0)) >= 0.90]
            if keep:
                seed[category][canonical] = _dedup_keep_order(keep)
    with open(os.path.join(out_dir, "entity_synonyms_seed_highconf.json"), "w", encoding="utf-8") as f:
        json.dump(seed, f, ensure_ascii=False, indent=2)

    # 4) prompt batches for ChatGPT Web Search
    write_prompt_batches(entities, out_dir=out_dir, batch_size=args.batch_size)

    # 5) index
    index = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "categories": {k: len(v) for k, v in entities.items()},
        "files": [
            "entity_canonicals.json",
            "synonym_candidates_nested.json",
            "synonym_candidates_review.csv",
            "synonym_candidates_review.xlsx",
            "entity_synonyms_seed_highconf.json",
            "websearch_prompts/README.md",
        ],
    }
    with open(os.path.join(out_dir, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"Generated synonym workspace: {out_dir}")
    print(f"Review file: {flat_xlsx}")


if __name__ == "__main__":
    main()
