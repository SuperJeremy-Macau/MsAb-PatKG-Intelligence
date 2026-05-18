import os
import sys
import re
import csv
import io
import json
import hashlib
import html
from datetime import datetime, timezone
from urllib.parse import quote

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st

from bsab_kg_qa_en.config import load_settings
from bsab_kg_qa_en.core import Orchestrator
from bsab_kg_qa_en.core.llm_provider import LLMProvider
from bsab_kg_qa_en.intents import IntentRegistry
from bsab_kg_qa_en.interaction import InteractiveQaFlowService, QueryClarifier, QueryDraft
from bsab_kg_qa_en.kg import Neo4jRunner
from bsab_kg_qa_en.ner import NERService


SETTINGS_PATH = os.path.join("bsab_kg_qa_en", "config", "settings.yaml")
LOCAL_SETTINGS_TXT = os.path.join("bsab_kg_qa_en", "config", "Local Settings.txt")
MAX_CLARIFICATION_TURNS = 5
SESSION_STATE_VERSION = "2026-04-22-query-flow-v2"

APP_TITLE = "MsAb-PatKG Intelligence"
APP_SUBTITLE = "Graph-grounded patent intelligence for multispecific antibody research."
APP_NAV_SECTIONS = ["Introduction", "Scenario", "Ask", "Contact"]
APP_COPYRIGHT = "Copyright (c) 2026 MsAb-PatKG Project. For academic research use only."
CONTACT_NAME = os.getenv("MSAB_CONTACT_NAME", "WANG MENGYANG")
CONTACT_EMAIL = os.getenv("MSAB_CONTACT_EMAIL", "cpuwangmengyang@163.com")
FEEDBACK_DIR = os.getenv("MSAB_FEEDBACK_DIR", os.path.join(ROOT, "feedback_submissions"))
FEEDBACK_LOG_PATH = os.path.join(FEEDBACK_DIR, "msab_patkg_feedback.jsonl")

SUPPORTED_SCOPE = [
    "Patent counts, yearly trends, and first-disclosure years",
    "Assignees, company activity, origin-level competition, and new entrants",
    "Resolved target pairs disclosed in patents and their competitive crowding",
    "Functional-of-target categories, pathways, technology classes, and cancer-expression context",
]

OUT_OF_SCOPE = [
    "Biological or clinical claims about the best target pair",
    "Efficacy, safety, developability, or probability-of-success prediction",
    "Mechanistic hypotheses not represented in the patent knowledge graph",
    "Target-pair ranking based on unresolved pathogen-derived or external antigen placeholders",
    "Medical, legal, investment, or freedom-to-operate advice",
]

SCENARIO_DESCRIPTIONS = {
    "Target-combination inspiration": "Find target-pair combinations, functional classes, pathways, and emerging patent signals.",
    "Patent coverage and competitive crowding": "Assess how crowded a target pair or target space is in the patent graph.",
    "Company scouting for licensing/investment": "Identify assignees, new entrants, and company activity around emerging target pairs.",
    "Assignee-origin macro-competition": "Compare origin-level competition and first-disclosure patterns across assignees.",
}

LANDSCAPE_CAVEAT = (
    "This system ranks and summarizes patent landscape signals. It does not evaluate biological superiority, "
    "clinical efficacy, safety, or commercial success."
)

RESOLVED_TARGETPAIR_CAVEAT = (
    "For target-pair ranking, diversity, and functional target-pair combination analyses, MsAb-PatKG uses only "
    "resolved target-pair identities. Anti-infective records with pathogen-derived or poorly standardized external "
    "antigens are retained in the broader curated dataset, but unresolved placeholder categories are excluded from "
    "these target-pair-specific analyses."
)

OUT_OF_SCOPE_PATTERNS = [
    r"\bbest\b",
    r"\bmost promising\b",
    r"\bbetter\b",
    r"\bpredict\b",
    r"\bprediction\b",
    r"\beffective\b",
    r"\befficacy\b",
    r"\bclinically\b",
    r"\bclinical success\b",
    r"\bdrug candidate\b",
    r"\bwill succeed\b",
    r"\bshould invest\b",
]

SCENARIO_EXAMPLES = {
    "Target-combination inspiration": [
        "Within target-pair combinations involving the Functional_of_Target category Adaptive_Immune_Checkpoint_Target, which target pairs have the highest patent counts? Return the top 10 target pairs ranked by patent count.",
        "Among target-pair combinations involving the Pathway Complement_Pathway_Target, which target pairs have the highest patent family counts? Return the top 10 target pairs ranked by patent family count.",
    ],
    "Patent coverage and competitive crowding": [
        "Which assignees have patents associated with the target-pair combination 4-1BB/CD3/EGFR? Return the list of assignees across all available years.",
        "Among patents associated with target-pair combinations that contain the target CCR5, which assignee has the highest patent count across all time?",
    ],
    "Company scouting for licensing/investment": [
        "Over the last 3 years, which assignees have the highest patent counts for target pairs whose first disclosure occurred within the last 3 years? Return the assignees ranked by patent count for these newly emerging target-pair combinations.",
        "Which target pairs were associated with assignees whose first entry into the bispecific-antibody patent space occurred in 2024?",
    ],
    "Assignee-origin macro-competition": [
        "Among target-pair combinations containing the target CD19 and associated with assignees from the Origin China, what are the first-disclosure year, assignee, and patent publication number for the leading target-pair combinations ranked by patent count?",
        "Across all years, which assignees from the Origin category Other are the earliest disclosers of the largest number of first-in-class target-pair combinations, ranked by the count of target pairs for which they are the first discloser?",
    ],
}

ENTITY_GUIDE_QUERIES = {
    "Assignees": "MATCH (a:Assignee)<-[:HAS_ASSIGNEE]-(p:Patent) RETURN a.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Targets": "MATCH (t:Target)<-[:HAS_TARGET]-(:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN t.symbol AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Target Pairs": "MATCH (tp:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) WHERE tp.name CONTAINS '/' RETURN tp.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Functions": "MATCH (f:Functional_of_Target)<-[:FUNCTIONED_AS]-(:Target)<-[:HAS_TARGET]-(:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN f.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Pathways": "MATCH (pw:Pathway)<-[:IN_PATHWAY]-(:Target)<-[:HAS_TARGET]-(:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN pw.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Tech Classes": "MATCH (tc:TechnologyClass1)<-[:HAS_TECHNOLOGY_CLASS1]-(tp:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN tc.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Origins": "MATCH (o:Origin)<-[:ORIGIN_FROM]-(:Assignee)<-[:HAS_ASSIGNEE]-(p:Patent) RETURN o.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
    "Cancers": "MATCH (c:Cancer)<-[:DIFFERENTIAL_AND_HIGHLY_EXPRESSED_IN]-(:Target)<-[:HAS_TARGET]-(:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN c.code AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
}

ENTITY_GUIDE_COUNT_QUERIES = {
    "Assignees": "MATCH (a:Assignee) RETURN count(DISTINCT a) AS total",
    "Targets": "MATCH (t:Target) RETURN count(DISTINCT t) AS total",
    "Target Pairs": "MATCH (tp:TargetPair) RETURN count(DISTINCT tp) AS total",
    "Functions": "MATCH (f:Functional_of_Target) RETURN count(DISTINCT f) AS total",
    "Pathways": "MATCH (pw:Pathway) RETURN count(DISTINCT pw) AS total",
    "Tech Classes": "MATCH (tc:TechnologyClass1) RETURN count(DISTINCT tc) AS total",
    "Origins": "MATCH (o:Origin) RETURN count(DISTINCT o) AS total",
    "Cancers": "MATCH (c:Cancer) RETURN count(DISTINCT c) AS total",
}

DATABASE_STAT_QUERIES = [
    {
        "label": "Patent Families",
        "caption": "curated in MsAb-PatKG",
        "cypher": "MATCH (f:Family) RETURN count(DISTINCT f) AS total",
        "fallback": None,
        "verified_total": 1421,
    },
    {
        "label": "Patent Publications",
        "caption": "linked to graph evidence",
        "cypher": "MATCH (p:Patent) RETURN count(DISTINCT p) AS total",
        "fallback": None,
        "verified_total": 16539,
    },
    {
        "label": "Resolved Target Pairs",
        "caption": "human target-pair identities",
        "cypher": "MATCH (tp:TargetPair) WHERE tp.name CONTAINS '/' RETURN count(DISTINCT tp) AS total",
        "fallback": "Target Pairs",
        "verified_total": 700,
    },
    {
        "label": "Targets",
        "caption": "therapeutic targets represented",
        "cypher": "MATCH (t:Target) RETURN count(DISTINCT t) AS total",
        "fallback": "Targets",
        "verified_total": 383,
    },
    {
        "label": "Pathways",
        "caption": "biological pathways connected",
        "cypher": "MATCH (pw:Pathway) RETURN count(DISTINCT pw) AS total",
        "fallback": "Pathways",
        "verified_total": 1028,
    },
    {
        "label": "Functional Categories",
        "caption": "target-role annotation classes",
        "cypher": "MATCH (f:Functional_of_Target) RETURN count(DISTINCT f) AS total",
        "fallback": "Functions",
        "verified_total": 55,
    },
    {
        "label": "Assignees",
        "caption": "organizations with patent activity",
        "cypher": "MATCH (a:Assignee) RETURN count(DISTINCT a) AS total",
        "fallback": "Assignees",
        "verified_total": 727,
    },
    {
        "label": "Technology Classes",
        "caption": "landscape segmentation classes",
        "cypher": "MATCH (tc:TechnologyClass1) RETURN count(DISTINCT tc) AS total",
        "fallback": "Tech Classes",
        "verified_total": 14,
    },
]

STATE_PROGRESS = {
    "idle": 0.0,
    "analyzing": 0.15,
    "clarifying_entities": 0.45,
    "clarifying_intent": 0.7,
    "draft_ready": 0.84,
    "awaiting_confirmation": 0.9,
    "executing": 0.96,
    "completed": 1.0,
    "error": 1.0,
}

STATE_LABEL = {
    "idle": "Ask",
    "analyzing": "Understand",
    "clarifying_entities": "Confirm Query",
    "clarifying_intent": "Confirm Query",
    "draft_ready": "Ready to Run",
    "awaiting_confirmation": "Ready to Run",
    "executing": "Run on MsAb-PatKG",
    "completed": "Completed",
    "error": "Error",
}

SLOT_LABEL = {
    "assignee": "assignee",
    "cancer": "cancer",
    "functional_of_target": "functional target class",
    "origin": "origin",
    "pathway": "pathway",
    "target": "target",
    "target1": "target 1",
    "target2": "target 2",
    "technologyclass1": "technology class",
    "tp_name": "target pair",
    "year": "year",
    "start_year": "start year",
    "end_year": "end year",
}


def _ensure_openai_api_key() -> None:
    if os.getenv("OPENAI_API_KEY"):
        return
    if not os.path.exists(LOCAL_SETTINGS_TXT):
        return
    try:
        with open(LOCAL_SETTINGS_TXT, "r", encoding="utf-8", errors="ignore") as fh:
            text = fh.read()
    except OSError:
        return
    match = re.search(r"OPENAI_API_KEY\s*[:=]\s*[\"']?([^\"'\r\n]+)", text)
    if match:
        os.environ["OPENAI_API_KEY"] = match.group(1).strip()


@st.cache_resource(show_spinner=False)
def build_services():
    _ensure_openai_api_key()
    cfg = load_settings(SETTINGS_PATH)
    neo = cfg["neo4j"]
    llm_cfg = cfg["llm"]
    props = cfg.get("props", {})
    intent_cfg = cfg.get("intent", {})

    runner = Neo4jRunner(
        uri=neo["uri"],
        user=neo["user"],
        password=neo["password"],
        database=neo["database"],
        max_rows=int(neo.get("max_rows", 50)),
    )
    registry = IntentRegistry(intent_cfg["definitions_dir"])
    llm = LLMProvider(
        base_url=llm_cfg["base_url"],
        api_key_env=llm_cfg["api_key_env"],
        model=llm_cfg["model"],
    )
    orchestrator = Orchestrator(
        runner=runner,
        registry=registry,
        llm=llm,
        props=props,
        enable_nl2cypher_fallback=bool(intent_cfg.get("enable_nl2cypher_fallback", True)),
        temperature_intent=float(llm_cfg.get("temperature_intent", 0.0)),
        temperature_answer=float(llm_cfg.get("temperature_answer", 0.2)),
        temperature_no_kg=float(llm_cfg.get("temperature_no_kg", 0.2)),
    )
    ner_service = NERService(runner, llm)
    clarifier = QueryClarifier(runner=runner, registry=registry, ner_service=ner_service, llm=llm)
    flow_service = InteractiveQaFlowService(clarifier=clarifier, orchestrator=orchestrator)
    return cfg, clarifier, flow_service


def _apply_professional_theme() -> None:
    st.markdown(
        """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;760;800&display=swap');
    :root {
        --msab-blue-900: #15327a;
        --msab-blue-800: #183b8f;
        --msab-blue-700: #1d4ed8;
        --msab-blue-600: #2563eb;
        --msab-blue-100: #dbeafe;
        --msab-blue-50: #eff6ff;
        --msab-ink: #152238;
        --msab-muted: #64748b;
        --msab-line: #d9e2f1;
        --msab-panel: #f8fbff;
        --msab-accent: #2563eb;
        --msab-success: #0f766e;
    }
    .stApp {
        background: #ffffff;
        color: var(--msab-ink);
        font-family: Inter, "Segoe UI", Arial, sans-serif;
    }
    .block-container {
        padding-top: 0;
        max-width: 1240px;
    }
    h1, h2, h3 {
        color: var(--msab-ink);
        letter-spacing: 0;
    }
    p, li, div {
        letter-spacing: 0;
    }
    #MainMenu, footer, header {
        visibility: hidden;
    }
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid var(--msab-line);
        padding: 0.8rem 1rem;
        border-radius: 8px;
    }
    div[data-testid="stChatMessage"] {
        border: 1px solid #eaecf0;
        border-radius: 8px;
        background: #ffffff;
    }
    div[data-testid="stExpander"] {
        border: 1px solid var(--msab-line);
        border-radius: 8px;
        background: #ffffff;
    }
    .msab-brand {
        display: flex;
        align-items: center;
        gap: 0.72rem;
        min-height: 3.1rem;
    }
    .msab-brand-mark {
        width: 2.15rem;
        height: 2.15rem;
        border: 2px solid #93c5fd;
        border-radius: 8px;
        transform: rotate(45deg);
        position: relative;
        flex: 0 0 auto;
    }
    .msab-brand-mark:after {
        content: "";
        position: absolute;
        inset: 0.35rem;
        border: 2px solid #ffffff;
        border-radius: 5px;
    }
    .msab-brand-name {
        color: #ffffff;
        font-size: 1.34rem;
        font-weight: 760;
        line-height: 1;
    }
    .msab-brand-sub {
        color: #bfdbfe;
        font-size: 0.72rem;
        font-weight: 600;
        margin-top: 0.25rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .msab-topbar {
        background: linear-gradient(135deg, #102a67 0%, #15327a 48%, #1d4ed8 100%);
        margin: 0 calc(50% - 50vw) 1.2rem;
        box-shadow: 0 10px 26px rgba(21, 50, 122, 0.18);
    }
    .msab-topbar-inner {
        max-width: 1240px;
        margin: 0 auto;
        min-height: 4.7rem;
        padding: 0.75rem 1.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 1.5rem;
    }
    .msab-nav-menu {
        display: flex;
        align-items: center;
        justify-content: flex-end;
        gap: 0.35rem;
        flex-wrap: wrap;
    }
    .msab-nav-link {
        color: #dbeafe;
        border: 1px solid rgba(219, 234, 254, 0.22);
        border-radius: 8px;
        padding: 0.68rem 0.95rem;
        min-width: 6.1rem;
        text-align: center;
        font-weight: 760;
        text-decoration: none;
        line-height: 1;
        transition: transform 140ms ease, background 140ms ease, border-color 140ms ease, box-shadow 140ms ease;
    }
    .msab-nav-link:hover,
    .msab-nav-link.active {
        color: #ffffff;
        background: rgba(255, 255, 255, 0.14);
        border-color: rgba(255, 255, 255, 0.42);
        box-shadow: 0 12px 26px rgba(8, 23, 61, 0.22);
        transform: translateY(-2px);
        text-decoration: none;
    }
    .msab-nav-link.disabled {
        opacity: 0.46;
        pointer-events: none;
    }
    .msab-nav-spacer {
        display: none;
    }
    .msab-hero-band {
        background:
            radial-gradient(circle at 85% 12%, rgba(96, 165, 250, 0.35), transparent 26rem),
            linear-gradient(135deg, #112b69 0%, #183b8f 48%, #1d4ed8 100%);
        border-radius: 8px;
        color: #ffffff;
        padding: 4.1rem 3.3rem;
        margin: 0.5rem 0 1.45rem;
        overflow: hidden;
    }
    .msab-kicker {
        color: #93c5fd;
        font-size: 0.78rem;
        font-weight: 760;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 1rem;
    }
    .msab-hero-title {
        max-width: 760px;
        font-size: 3.1rem;
        line-height: 1.08;
        font-weight: 800;
        margin-bottom: 1rem;
        color: #ffffff;
    }
    .msab-hero-subtitle {
        max-width: 780px;
        color: #dbeafe;
        line-height: 1.65;
        font-size: 1.02rem;
    }
    .msab-hero-note {
        max-width: 760px;
        color: #bfdbfe;
        font-size: 0.88rem;
        margin-top: 1.2rem;
    }
    .msab-section-title {
        color: var(--msab-blue-900);
        font-size: 1.75rem;
        font-weight: 800;
        margin: 1.4rem 0 0.35rem;
    }
    .msab-section-subtitle {
        color: var(--msab-muted);
        max-width: 850px;
        line-height: 1.6;
        margin-bottom: 0.75rem;
    }
    .msab-stat-card {
        border: 1px solid var(--msab-line);
        border-radius: 8px;
        background: #ffffff;
        padding: 1.25rem 1.25rem 1.15rem;
        min-height: 145px;
        box-shadow: 0 10px 25px rgba(21, 50, 122, 0.07);
    }
    .msab-stat-icon {
        width: 2.15rem;
        height: 2.15rem;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #ffffff;
        background: linear-gradient(135deg, #2563eb 0%, #38bdf8 100%);
        font-weight: 800;
        margin-bottom: 1rem;
    }
    .msab-stat-value {
        color: var(--msab-blue-900);
        font-size: 2rem;
        line-height: 1;
        font-weight: 800;
        margin-bottom: 0.45rem;
    }
    .msab-stat-label {
        color: var(--msab-ink);
        font-size: 0.94rem;
        font-weight: 700;
        margin-bottom: 0.35rem;
    }
    .msab-stat-caption {
        color: #7183a6;
        font-size: 0.86rem;
    }
    .msab-card {
        border: 1px solid var(--msab-line);
        border-radius: 8px;
        background: #ffffff;
        padding: 1.05rem 1.1rem;
        min-height: 136px;
        box-shadow: 0 8px 22px rgba(21, 50, 122, 0.05);
    }
    .msab-card h4 {
        margin: 0 0 0.35rem 0;
        color: var(--msab-blue-900);
        font-size: 1rem;
    }
    .msab-card p {
        color: var(--msab-muted);
        margin: 0;
        line-height: 1.45;
        font-size: 0.92rem;
    }
    .msab-feature-card {
        min-height: 158px;
        height: 158px;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
    }
    .msab-caveat {
        border-left: 4px solid var(--msab-blue-600);
        background: var(--msab-blue-50);
        padding: 0.75rem 0.9rem;
        border-radius: 6px;
        color: #1e3a8a;
        margin: 0.75rem 0;
    }
    .msab-section-card {
        border: 1px solid var(--msab-line);
        border-radius: 8px;
        background: #ffffff;
        padding: 1.05rem 1.1rem;
        margin: 0.65rem 0;
        box-shadow: 0 8px 22px rgba(21, 50, 122, 0.04);
    }
    .msab-scope-list {
        margin: 0.35rem 0 0;
        padding-left: 1rem;
        color: var(--msab-muted);
        line-height: 1.65;
        font-size: 0.92rem;
    }
    .msab-scope-panel {
        background: #ffffff;
        border: 1px solid #bfdbfe;
        border-radius: 8px;
        padding: 1rem 1.05rem;
        box-sizing: border-box;
        min-height: 430px;
        box-shadow: 0 8px 22px rgba(37, 99, 235, 0.06);
    }
    .msab-scope-panel h4 {
        color: var(--msab-blue-900);
        margin: 0 0 0.7rem;
        font-size: 1rem;
    }
    .msab-scope-panel ul {
        margin: 0;
        padding-left: 1.05rem;
        color: #35537f;
        line-height: 1.65;
        font-size: 0.92rem;
    }
    .msab-scope-panel.supported {
        background: linear-gradient(180deg, #ffffff 0%, #eff6ff 100%);
    }
    .msab-scope-panel.unsupported {
        background: #ffffff;
    }
    .msab-contact-row {
        border-bottom: 1px solid #edf2fb;
        padding: 0.85rem 0;
    }
    .msab-contact-label {
        color: var(--msab-blue-900);
        font-weight: 760;
        margin-bottom: 0.2rem;
    }
    .msab-contact-text {
        color: var(--msab-muted);
        line-height: 1.55;
    }
    .msab-footer {
        border-top: 1px solid var(--msab-line);
        color: var(--msab-muted);
        font-size: 0.86rem;
        margin-top: 2.25rem;
        padding-top: 1rem;
    }
    .msab-nav-label {
        display: none;
    }
    .stButton > button,
    .stDownloadButton > button,
    div[data-testid="stFormSubmitButton"] button {
        border: 1px solid var(--msab-blue-600) !important;
        border-radius: 8px;
        min-height: 2.55rem;
        font-weight: 700;
        letter-spacing: 0;
        color: var(--msab-blue-900) !important;
        background: #ffffff !important;
        transition: transform 140ms ease, background 140ms ease, box-shadow 140ms ease;
    }
    .stButton > button:hover,
    .stDownloadButton > button:hover,
    div[data-testid="stFormSubmitButton"] button:hover {
        background: var(--msab-blue-600) !important;
        border-color: var(--msab-blue-600) !important;
        color: #ffffff !important;
        box-shadow: 0 10px 22px rgba(37, 99, 235, 0.18);
        transform: translateY(-1px);
    }
    .stButton > button[kind="primary"],
    .stDownloadButton > button[kind="primary"],
    div[data-testid="stFormSubmitButton"] button[kind="primary"] {
        background: var(--msab-blue-600) !important;
        border-color: var(--msab-blue-600) !important;
        color: #ffffff !important;
    }
    .stButton > button[kind="primary"]:hover,
    .stDownloadButton > button[kind="primary"]:hover,
    div[data-testid="stFormSubmitButton"] button[kind="primary"]:hover {
        background: var(--msab-blue-700) !important;
        border-color: var(--msab-blue-700) !important;
    }
    @media (max-width: 768px) {
        .msab-topbar-inner {
            align-items: flex-start;
            flex-direction: column;
            padding: 0.95rem 1rem;
        }
        .msab-nav-menu {
            width: 100%;
            justify-content: flex-start;
        }
        .msab-nav-link {
            flex: 1 1 42%;
            min-width: 0;
        }
        .msab-feature-card {
            height: auto;
        }
        .msab-hero-band {
            padding: 2.2rem 1.35rem;
        }
        .msab-hero-title {
            font-size: 2.05rem;
        }
        .msab-brand-name {
            font-size: 1.05rem;
        }
    }
</style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _load_entity_guide(_clarifier, limit: int = 6, selected_labels: list[str] | None = None):
    guide = {}
    labels = selected_labels or list(ENTITY_GUIDE_QUERIES.keys())
    for label in labels:
        cypher = ENTITY_GUIDE_QUERIES[label].format(limit=limit)
        try:
            rows = _clarifier.runner.run(cypher, enforce_limit=False)
        except Exception:
            rows = []
        values = [str(row.get("value")).strip() for row in rows if row.get("value")]
        if values:
            guide[label] = values
    return guide


@st.cache_data(ttl=3600, show_spinner=False)
def _load_entity_guide_counts(_clarifier, selected_labels: list[str] | None = None):
    counts = {}
    labels = selected_labels or list(ENTITY_GUIDE_COUNT_QUERIES.keys())
    for label in labels:
        cypher = ENTITY_GUIDE_COUNT_QUERIES[label]
        try:
            rows = _clarifier.runner.run(cypher, enforce_limit=False)
        except Exception:
            rows = []
        if rows and rows[0].get("total") is not None:
            counts[label] = int(rows[0]["total"])
    return counts


def _format_stat_value(value) -> str:
    if value is None:
        return "--"
    try:
        number = int(value)
    except (TypeError, ValueError):
        return str(value)
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if number >= 10_000:
        return f"{number / 1_000:.1f}K"
    return f"{number:,}"


def _load_database_stats(clarifier):
    stats = []
    fallback_counts = None
    for item in DATABASE_STAT_QUERIES:
        total = None
        try:
            rows = clarifier.runner.run(item["cypher"], enforce_limit=False)
        except Exception:
            rows = []
        if rows and rows[0].get("total") is not None:
            try:
                total = int(rows[0]["total"])
            except (TypeError, ValueError):
                total = rows[0]["total"]
        if total is None and item.get("fallback"):
            if fallback_counts is None:
                fallback_counts = _load_entity_guide_counts(clarifier)
            total = fallback_counts.get(item["fallback"])
        if total is None:
            total = item.get("verified_total")
        stats.append({**item, "value": total})
    return stats


@st.cache_data(ttl=3600, show_spinner=False)
def _load_database_stats_cached(_clarifier):
    return _load_database_stats(_clarifier)


def _database_stats_snapshot():
    return [{**item, "value": item.get("verified_total")} for item in DATABASE_STAT_QUERIES]


def _start_example_query(clarifier, flow_service, question: str, intent_name: str | None = None, auto_run: bool = False):
    draft = clarifier.start(question)
    if intent_name and draft.selected_intent != intent_name:
        draft = clarifier.apply_intent_selection(draft, intent_name)
    ready, _ = clarifier.validate_ready_for_execution(draft)
    if auto_run and ready:
        return _queue_execution(draft)
    if auto_run:
        draft = _advance_toward_execution(draft, clarifier, flow_service, wants_to_run=True, allow_top_intent=True)
        if draft.flow_state in {"draft_ready", "awaiting_confirmation"} and draft.selected_intent:
            return _queue_execution(draft)
    return draft


def _should_auto_run_draft(draft: QueryDraft | None) -> bool:
    if draft is None:
        return False
    if draft.flow_state not in {"draft_ready", "awaiting_confirmation"}:
        return False
    if not draft.selected_intent:
        return False
    return not draft.slots


def _maybe_auto_execute_draft(draft: QueryDraft, clarifier, flow_service):
    draft = clarifier.resolve_unique_candidate_slots(draft)
    if clarifier.should_auto_execute(draft):
        return _queue_execution(draft)
    return draft


def _queue_execution(draft: QueryDraft) -> QueryDraft:
    draft.flow_state = "executing"
    draft.next_question = None
    st.session_state.execution_pending = True
    st.session_state.execution_running = False
    return draft


def _interaction_locked() -> bool:
    return bool(st.session_state.get("execution_pending") or st.session_state.get("execution_running"))


def _advance_toward_execution(
    draft: QueryDraft,
    clarifier,
    flow_service,
    *,
    wants_to_run: bool = False,
    allow_top_intent: bool = False,
):
    draft = clarifier.resolve_unique_candidate_slots(draft)
    if (wants_to_run or allow_top_intent) and not draft.selected_intent and draft.intent_candidates:
        draft = clarifier.apply_intent_selection(draft, draft.intent_candidates[0].name)
        draft = clarifier.resolve_unique_candidate_slots(draft)
    ready, _ = clarifier.validate_ready_for_execution(draft)
    if wants_to_run and ready:
        return _queue_execution(draft)
    return _maybe_auto_execute_draft(draft, clarifier, flow_service)


def _ensure_state():
    if st.session_state.get("interactive_state_version") != SESSION_STATE_VERSION:
        st.session_state.interactive_state_version = SESSION_STATE_VERSION
        st.session_state.interactive_draft = None
        st.session_state.guide_entity_focus = "Assignees"
        st.session_state.execution_pending = False
        st.session_state.execution_running = False
        st.session_state.scope_acknowledged = False
        st.session_state.site_section = "Introduction"
    if "interactive_draft" not in st.session_state:
        st.session_state.interactive_draft = None
    if "guide_entity_focus" not in st.session_state:
        st.session_state.guide_entity_focus = "Assignees"
    if "execution_pending" not in st.session_state:
        st.session_state.execution_pending = False
    if "execution_running" not in st.session_state:
        st.session_state.execution_running = False
    if "scope_acknowledged" not in st.session_state:
        st.session_state.scope_acknowledged = False
    if "site_section" not in st.session_state:
        st.session_state.site_section = "Introduction"


def _slot_display_name(slot_name: str) -> str:
    return SLOT_LABEL.get(slot_name, slot_name.replace("_", " ").title())


def _is_out_of_scope_question(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not normalized:
        return False
    has_target_context = any(term in normalized for term in ("target", "target pair", "combination", "drug", "therapy"))
    has_landscape_metric = any(
        term in normalized
        for term in ("patent", "assignee", "company", "origin", "year", "trend", "count", "disclosure", "crowding")
    )
    if has_landscape_metric:
        return False
    return has_target_context and any(re.search(pattern, normalized) for pattern in OUT_OF_SCOPE_PATTERNS)


def _build_out_of_scope_draft(question: str) -> QueryDraft:
    answer = """
This question is outside the scope of MsAb-PatKG Intelligence.

MsAb-PatKG can analyze patent landscape signals such as patent counts, first-disclosure timing, assignees, origins, target-pair crowding, pathways, functional categories, and technology classes. It does not determine whether a target pair is biologically best, clinically effective, safer, or more likely to succeed.

For target-pair-specific analyses, unresolved placeholder categories such as anti-infective external-antigen records are not treated as resolved human target pairs.

You can reformulate the question as one of these patent landscape queries:

- Which target pairs have the highest patent count?
- Which target pairs first appeared in the last 3 years?
- Which assignees are active around PD-1/VEGFA?
- Which functional categories are most prevalent among recent target-pair patents?
    """.strip()
    return QueryDraft(
        raw_question=question,
        flow_state="completed",
        chat_history=[{"role": "user", "content": question}],
        selected_intent="OUT_OF_SCOPE",
        answer_bundle={
            "mode": "scope_guardrail",
            "answer": answer,
            "debug": {
                "intent": "OUT_OF_SCOPE",
                "reason": "The question asks for biological, clinical, or predictive judgment rather than a patent landscape metric.",
                "scope": "Patent landscape analytics only",
            },
        },
    )


def _has_active_target_pair(draft: QueryDraft) -> bool:
    tp_slot = draft.slots.get("tp_name")
    if not tp_slot:
        return False
    return bool(tp_slot.selected_value or tp_slot.raw_text or tp_slot.candidates)


def _should_hide_slot(draft: QueryDraft, slot_name: str) -> bool:
    return slot_name in {"target1", "target2"} and _has_active_target_pair(draft)


def _visible_slots(draft: QueryDraft):
    return [slot for slot in draft.slots.values() if not _should_hide_slot(draft, slot.slot_name)]


def _visible_unresolved_required_slots(draft: QueryDraft):
    return [slot for slot in _visible_slots(draft) if slot.required and not slot.is_ready()]


def _visible_resolved_slots(draft: QueryDraft):
    return [slot for slot in _visible_slots(draft) if slot.status == "resolved"]


def _current_focus_slot(draft: QueryDraft):
    unresolved = _visible_unresolved_required_slots(draft)
    if unresolved:
        return unresolved[0]
    optional = [slot for slot in _visible_slots(draft) if not slot.required and not slot.is_ready()]
    return optional[0] if optional else None


def _render_sidebar(draft: QueryDraft | None):
    with st.sidebar:
        st.header("MsAb-PatKG")
        st.caption("Patent landscape intelligence, not biological prediction.")
        if st.button("Read Scope & Usage", use_container_width=True, disabled=_interaction_locked()):
            st.session_state.scope_acknowledged = False
            st.rerun()
        st.markdown("---")
        st.subheader("Query Status")
        if draft is None:
            st.write("1. Ask your question")
            st.write("2. Confirm the rewritten query")
            st.write("3. Run query on MsAb-PatKG")
        else:
            st.write(f"Stage: `{STATE_LABEL.get(draft.flow_state, draft.flow_state)}`")
            st.write(f"Clarification turns: `{draft.turn_count}/{MAX_CLARIFICATION_TURNS}`")
            st.write(f"Intent: `{draft.selected_intent or 'Not confirmed'}`")
            unresolved = _visible_unresolved_required_slots(draft)
            st.write(f"Unresolved items: `{len(unresolved)}`")
            resolved = _visible_resolved_slots(draft)
            if resolved:
                st.subheader("Confirmed")
                for slot in resolved:
                    st.write(f"- `{_slot_display_name(slot.slot_name)}`: `{slot.selected_value}`")

        st.markdown("---")
        with st.expander("Help & Examples", expanded=False):
            st.caption("Use these quick examples or real KG names whenever you need guidance.")
            st.write("**Supported scope**")
            for item in SUPPORTED_SCOPE:
                st.caption(f"- {item}")
            st.write("**Real entity examples in this KG**")
            st.caption("Use the Scenario page entity guide to inspect up to 50 real names for one category at a time.")
            st.write("**Scenario walkthroughs**")
            for scenario_name, questions in SCENARIO_EXAMPLES.items():
                st.write(f"- {scenario_name}")
                for question in questions:
                    st.caption(question)
            guide = _load_entity_guide(st.session_state._clarifier_ref, limit=8)
            for label, values in guide.items():
                st.write(f"**{label}:** " + ", ".join(f"`{value}`" for value in values))


def _render_backend_warning(clarifier):
    warning = getattr(clarifier, "backend_warning", None)
    if not warning:
        return
    st.warning(
        "Neo4j-backed candidate lookup is not fully ready. "
        "The chat can still help rewrite and confirm your query, but database suggestions may be limited.\n\n"
        f"Details: {warning}"
    )


def _render_progress_header(draft: QueryDraft | None):
    if draft is None:
        st.progress(0.0)
        st.caption("Step 1 of 3: Ask your question. The system will rewrite it before any query runs.")
        return
    st.progress(STATE_PROGRESS.get(draft.flow_state, 0.0))
    stage = STATE_LABEL.get(draft.flow_state, draft.flow_state)
    if draft.flow_state == "completed":
        st.caption("Current stage: Completed. You can ask a new question below at any time.")
        return
    if draft.flow_state == "error":
        st.caption("Current stage: Error. You can fix the current draft or ask a new question below.")
        return
    if draft.flow_state == "executing":
        st.caption("Current stage: Running. MsAb-PatKG is querying Neo4j and generating the answer; inputs are locked until this finishes.")
        return
    st.caption(f"Current stage: {stage}. Please finish query confirmation before MsAb-PatKG starts the final search.")


def _query_preview_text(draft: QueryDraft) -> str:
    preview = draft.rewritten_question or draft.raw_question
    return preview.strip()


def _answer_uses_resolved_targetpair_scope(draft: QueryDraft) -> bool:
    if not draft.answer_bundle:
        return False
    debug = draft.answer_bundle.get("debug", {}) if isinstance(draft.answer_bundle, dict) else {}
    intent = str(debug.get("final_intent") or debug.get("intent") or draft.selected_intent or "").upper()
    cypher = str(debug.get("cypher") or "")
    rows = debug.get("graph_results") or []
    if debug.get("resolved_targetpair_scope"):
        return True
    if "TP.NAME CONTAINS '/'" in cypher.upper():
        return True
    if any(key in intent for key in ("TOP_TARGETPAIR", "TARGETPAIR_DIVERSITY", "DISTINCT_TARGETPAIR", "FUNCTION_COMBINATION")):
        return True
    if isinstance(rows, list) and rows:
        return any(isinstance(row, dict) and ("target_pair" in row or "target_pairs" in row) for row in rows)
    return False


def _render_header():
    return None


def _set_site_section(section: str) -> None:
    if section not in APP_NAV_SECTIONS:
        return
    st.session_state.site_section = section
    try:
        st.query_params["site_section"] = section
    except Exception:
        return


def _render_navigation() -> str:
    current = st.session_state.get("site_section", "Introduction")
    query_section = st.query_params.get("site_section")
    if isinstance(query_section, list):
        query_section = query_section[0] if query_section else None
    if query_section in APP_NAV_SECTIONS and not (_interaction_locked() and query_section != "Ask"):
        current = query_section
        st.session_state.site_section = current
    if current not in APP_NAV_SECTIONS:
        current = "Introduction"
        st.session_state.site_section = current

    nav_links = []
    for section in APP_NAV_SECTIONS:
        is_disabled = _interaction_locked() and section != "Ask"
        classes = ["msab-nav-link"]
        if section == current:
            classes.append("active")
        if is_disabled:
            classes.append("disabled")
        href = "#" if is_disabled else f"?site_section={quote(section)}"
        nav_links.append(f'<a class="{" ".join(classes)}" href="{href}">{html.escape(section)}</a>')

    st.markdown(
        f"""
<div class="msab-topbar">
  <div class="msab-topbar-inner">
<div class="msab-brand">
  <div class="msab-brand-mark"></div>
  <div>
    <div class="msab-brand-name">{APP_TITLE}</div>
    <div class="msab-brand-sub">Patent landscape platform</div>
  </div>
</div>
    <nav class="msab-nav-menu" aria-label="Primary navigation">
      {"".join(nav_links)}
    </nav>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )
    return st.session_state.get("site_section", current)


def _render_footer() -> None:
    st.markdown(
        f"""
<div class="msab-footer">
  <div>{APP_COPYRIGHT}</div>
  <div>MsAb-PatKG Intelligence summarizes graph-backed patent landscape evidence. It is not medical, legal, investment, or target-prioritization advice.</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_scope_gate(show_header: bool = True) -> bool:
    if st.session_state.get("scope_acknowledged"):
        return True
    if show_header:
        _render_header()
    st.markdown("### Before You Use MsAb-PatKG")
    st.markdown(
        "MsAb-PatKG Intelligence is designed for **patent landscape analysis** of bispecific antibody patents. "
        "It is not a target-pair prediction engine and should not be used to infer biological or clinical superiority."
    )

    supported_items = "".join(f"<li>{html.escape(item)}</li>" for item in SUPPORTED_SCOPE)
    unsupported_items = "".join(f"<li>{html.escape(item)}</li>" for item in OUT_OF_SCOPE)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            f"""
<div class="msab-scope-panel supported">
  <h4>Supported Questions</h4>
  <ul>{supported_items}</ul>
</div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"""
<div class="msab-scope-panel unsupported">
  <h4>Not Supported</h4>
  <ul>{unsupported_items}</ul>
</div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        """
#### Recommended Workflow
1. Start from one of the curated scenarios.
2. Use the real-entity guide to inspect exact KG names.
3. Ask questions using patent landscape metrics such as patent count, first disclosure, assignee, origin, pathway, or functional category.
        """
    )
    st.markdown(f'<div class="msab-caveat">{LANDSCAPE_CAVEAT}</div>', unsafe_allow_html=True)
    if st.button("I understand the scope and want to continue", type="primary", use_container_width=True):
        st.session_state.scope_acknowledged = True
        st.rerun()
    return False


def _render_chat_intro():
    with st.chat_message("assistant"):
        st.markdown(
            """
Ask a patent landscape question below, or start from a scenario example. I will rewrite the question into a database-friendly query, confirm entities when needed, and then run it on MsAb-PatKG.
            """
        )


def _render_persistent_guide(clarifier, flow_service):
    st.markdown('<div class="msab-section-title">Research Console</div>', unsafe_allow_html=True)
    scenario_tab, entity_tab, scope_tab = st.tabs(["Scenario Questions", "Real Entity Examples", "Scope & Limitations"])
    with scenario_tab:
        _render_scenario_quick_start(clarifier, flow_service)
    with entity_tab:
        _render_entity_examples_guide(clarifier)
    with scope_tab:
        _render_scope_summary()


def _render_database_stats(clarifier=None, *, live: bool = False):
    st.markdown('<div class="msab-section-title">Current database coverage</div>', unsafe_allow_html=True)
    if live and clarifier is not None:
        subtitle = (
            "Statistics are calculated from the current MsAb-PatKG knowledge graph and describe "
            "patent-landscape coverage, not biological or clinical performance. Values fall back to "
            "the database snapshot verified on May 11, 2026 if the live graph is temporarily unavailable."
        )
        stats = _load_database_stats_cached(clarifier)
    else:
        subtitle = (
            "Statistics use the database snapshot verified on May 11, 2026 so the Introduction page can "
            "load quickly. Live graph-backed querying starts when you enter Scenario or Ask."
        )
        stats = _database_stats_snapshot()
    st.markdown(
        f"""
<div class="msab-section-subtitle">
{subtitle}
</div>
        """,
        unsafe_allow_html=True,
    )
    for row_start in range(0, len(stats), 4):
        cols = st.columns(4)
        for idx, item in enumerate(stats[row_start:row_start + 4]):
            with cols[idx]:
                st.markdown(
                    f"""
<div class="msab-stat-card">
  <div class="msab-stat-icon">#</div>
  <div class="msab-stat-value">{_format_stat_value(item.get("value"))}</div>
  <div class="msab-stat-label">{item["label"]}</div>
  <div class="msab-stat-caption">{item["caption"]}</div>
</div>
                    """,
                    unsafe_allow_html=True,
                )


def _render_introduction_section():
    st.markdown(
        f"""
<section class="msab-hero-band">
  <div class="msab-kicker">Multispecific antibody patent landscape</div>
  <div class="msab-hero-title">Queryable patent intelligence for multispecific antibodies</div>
  <div class="msab-hero-subtitle">
    {APP_TITLE} turns a manually curated multispecific-antibody patent landscape into a structured,
    queryable, and traceable knowledge graph. Use it to explore patent families, target-pair combinations,
    assignees, pathways, functional categories, technology classes, origins, and time-based competitive activity.
  </div>
  <div class="msab-hero-note">
    The current resource focuses on core patents that directly reflect multispecific-antibody construction and competitive positioning. It is not a target-pair prediction engine.
  </div>
</section>
        """,
        unsafe_allow_html=True,
    )
    cta_cols = st.columns([1, 1, 4])
    with cta_cols[0]:
        if st.button("Start Asking", type="primary", use_container_width=True):
            _set_site_section("Ask")
            st.rerun()
    with cta_cols[1]:
        if st.button("View Scenarios", use_container_width=True):
            _set_site_section("Scenario")
            st.rerun()

    _render_database_stats()

    st.markdown('<div class="msab-section-title">What this platform is for</div>', unsafe_allow_html=True)
    cols = st.columns(3)
    with cols[0]:
        st.markdown(
            """
<div class="msab-card msab-feature-card">
  <h4>Patent Landscape Analysis</h4>
  <p>Explore patent counts, publication timelines, first-disclosure years, assignee activity, regional origins, and competitive crowding.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            """
<div class="msab-card msab-feature-card">
  <h4>Target-Pair Evidence</h4>
  <p>Inspect resolved target-pair combinations disclosed in core patents and connect them to pathways, functional categories, cancers, and technology classes.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            """
<div class="msab-card msab-feature-card">
  <h4>Grounded QA Workflow</h4>
  <p>Ask a question, confirm the interpreted entities and intent, then receive graph-backed results with explicit scope limitations.</p>
</div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown('<div class="msab-section-title">Data scope</div>', unsafe_allow_html=True)
    st.markdown(
        """
<div class="msab-section-card">
  <h4>Curated core patent landscape</h4>
  <p>MsAb-PatKG is built from systematically retrieved, LLM-screened, manually denoised, family-normalized, and semantically annotated patents related to bispecific and other multispecific antibodies.</p>
  <ul class="msab-scope-list">
    <li>Included: core patents that explicitly reflect multispecific-antibody construction, target-combination design, and competitive positioning.</li>
    <li>Structured entities: Patent, Family, Year, Assignee, Origin, Target, TargetPair, Pathway, Functional_of_Target, Cancer, and TechnologyClass1.</li>
    <li>Resolved target-pair analyses use 700 fully resolved human target-pair identities and exclude the unresolved anti-infective placeholder category.</li>
    <li>Not intended as: a complete universe of all broadly related antibody patents, platform-only patents, formulation-only patents, early validation records, or target-pair prediction evidence.</li>
  </ul>
</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f'<div class="msab-caveat">{LANDSCAPE_CAVEAT}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="msab-caveat">{RESOLVED_TARGETPAIR_CAVEAT}</div>', unsafe_allow_html=True)

    if not st.session_state.get("scope_acknowledged"):
        if st.button("I understand the scope and want to use the platform", type="primary", use_container_width=True):
            st.session_state.scope_acknowledged = True
            _set_site_section("Ask")
            st.rerun()


def _render_home_section(clarifier, flow_service):
    _render_introduction_section()


def _render_scenario_section(clarifier, flow_service):
    st.markdown('<div class="msab-section-title">Scenario</div>', unsafe_allow_html=True)
    st.markdown(
        """
<div class="msab-section-subtitle">
Start from one of the curated analysis scenarios when you are not sure how to phrase a patent-landscape question.
Each example is intentionally scoped to database-backed evidence.
</div>
        """,
        unsafe_allow_html=True,
    )
    _render_scenario_quick_start(clarifier, flow_service)
    st.markdown('<div class="msab-section-title">Use real graph entities</div>', unsafe_allow_html=True)
    _render_entity_examples_guide(clarifier)


def _render_ask_section(draft: QueryDraft | None, clarifier, flow_service):
    st.markdown('<div class="msab-section-title">Ask MsAb-PatKG</div>', unsafe_allow_html=True)
    st.markdown(
        """
<div class="msab-section-subtitle">
Use this console for patent-landscape questions. The system will first rewrite and confirm your query before running it against the knowledge graph.
</div>
        """,
        unsafe_allow_html=True,
    )
    cols = st.columns(3)
    with cols[0]:
        st.markdown(
            """
<div class="msab-card">
  <h4>Ask within scope</h4>
  <p>Use patent metrics such as count, trend, first disclosure, assignee, origin, target pair, pathway, or technology class.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            """
<div class="msab-card">
  <h4>Confirm before search</h4>
  <p>The assistant may ask you to confirm entities or intent before it sends a query to MsAb-PatKG.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            """
<div class="msab-card">
  <h4>Read results as evidence</h4>
  <p>Answers summarize graph-backed patent evidence and should not be treated as clinical, legal, or investment advice.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with st.expander("Example questions", expanded=False):
        _render_scenario_quick_start(clarifier, flow_service)
    _render_progress_header(draft)
    _render_execution_notice()
    if draft is None:
        _render_chat_intro()
    else:
        render_chat_flow(draft, clarifier, flow_service)


def _render_status_section(draft: QueryDraft | None, clarifier):
    st.markdown("### Status")
    warning = getattr(clarifier, "backend_warning", None)
    cols = st.columns(4)
    with cols[0]:
        st.metric("Application", "Online")
    with cols[1]:
        st.metric("Knowledge Graph", "Limited" if warning else "Ready")
    with cols[2]:
        st.metric("LLM Entity Matching", "Enabled")
    with cols[3]:
        st.metric("Current Stage", STATE_LABEL.get(draft.flow_state, "Idle") if draft else "Idle")

    st.markdown("#### Current Query")
    if draft is None:
        st.info("No active query. Open the Ask page or run a Scenario example to start.")
    else:
        st.write(f"**Intent:** `{draft.selected_intent or 'Not confirmed'}`")
        st.write(f"**State:** `{STATE_LABEL.get(draft.flow_state, draft.flow_state)}`")
        st.write(f"**Clarification turns:** `{draft.turn_count}/{MAX_CLARIFICATION_TURNS}`")
        if _visible_resolved_slots(draft):
            st.write("**Resolved entities:**")
            for slot in _visible_resolved_slots(draft):
                st.write(f"- `{_slot_display_name(slot.slot_name)}`: `{slot.selected_value}`")
        if _visible_unresolved_required_slots(draft):
            st.write("**Still needs confirmation:**")
            for slot in _visible_unresolved_required_slots(draft):
                st.write(f"- `{_slot_display_name(slot.slot_name)}`")

    st.markdown("#### Public Scope")
    _render_scope_summary()
    if warning:
        st.warning(f"Neo4j-backed candidate lookup warning: {warning}")


def _scenario_examples_csv() -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["scenario", "question"])
    writer.writeheader()
    for scenario_name, questions in SCENARIO_EXAMPLES.items():
        for question in questions:
            writer.writerow({"scenario": scenario_name, "question": question})
    return buffer.getvalue()


def _entity_examples_csv(clarifier, limit: int = 50) -> str:
    counts = _load_entity_guide_counts(clarifier)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["entity_type", "value", "rank", "shown_limit", "total_count"])
    writer.writeheader()
    for label in ENTITY_GUIDE_QUERIES:
        guide = _load_entity_guide(clarifier, limit=limit, selected_labels=[label])
        for rank, value in enumerate(guide.get(label, []), start=1):
            writer.writerow(
                {
                    "entity_type": label,
                    "value": value,
                    "rank": rank,
                    "shown_limit": limit,
                    "total_count": counts.get(label, ""),
                }
            )
    return buffer.getvalue()


def _render_download_section(clarifier):
    st.markdown("### Download")
    st.markdown(
        "These files are lightweight public-facing aids for using the web tool. "
        "They are not a full export of the underlying patent knowledge graph."
    )
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            """
<div class="msab-section-card">
  <h4>Scenario Questions</h4>
  <p>Download the curated scenario examples shown on the Scenario page.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        st.download_button(
            "Download scenario questions CSV",
            data=_scenario_examples_csv(),
            file_name="msab_patkg_scenario_questions.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=_interaction_locked(),
        )
    with col2:
        st.markdown(
            """
<div class="msab-section-card">
  <h4>Real Entity Examples</h4>
  <p>Download up to 50 high-frequency examples for each supported entity type.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        st.download_button(
            "Download entity examples CSV",
            data=_entity_examples_csv(clarifier, limit=50),
            file_name="msab_patkg_entity_examples_top50.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=_interaction_locked(),
        )
    st.info(
        "If an entity category contains more than 50 items, the complete list should be provided through the manuscript supporting information or by contacting the authors."
    )


def _render_contact_section():
    st.markdown('<div class="msab-section-title">Contact</div>', unsafe_allow_html=True)
    st.markdown(
        """
<div class="msab-section-subtitle">
For academic access, feedback, full entity lists, supporting-information questions, or deployment issues, please contact the manuscript authors.
</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
<div class="msab-section-card">
  <div class="msab-contact-row">
    <div class="msab-contact-label">Contact</div>
    <div class="msab-contact-text">{CONTACT_NAME}. Email: {CONTACT_EMAIL}.</div>
  </div>
  <div class="msab-contact-row">
    <div class="msab-contact-label">Academic use</div>
    <div class="msab-contact-text">MsAb-PatKG Intelligence is provided for research and manuscript-supporting exploration of multispecific-antibody patent intelligence.</div>
  </div>
  <div class="msab-contact-row">
    <div class="msab-contact-label">Manuscript</div>
    <div class="msab-contact-text">Knowledge-Graph-Driven Patent Intelligence for Bispecific and Multispecific Antibodies.</div>
  </div>
  <div class="msab-contact-row">
    <div class="msab-contact-label">Answer feedback</div>
    <div class="msab-contact-text">After any answer, use the feedback form to save the question, answer, route, Cypher, returned-row summary, and user comment as a structured log. The generated email link can submit the log to the contact email for review.</div>
  </div>
  <div class="msab-contact-row">
    <div class="msab-contact-label">Recommended contact topics</div>
    <div class="msab-contact-text">Supported patent landscape scenarios, full entity-list access, failed entity matching, ambiguous query interpretation, unsatisfactory answers, reproducibility, citation, or supporting-information requests.</div>
  </div>
  <div class="msab-contact-row">
    <div class="msab-contact-label">Data note</div>
    <div class="msab-contact-text">The deployed graph currently contains 16,539 patent publications and 1,421 patent families. Add the final manuscript DOI, database version, update date, and supporting-information link after publication metadata is finalized.</div>
  </div>
  <div class="msab-contact-row">
    <div class="msab-contact-label">Scope disclaimer</div>
    <div class="msab-contact-text">The platform summarizes patent-landscape evidence only. It is not medical, legal, investment, freedom-to-operate, or target-prioritization advice.</div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_scope_summary():
    st.markdown("**What MsAb-PatKG can answer**")
    for item in SUPPORTED_SCOPE:
        st.write(f"- {item}")
    st.markdown("**What it should not be asked to decide**")
    for item in OUT_OF_SCOPE:
        st.write(f"- {item}")
    st.markdown(f'<div class="msab-caveat">{LANDSCAPE_CAVEAT}</div>', unsafe_allow_html=True)


def _render_scenario_quick_start(clarifier, flow_service):
    st.markdown("**Scenario Walkthroughs**")
    st.caption(
        "Start here if you are not familiar with the database. These examples keep the question inside the patent landscape scope."
    )
    scenario_items = list(SCENARIO_EXAMPLES.items())
    for row_start in range(0, len(scenario_items), 2):
        cols = st.columns(2)
        for col_idx, (scenario_name, questions) in enumerate(scenario_items[row_start:row_start + 2]):
            with cols[col_idx]:
                with st.container(border=True):
                    st.markdown(f"**{scenario_name}**")
                    st.caption(SCENARIO_DESCRIPTIONS.get(scenario_name, ""))
                    for q_idx, question in enumerate(questions):
                        if st.button(
                            f"Run Example {q_idx + 1}",
                            key=f"scenario_{row_start}_{col_idx}_{q_idx}",
                            use_container_width=True,
                            disabled=_interaction_locked(),
                        ):
                            st.session_state.scope_acknowledged = True
                            st.session_state.interactive_draft = _start_example_query(
                                clarifier,
                                flow_service,
                                question,
                                auto_run=True,
                            )
                            _set_site_section("Ask")
                            st.rerun()
                        st.caption(question)


def _render_entity_examples_guide(clarifier):
    st.markdown("**Real Entity Examples In This KG**")
    st.caption(
        "Choose one entity category to inspect real names from the graph. "
        "To keep the guide readable, at most 50 names are shown for each category."
    )
    labels = list(ENTITY_GUIDE_QUERIES.keys())
    cols = st.columns(4)
    for idx, label in enumerate(labels):
        with cols[idx % 4]:
            if st.button(label, key=f"guide_focus_{label}", use_container_width=True, disabled=_interaction_locked()):
                st.session_state.guide_entity_focus = label
                st.rerun()
    focus = st.session_state.get("guide_entity_focus", "Assignees")
    focused_guide = _load_entity_guide(clarifier, limit=50, selected_labels=[focus])
    focused_counts = _load_entity_guide_counts(clarifier, selected_labels=[focus])
    if not focused_guide:
        st.info("Entity examples are temporarily unavailable.")
        return
    total = focused_counts.get(focus)
    if total is not None:
        st.write(f"**{focus}: showing up to 50 real names out of {total} total**")
    else:
        st.write(f"**{focus}: showing up to 50 real names**")
    for label, values in focused_guide.items():
        st.write(", ".join(f"`{value}`" for value in values))
    if total is not None and total > 50:
        st.caption(
            "More than 50 entities exist in this category. "
            "If you need the full list, please contact the authors or consult the paper's supporting information."
        )


def _render_chat_history(draft: QueryDraft):
    rendered_initial_user = False
    for message in draft.chat_history:
        role = message.get("role", "assistant")
        content = message.get("content", "")
        if not content:
            continue
        if role == "user" and not rendered_initial_user and content == draft.raw_question:
            rendered_initial_user = True
        with st.chat_message("user" if role == "user" else "assistant"):
            st.write(content)


def _render_assistant_understanding(draft: QueryDraft):
    with st.chat_message("assistant"):
        st.markdown("**Current query draft**")
        st.info(_query_preview_text(draft))
        if draft.intent_candidates:
            top = draft.intent_candidates[0]
            st.caption(f"Most likely intent: {top.name} (score={top.confidence:.2f})")


def _render_resolved_summary(draft: QueryDraft):
    resolved = _visible_resolved_slots(draft)
    if not resolved:
        return
    with st.chat_message("assistant"):
        st.markdown("**Already confirmed**")
        for slot in resolved:
            st.write(f"- {_slot_display_name(slot.slot_name)}: `{slot.selected_value}`")


def _render_confirm_query_banner(draft: QueryDraft):
    if draft.flow_state not in {"clarifying_entities", "clarifying_intent", "draft_ready", "awaiting_confirmation"}:
        return
    with st.container(border=True):
        st.markdown("**Confirm Query Before Search**")
        st.write("The system is still confirming your query. MsAb-PatKG will not run until you approve the final draft.")
        st.code(_query_preview_text(draft))
        if draft.next_question:
            st.caption(f"Next confirmation step: {draft.next_question}")


def _render_execution_notice():
    if not _interaction_locked():
        return
    with st.container(border=True):
        st.markdown("**Running on MsAb-PatKG**")
        st.info(
            "The query is running against the knowledge graph and answer generator. "
            "This can take a few seconds. Other inputs are temporarily locked to avoid conflicting requests."
        )
        st.progress(0.96)
        if st.session_state.get("execution_pending") and not st.session_state.get("execution_running"):
            if st.button("Stop queued run", type="secondary"):
                draft = st.session_state.get("interactive_draft")
                if draft is not None:
                    draft.flow_state = "draft_ready" if draft.selected_intent else "clarifying_intent"
                    draft.next_question = "The queued run was stopped. You can refine or run the query again."
                    st.session_state.interactive_draft = draft
                st.session_state.execution_pending = False
                st.session_state.execution_running = False
                st.rerun()


def _run_pending_execution(flow_service):
    if not st.session_state.get("execution_pending"):
        return
    draft = st.session_state.get("interactive_draft")
    if draft is None:
        st.session_state.execution_pending = False
        st.session_state.execution_running = False
        return
    st.session_state.execution_running = True
    with st.spinner("Running Cypher and generating the answer. Please wait..."):
        draft = flow_service.execute(draft)
    st.session_state.interactive_draft = draft
    st.session_state.execution_pending = False
    st.session_state.execution_running = False
    st.rerun()


def _render_focus_prompt(draft: QueryDraft, clarifier, flow_service):
    slot = _current_focus_slot(draft)
    if slot is None:
        return
    with st.chat_message("assistant"):
        label = _slot_display_name(slot.slot_name)
        st.write(draft.next_question or f"I still need to confirm the `{label}` before I can finalize the query.")

        if slot.slot_name in {"assignee", "target", "cancer", "origin"}:
            st.caption(
                "If you are unsure about the exact database name, use the real-entity guide on the Scenario screen first, then ask with one of those names."
            )

        if slot.candidates:
            top_candidates = slot.candidates[:3]
            candidate_names = [f"`{candidate.label}`" for candidate in top_candidates]
            if len(candidate_names) == 1:
                suggestion_text = candidate_names[0]
            elif len(candidate_names) == 2:
                suggestion_text = " or ".join(candidate_names)
            else:
                suggestion_text = ", ".join(candidate_names[:-1]) + f", or {candidate_names[-1]}"
            st.write(
                f"In the database, I found similar {label} values such as {suggestion_text}. "
                "If one of these is what you mean, you can pick it directly below."
            )
            for idx, candidate in enumerate(top_candidates):
                button_label = f"Use {candidate.label}"
                if st.button(button_label, key=f"use_candidate_{slot.slot_name}_{idx}", disabled=_interaction_locked()):
                    updated = clarifier.apply_slot_selection(draft, slot.slot_name, candidate.value)
                    st.session_state.interactive_draft = _advance_toward_execution(updated, clarifier, flow_service)
                    st.rerun()
            if len(slot.candidates) > 3:
                with st.expander("Show more similar values", expanded=False):
                    for idx, candidate in enumerate(slot.candidates[3:6], start=3):
                        score = f"{candidate.score:.2f}" if candidate.score is not None else candidate.source
                        cols = st.columns([5, 2])
                        with cols[0]:
                            st.write(f"`{candidate.label}`")
                            st.caption(f"source={candidate.source} score={score}")
                        with cols[1]:
                            if st.button(
                                f"Use {candidate.label}",
                                key=f"use_candidate_more_{slot.slot_name}_{idx}",
                                disabled=_interaction_locked(),
                            ):
                                updated = clarifier.apply_slot_selection(
                                    draft,
                                    slot.slot_name,
                                    candidate.value,
                                )
                                st.session_state.interactive_draft = _advance_toward_execution(
                                    updated,
                                    clarifier,
                                    flow_service,
                                )
                                st.rerun()


def _render_intent_confirmation(draft: QueryDraft, clarifier, flow_service):
    with st.chat_message("assistant"):
        st.markdown("**Please confirm the query type**")
        st.write("I have enough entity information. The next step is to confirm the intent before running the query.")
        for idx, candidate in enumerate(draft.intent_candidates[:4]):
            with st.container(border=True):
                st.markdown(f"**{candidate.name}**")
                st.write(candidate.description or "No description")
                required = ", ".join(candidate.required_slots) or "none"
                st.caption(f"Required fields: {required}")
                if st.button("Use this query type", key=f"use_intent_{idx}", disabled=_interaction_locked()):
                    updated = clarifier.apply_intent_selection(draft, candidate.name)
                    st.session_state.interactive_draft = _advance_toward_execution(updated, clarifier, flow_service)
                    st.rerun()


def _render_draft_confirmation(draft: QueryDraft, flow_service):
    with st.chat_message("assistant"):
        st.markdown("**Ready to run on MsAb-PatKG**")
        st.write(draft.next_question or "If this matches your intent, I can now run it on MsAb-PatKG.")
        cols = st.columns(3)
        with cols[0]:
            if st.button("Yes, run this query", type="primary", disabled=_interaction_locked()):
                st.session_state.interactive_draft = _queue_execution(draft)
                st.rerun()
        with cols[1]:
            if st.button("Refine entities", disabled=_interaction_locked()):
                draft.flow_state = "clarifying_entities"
                st.session_state.interactive_draft = draft
                st.rerun()
        with cols[2]:
            if st.button("Refine intent", disabled=_interaction_locked()):
                draft.flow_state = "clarifying_intent"
                st.session_state.interactive_draft = draft
                st.rerun()


def _truncate_text(value: str, limit: int = 5000) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


def _feedback_payload(draft: QueryDraft, issue_type: str, comment: str, consent: bool) -> dict:
    debug = draft.answer_bundle.get("debug", {}) if draft.answer_bundle else {}
    rows = debug.get("graph_results") or []
    answer_text = draft.answer_bundle.get("answer", "") if draft.answer_bundle else ""
    answer_key = hashlib.sha256(f"{draft.raw_question}|{answer_text}".encode("utf-8")).hexdigest()[:16]
    timestamp = datetime.now(timezone.utc).isoformat()
    seed = f"{timestamp}|{draft.raw_question}|{issue_type}|{comment}"
    feedback_id = "fb_" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]
    return {
        "feedback_id": feedback_id,
        "answer_key": answer_key,
        "timestamp_utc": timestamp,
        "issue_type": issue_type,
        "user_comment": comment.strip(),
        "user_consented_to_review": consent,
        "raw_question": draft.raw_question,
        "rewritten_question": draft.rewritten_question,
        "execution_question": draft.execution_question,
        "selected_intent": draft.selected_intent,
        "flow_state": draft.flow_state,
        "answer_excerpt": _truncate_text(answer_text, 5000),
        "debug_summary": {
            "mode": draft.answer_bundle.get("mode") if draft.answer_bundle else None,
            "intent": debug.get("final_intent") or debug.get("intent"),
            "rows": debug.get("rows"),
            "cypher": debug.get("cypher"),
            "params": debug.get("params"),
            "resolved_targetpair_scope": debug.get("resolved_targetpair_scope"),
            "fallback": debug.get("fallback"),
            "fallback_reason": debug.get("fallback_reason"),
            "graph_results_preview": rows[:20] if isinstance(rows, list) else rows,
        },
    }


def _save_feedback_payload(payload: dict) -> tuple[bool, str]:
    try:
        os.makedirs(FEEDBACK_DIR, exist_ok=True)
        with open(FEEDBACK_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return True, FEEDBACK_LOG_PATH
    except OSError as exc:
        return False, str(exc)


def _feedback_mailto(payload: dict) -> str:
    subject = f"MsAb-PatKG feedback: {payload['feedback_id']}"
    body = "\n".join(
        [
            "Dear MsAb-PatKG team,",
            "",
            "I would like to submit feedback on a web-interface answer.",
            "",
            f"Feedback ID: {payload['feedback_id']}",
            f"Issue type: {payload['issue_type']}",
            f"User comment: {payload['user_comment'] or '(not provided)'}",
            "",
            f"Question: {payload['raw_question']}",
            f"Rewritten question: {payload.get('rewritten_question') or '(not available)'}",
            f"Selected intent: {payload.get('selected_intent') or '(not available)'}",
            f"Rows returned: {payload['debug_summary'].get('rows')}",
            "",
            "Answer excerpt:",
            _truncate_text(payload.get("answer_excerpt", ""), 1200),
            "",
            "This feedback can be used to improve entity normalization, query rewriting, template coverage, and route selection.",
        ]
    )
    return f"mailto:{CONTACT_EMAIL}?subject={quote(subject)}&body={quote(body)}"


def _render_feedback_form(draft: QueryDraft) -> None:
    answer_text = draft.answer_bundle.get("answer", "") if draft.answer_bundle else ""
    current_answer_key = hashlib.sha256(f"{draft.raw_question}|{answer_text}".encode("utf-8")).hexdigest()[:16]
    with st.expander("Submit feedback on this answer", expanded=False):
        st.caption(
            "Use this if the answer is incorrect, incomplete, out of scope, or the system misunderstood an entity. "
            "Submitted logs help refine entity normalization, query rewriting, template coverage, and route selection."
        )
        with st.form("answer_feedback_form", clear_on_submit=False):
            issue_type = st.selectbox(
                "What was the main issue?",
                [
                    "Incorrect answer",
                    "Missing or weak patent evidence",
                    "Entity matching problem",
                    "Wrong query type or route",
                    "Out-of-scope question handled poorly",
                    "Interface or usability issue",
                    "Other",
                ],
            )
            comment = st.text_area(
                "Optional details",
                placeholder="Describe what went wrong, what you expected, or which entity/query should be reviewed.",
                height=120,
            )
            consent = st.checkbox(
                "I agree that this question, answer, and technical query log may be reviewed by the MsAb-PatKG team for system improvement.",
                value=True,
            )
            submitted = st.form_submit_button("Save feedback log")

        if submitted:
            if not consent:
                st.warning("Please confirm review consent before saving a feedback log.")
                return
            payload = _feedback_payload(draft, issue_type, comment, consent)
            ok, message = _save_feedback_payload(payload)
            st.session_state.last_feedback_payload = payload
            if ok:
                st.success(f"Feedback saved locally with ID `{payload['feedback_id']}`.")
                st.caption(f"Server-side log path: `{message}`")
            else:
                st.error(f"Could not save the server-side feedback log: {message}")

        payload = st.session_state.get("last_feedback_payload")
        if payload and payload.get("answer_key") != current_answer_key:
            payload = None
        if payload:
            mailto = html.escape(_feedback_mailto(payload), quote=True)
            st.markdown(
                f'<a href="{mailto}" target="_blank">Send this feedback to {CONTACT_EMAIL}</a>',
                unsafe_allow_html=True,
            )
            st.download_button(
                "Download feedback JSON",
                data=json.dumps(payload, ensure_ascii=False, indent=2),
                file_name=f"{payload['feedback_id']}.json",
                mime="application/json",
                use_container_width=True,
            )


def _render_answer(draft: QueryDraft):
    debug = draft.answer_bundle.get("debug", {}) if draft.answer_bundle else {}
    rows = debug.get("graph_results") or []
    with st.chat_message("assistant"):
        st.markdown("**MsAb-PatKG Answer**")
        if draft.selected_intent and draft.selected_intent != "OUT_OF_SCOPE":
            cols = st.columns(3)
            with cols[0]:
                st.metric("Interpreted Intent", draft.selected_intent)
            with cols[1]:
                st.metric("Rows Returned", str(debug.get("rows", len(rows) if rows else 0)))
            with cols[2]:
                st.metric("Evidence Type", "Patent KG")
            st.markdown(f'<div class="msab-caveat">{LANDSCAPE_CAVEAT}</div>', unsafe_allow_html=True)
            if _answer_uses_resolved_targetpair_scope(draft):
                st.markdown(f'<div class="msab-caveat">{RESOLVED_TARGETPAIR_CAVEAT}</div>', unsafe_allow_html=True)
        st.markdown(draft.answer_bundle.get("answer", ""))
        if rows:
            with st.expander("Result table", expanded=False):
                st.dataframe(rows, use_container_width=True)
        st.caption("Ask a new patent landscape question below to start a fresh query.")
    _render_feedback_form(draft)
    with st.expander("Technical details", expanded=False):
        st.json(debug)


def _render_error(draft: QueryDraft):
    with st.chat_message("assistant"):
        st.error(draft.error_message or "Unknown error")
        if st.button("Go back to query confirmation", disabled=_interaction_locked()):
            if _visible_unresolved_required_slots(draft):
                draft.flow_state = "clarifying_entities"
            elif not draft.selected_intent:
                draft.flow_state = "clarifying_intent"
            else:
                draft.flow_state = "draft_ready"
            draft.error_message = None
            st.session_state.interactive_draft = draft
            st.rerun()


def _render_turn_limit_warning(draft: QueryDraft, clarifier, flow_service):
    if not draft.fallback_ready:
        return
    with st.chat_message("assistant"):
        st.warning(
            draft.fallback_reason
            or "We have already spent several clarification turns. I can now use the current best rewritten query and ask MsAb-PatKG directly."
        )
        if st.button("Run with current best query", key="run_best_query", disabled=_interaction_locked()):
            if not draft.selected_intent and draft.intent_candidates:
                draft = clarifier.apply_intent_selection(draft, draft.intent_candidates[0].name)
            st.session_state.interactive_draft = _queue_execution(draft)
            st.rerun()


def render_chat_flow(draft: QueryDraft, clarifier, flow_service):
    _render_chat_history(draft)
    _render_confirm_query_banner(draft)

    if draft.flow_state == "clarifying_entities":
        _render_assistant_understanding(draft)
        _render_resolved_summary(draft)
        _render_focus_prompt(draft, clarifier, flow_service)
        _render_turn_limit_warning(draft, clarifier, flow_service)
    elif draft.flow_state == "clarifying_intent":
        _render_assistant_understanding(draft)
        _render_resolved_summary(draft)
        _render_intent_confirmation(draft, clarifier, flow_service)
        _render_turn_limit_warning(draft, clarifier, flow_service)
    elif draft.flow_state in {"draft_ready", "awaiting_confirmation"}:
        _render_resolved_summary(draft)
        _render_draft_confirmation(draft, flow_service)
    elif draft.flow_state == "completed":
        _render_answer(draft)
    elif draft.flow_state == "error":
        _render_error(draft)
    elif draft.flow_state in {"analyzing", "executing"}:
        with st.chat_message("assistant"):
            with st.spinner(f"{STATE_LABEL.get(draft.flow_state, draft.flow_state)}..."):
                st.write("MsAb-PatKG is running the confirmed query. Please wait for the answer before sending another request.")


def _handle_entity_reply(draft: QueryDraft, clarifier, user_input: str):
    slot = _current_focus_slot(draft)
    if slot is None:
        return draft
    text = user_input.strip()
    if not text:
        return draft
    if slot.category == "year" and text.isdigit():
        return clarifier.apply_slot_selection(draft, slot.slot_name, text)
    return clarifier.apply_manual_slot_input(draft, slot.slot_name, text)


def _is_affirmative(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9\s]", " ", (text or "").lower())
    phrases = [
        "yes", "yeah", "yep", "correct", "that's right", "that is right",
        "exactly", "sure", "please do", "go ahead", "run it", "run the query",
        "use this", "looks good", "that is what i want", "this is what i want",
    ]
    return any(phrase in normalized for phrase in phrases)


def _is_negative(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9\s]", " ", (text or "").lower())
    phrases = ["no", "not really", "incorrect", "wrong", "not this", "something else"]
    return any(phrase in normalized for phrase in phrases)


def _wants_to_run(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9\s]", " ", (text or "").lower())
    phrases = [
        "run", "run it", "run the query", "generate cypher", "tell me the answer",
        "search now", "execute", "go ahead",
    ]
    return any(phrase in normalized for phrase in phrases)


def _handle_followup_input(draft: QueryDraft, clarifier, flow_service, user_input: str):
    text = (user_input or "").strip()
    draft.chat_history.append({"role": "user", "content": text})
    draft.turn_count += 1
    affirmative = _is_affirmative(text)
    wants_to_run = _wants_to_run(text)
    negative = _is_negative(text)

    if draft.flow_state == "clarifying_entities":
        slot = _current_focus_slot(draft)
        if slot and affirmative and slot.raw_text:
            selected_value = slot.candidates[0].value if len(slot.candidates) == 1 else slot.raw_text
            updated = clarifier.apply_slot_selection(draft, slot.slot_name, selected_value)
            return _advance_toward_execution(updated, clarifier, flow_service, wants_to_run=wants_to_run)
        if slot and negative:
            return clarifier.reject_slot_candidates(draft, slot.slot_name)
        updated = _handle_entity_reply(draft, clarifier, user_input)
        return _advance_toward_execution(updated, clarifier, flow_service, wants_to_run=wants_to_run)

    if draft.flow_state == "clarifying_intent":
        if affirmative and draft.selected_intent:
            updated = clarifier.apply_intent_selection(draft, draft.selected_intent)
            return _advance_toward_execution(updated, clarifier, flow_service, wants_to_run=wants_to_run)
        if affirmative and draft.intent_candidates:
            updated = clarifier.apply_intent_selection(draft, draft.intent_candidates[0].name)
            return _advance_toward_execution(updated, clarifier, flow_service, wants_to_run=wants_to_run)
        lowered = user_input.strip().lower()
        for candidate in draft.intent_candidates:
            if lowered == candidate.name.lower() or lowered in candidate.name.lower():
                updated = clarifier.apply_intent_selection(draft, candidate.name)
                return _advance_toward_execution(updated, clarifier, flow_service, wants_to_run=wants_to_run)
        draft.error_message = "I could not match that reply to one of the current intent candidates."
        draft.flow_state = "error"
        return draft

    if draft.flow_state in {"draft_ready", "awaiting_confirmation"} and affirmative:
        return _queue_execution(draft)

    return draft


def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    _apply_professional_theme()
    _ensure_state()

    _render_header()
    if _interaction_locked():
        _set_site_section("Ask")
    section = _render_navigation()

    if not st.session_state.get("scope_acknowledged") and section == "Ask":
        st.info("Please review and acknowledge the platform scope before using the interactive KG tools.")
        if not _render_scope_gate(show_header=False):
            _render_footer()
            return

    if section == "Contact":
        _render_contact_section()
        _render_footer()
        return

    if section == "Introduction":
        _render_introduction_section()
        _render_footer()
        return

    with st.spinner("Loading MsAb-PatKG graph services..."):
        cfg, clarifier, flow_service = build_services()
    st.session_state._clarifier_ref = clarifier

    _render_backend_warning(clarifier)

    draft = st.session_state.interactive_draft

    if section == "Scenario":
        _render_scenario_section(clarifier, flow_service)
    elif section == "Ask":
        _render_sidebar(draft)
        _render_ask_section(draft, clarifier, flow_service)
        _run_pending_execution(flow_service)

        user_input = st.chat_input(
            "Ask a question or reply to the current confirmation prompt...",
            disabled=_interaction_locked(),
        )
        if user_input:
            current = st.session_state.interactive_draft
            if current is None or current.flow_state in {"completed", "error"}:
                if _is_out_of_scope_question(user_input):
                    draft = _build_out_of_scope_draft(user_input)
                else:
                    draft = clarifier.start(user_input)
                if _should_auto_run_draft(draft):
                    draft = _queue_execution(draft)
                draft = _maybe_auto_execute_draft(draft, clarifier, flow_service)
                st.session_state.interactive_draft = draft
            else:
                st.session_state.interactive_draft = _handle_followup_input(
                    st.session_state.interactive_draft,
                    clarifier,
                    flow_service,
                    user_input,
                )
            _set_site_section("Ask")
            st.rerun()

        draft = st.session_state.interactive_draft
        if draft is not None:
            with st.expander("Draft Summary", expanded=False):
                st.json(draft.to_summary())

        if st.button("Start Over", key="start_over_main", disabled=_interaction_locked()):
            st.session_state.interactive_draft = None
            st.session_state.execution_pending = False
            st.session_state.execution_running = False
            st.rerun()
    elif section == "Contact":
        _render_contact_section()

    _render_footer()


if __name__ == "__main__":
    main()
