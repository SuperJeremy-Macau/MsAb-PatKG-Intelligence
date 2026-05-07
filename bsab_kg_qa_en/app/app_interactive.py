import os
import sys
import re

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
    "Target Pairs": "MATCH (tp:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent) RETURN tp.name AS value, count(DISTINCT p) AS weight ORDER BY weight DESC, value LIMIT {limit}",
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


def _load_entity_guide(clarifier, limit: int = 6, selected_labels: list[str] | None = None):
    guide = {}
    labels = selected_labels or list(ENTITY_GUIDE_QUERIES.keys())
    for label in labels:
        cypher = ENTITY_GUIDE_QUERIES[label].format(limit=limit)
        try:
            rows = clarifier.runner.run(cypher, enforce_limit=False)
        except Exception:
            rows = []
        values = [str(row.get("value")).strip() for row in rows if row.get("value")]
        if values:
            guide[label] = values
    return guide


def _load_entity_guide_counts(clarifier, selected_labels: list[str] | None = None):
    counts = {}
    labels = selected_labels or list(ENTITY_GUIDE_COUNT_QUERIES.keys())
    for label in labels:
        cypher = ENTITY_GUIDE_COUNT_QUERIES[label]
        try:
            rows = clarifier.runner.run(cypher, enforce_limit=False)
        except Exception:
            rows = []
        if rows and rows[0].get("total") is not None:
            counts[label] = int(rows[0]["total"])
    return counts


def _start_example_query(clarifier, flow_service, question: str, intent_name: str | None = None, auto_run: bool = False):
    draft = clarifier.start(question)
    if intent_name and draft.selected_intent != intent_name:
        draft = clarifier.apply_intent_selection(draft, intent_name)
    ready, _ = clarifier.validate_ready_for_execution(draft)
    if auto_run and ready:
        return _queue_execution(draft)
    if auto_run:
        draft = _advance_toward_execution(draft, clarifier, flow_service, wants_to_run=True, allow_top_intent=True)
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
    if "interactive_draft" not in st.session_state:
        st.session_state.interactive_draft = None
    if "guide_entity_focus" not in st.session_state:
        st.session_state.guide_entity_focus = "Assignees"
    if "execution_pending" not in st.session_state:
        st.session_state.execution_pending = False
    if "execution_running" not in st.session_state:
        st.session_state.execution_running = False


def _slot_display_name(slot_name: str) -> str:
    return SLOT_LABEL.get(slot_name, slot_name.replace("_", " ").title())


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
        st.header("Query Status")
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
            st.write("**Real entity examples in this KG**")
            st.caption("Use the main-page entity guide to inspect up to 50 real names for one category at a time.")
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


def _render_chat_intro():
    with st.chat_message("assistant"):
        st.markdown(
            """
I will first rewrite your question into a database-friendly query draft, then help you confirm entities and intent.
            """
        )


def _render_persistent_guide(clarifier, flow_service):
    with st.expander("Guide: Quick Start And Real Entity Examples", expanded=False):
        _render_entity_examples_guide(clarifier)
        st.markdown("---")
        _render_scenario_quick_start(clarifier, flow_service)


def _render_scenario_quick_start(clarifier, flow_service):
    st.markdown("**Scenario Walkthroughs**")
    st.caption(
        "These are paper-style multi-hop questions chosen from the four main scenarios so users can quickly experience the KG workflow."
    )
    scenario_items = list(SCENARIO_EXAMPLES.items())
    for row_start in range(0, len(scenario_items), 2):
        cols = st.columns(2)
        for col_idx, (scenario_name, questions) in enumerate(scenario_items[row_start:row_start + 2]):
            with cols[col_idx]:
                with st.container(border=True):
                    st.markdown(f"**{scenario_name}**")
                    for q_idx, question in enumerate(questions):
                        if st.button(
                            f"Run Example {q_idx + 1}",
                            key=f"scenario_{row_start}_{col_idx}_{q_idx}",
                            use_container_width=True,
                            disabled=_interaction_locked(),
                        ):
                            st.session_state.interactive_draft = _start_example_query(
                                clarifier,
                                flow_service,
                                question,
                                auto_run=True,
                            )
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
                "If you are unsure about the exact database name, use the real-entity guide on the home screen first, then ask with one of those names."
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


def _render_intent_confirmation(draft: QueryDraft, clarifier):
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


def _render_answer(draft: QueryDraft):
    with st.chat_message("assistant"):
        st.markdown("**MsAb-PatKG Answer**")
        st.markdown(draft.answer_bundle.get("answer", ""))
        st.caption("Ask a new question below to start a fresh query.")
    with st.expander("Debug", expanded=False):
        st.json(draft.answer_bundle.get("debug", {}))


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
        _render_intent_confirmation(draft, clarifier)
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
    cfg, clarifier, flow_service = build_services()
    _ensure_state()
    st.session_state._clarifier_ref = clarifier

    st.set_page_config(page_title=cfg["app"]["title"], layout=cfg["app"].get("layout", "wide"))
    st.title(cfg["app"]["title"])
    st.caption("Conversational query confirmation for MsAb-PatKG")
    st.markdown("---")
    _render_backend_warning(clarifier)

    draft = st.session_state.interactive_draft
    _render_sidebar(draft)
    _render_progress_header(draft)
    _render_execution_notice()
    _render_persistent_guide(clarifier, flow_service)

    if draft is None:
        _render_chat_intro()
    else:
        render_chat_flow(draft, clarifier, flow_service)

    _run_pending_execution(flow_service)

    user_input = st.chat_input(
        "Ask a question or reply to the current confirmation prompt...",
        disabled=_interaction_locked(),
    )
    if user_input:
        current = st.session_state.interactive_draft
        if current is None or current.flow_state in {"completed", "error"}:
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


if __name__ == "__main__":
    main()
