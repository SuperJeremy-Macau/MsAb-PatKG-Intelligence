# bsab_kg_qa_en/app/app_demo.py
import os, sys

# Ensure project root is on sys.path for Streamlit execution
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st
import re

from bsab_kg_qa_en.config import load_settings
from bsab_kg_qa_en.kg import Neo4jRunner
from bsab_kg_qa_en.intents import IntentRegistry
from bsab_kg_qa_en.core import Orchestrator
from bsab_kg_qa_en.core.llm_provider import LLMProvider


SETTINGS_PATH = os.path.join("bsab_kg_qa_en", "config", "settings.yaml")


def extract_target_pairs(rows):
    target_pairs = []
    for row in rows or []:
        for key in ("target_pair", "tp_name", "name"):
            value = row.get(key)
            if value:
                target_pairs.append(str(value))
                break
    return list(dict.fromkeys(target_pairs))


def maybe_expand_with_context(question: str, target_pairs):
    if not target_pairs:
        return question
    triggers = [
        r"\babove\b",
        r"\bthese\b",
        r"\bthose\b",
        r"上述",
        r"以上",
    ]
    if not any(re.search(t, question, flags=re.IGNORECASE) for t in triggers):
        return question
    if len(target_pairs) == 1:
        return f"{question} (target pair: {target_pairs[0]})"
    return question


def build_orchestrator():
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

    orch = Orchestrator(
        runner=runner,
        registry=registry,
        llm=llm,
        props=props,
        enable_nl2cypher_fallback=bool(intent_cfg.get("enable_nl2cypher_fallback", True)),
        temperature_intent=float(llm_cfg.get("temperature_intent", 0.0)),
        temperature_answer=float(llm_cfg.get("temperature_answer", 0.2)),
        temperature_no_kg=float(llm_cfg.get("temperature_no_kg", 0.2)),
    )
    return cfg, orch, runner, registry


def main():
    cfg, orch, runner, registry = build_orchestrator()

    st.set_page_config(
        page_title=cfg["app"]["title"],
        page_icon="🧬",
        layout=cfg["app"].get("layout", "wide"),
    )

    st.title(cfg["app"]["title"])
    st.caption(cfg["app"]["caption"])
    st.markdown("---")

    with st.sidebar:
        st.header("📘 About")
        st.write(
            "Left: KG-enabled answer (Neo4j + Intent-Cypher + LLM). "
            "Right: non-KG answer (LLM-only general expert response)."
        )

        st.subheader("✅ Loaded intents")
        for i in registry.list(only_show=True):
            st.write(f"- **{i.name}**: {i.description}")

        st.subheader("💡 Example questions")
        for i in registry.list(only_show=True):
            for ex in i.ui.get("examples", []):
                st.write(f"- {ex}")

        st.info("Make sure OPENAI_API_KEY is set in your environment and Neo4j settings are correct in config/settings.yaml.")
        show_debug = st.checkbox("Show Debug (intent/params/cypher/rows)", value=True)

    if "history" not in st.session_state:
        st.session_state.history = []
    if "last_target_pairs" not in st.session_state:
        st.session_state.last_target_pairs = []

    for item in st.session_state.history:
        st.markdown(f"**Q:** {item['q']}")

    st.markdown("### 💬 Enter your question")
    user_input = st.chat_input("e.g., When was the first patent published for PD-1×VEGFA?")

    if user_input:
        expanded_input = maybe_expand_with_context(user_input, st.session_state.last_target_pairs)
        st.session_state.history.append({"q": user_input})

        col1, col2 = st.columns(2, gap="large")
        with col1:
            st.subheader("🧠 KG-enabled answer")
            with st.spinner("KG mode: intent → Neo4j → synthesis..."):
                try:
                    bundle = orch.answer(expanded_input, mode="kg")
                    st.markdown(bundle.answer)
                    if show_debug:
                        with st.expander("Debug (KG)", expanded=False):
                            st.json(bundle.debug)
                    graph_rows = bundle.debug.get("graph_results")
                    if graph_rows is None:
                        bundles = bundle.debug.get("bundles") or []
                        graph_rows = []
                        for b in bundles:
                            graph_rows.extend(b.get("graph_results") or [])
                    st.session_state.last_target_pairs = extract_target_pairs(graph_rows)
                except Exception as e:
                    st.error(f"KG mode error: {e}")

        with col2:
            st.subheader("💬 non-KG answer")
            with st.spinner("no-KG mode: general expert response..."):
                try:
                    bundle2 = orch.answer(user_input, mode="no_kg")
                    st.markdown(bundle2.answer)
                    if show_debug:
                        with st.expander("Debug (no-KG)", expanded=False):
                            st.json(bundle2.debug)
                except Exception as e:
                    st.error(f"no-KG mode error: {e}")


if __name__ == "__main__":
    main()
