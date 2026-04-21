# bsab_kg_qa_en/core/answer_synthesizer.py
from __future__ import annotations
from typing import Any, Dict, List
import json

from bsab_kg_qa_en.core.llm_provider import LLMProvider


class AnswerSynthesizer:
    def __init__(self, llm: LLMProvider, temperature_answer: float = 0.2, temperature_no_kg: float = 0.2):
        self.llm = llm
        self.temperature_answer = temperature_answer
        self.temperature_no_kg = temperature_no_kg

    def answer_with_graph(self, question: str, intent_name: str, graph_results: List[Dict[str, Any]]) -> str:
        system_prompt = """
You are an expert in bispecific antibody (BsAb) patent landscapes and target-pair strategies.

You will receive:
- The user's question
- The intent_name (the question type)
- graph_results: JSON list returned from Neo4j (knowledge graph)

Instructions:
1) Start with a one-sentence statement of what you are answering (based on intent_name).
2) Summarize key findings in a concise, structured way (bullets or Year: Count).
3) Do NOT introduce any facts not present in graph_results (no invented years, counts, assignees, or publication numbers).
4) If graph_results is empty, explicitly state that the KG currently has no relevant data.
"""
        user_prompt = f"""User question: {question}

intent_name: {intent_name}

graph_results:
{json.dumps(graph_results, ensure_ascii=False, indent=2)}

Provide the answer based strictly on graph_results:"""
        return self.llm.chat(system_prompt.strip(), user_prompt, temperature=self.temperature_answer)

    def answer_with_multi_graph(self, question: str, intent_bundles: List[Dict[str, Any]]) -> str:
        system_prompt = """
You are an expert in bispecific antibody (BsAb) patent landscapes and target-pair strategies.

You will receive:
- The user's question
- intent_bundles: a JSON list of intent-specific results. Each bundle contains:
  - intent name
  - sub-question
  - graph_results: JSON list returned from Neo4j (knowledge graph)

Instructions:
1) Start with a one-sentence statement summarizing the overall question.
2) For each intent bundle, summarize the distribution/result in a concise, structured way (bullets or Year: Count).
3) Then provide a combined synthesis that answers the original question, based ONLY on the bundled graph_results.
4) Do NOT introduce any facts not present in graph_results (no invented years, counts, assignees, or publication numbers).
5) If any bundle has empty graph_results, explicitly note that the KG has no relevant data for that intent.
"""
        user_prompt = f"""User question: {question}

intent_bundles:
{json.dumps(intent_bundles, ensure_ascii=False, indent=2)}

Provide the answer based strictly on intent_bundles:"""
        return self.llm.chat(system_prompt.strip(), user_prompt, temperature=self.temperature_answer)

    def answer_no_kg(self, question: str) -> str:
        system_prompt = """
You are an expert in bispecific antibody (BsAb) patents and R&D strategy.
You do NOT have access to any knowledge graph or external database in this mode.

Requirements:
1) Directly answer the user's question naturally, without meta-preface (do not start with statements like "non-KG answer", "without KG", or "I will try to answer").
2) Do NOT fabricate specific publication numbers, exact patent counts, or exact year-by-year statistics.
3) Provide actionable suggestions on what additional information would enable a precise KG-backed answer (e.g., target pair, assignee, time range).
"""
        user_prompt = f"User question: {question}\n\nRespond following the requirements:"
        return self.llm.chat(system_prompt.strip(), user_prompt, temperature=self.temperature_no_kg)
