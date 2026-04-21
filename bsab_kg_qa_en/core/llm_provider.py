# bsab_kg_qa_en/core/llm_provider.py
from __future__ import annotations
import os
from openai import OpenAI


class LLMProvider:
    def __init__(self, base_url: str, api_key_env: str, model: str):
        # Unified API credential for all LLM calls.
        env_name = "OPENAI_API_KEY"
        api_key = os.getenv(env_name)
        if not api_key:
            raise EnvironmentError(f"Missing env var {env_name} for LLM API key.")
        self._client = OpenAI(api_key=api_key, base_url=base_url)
        self._model = model

    def chat(self, system: str, user: str, temperature: float = 0.0) -> str:
        resp = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=temperature,
        )
        return (resp.choices[0].message.content or "").strip()
