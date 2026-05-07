# bsab_kg_qa_en/core/llm_provider.py
from __future__ import annotations
import json
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

    @property
    def model(self) -> str:
        return self._model

    def respond_json(
        self,
        *,
        system: str,
        user: str,
        reasoning_effort: str = "medium",
        verbosity: str = "low",
    ) -> dict:
        response = self._client.responses.create(
            model=self._model,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            reasoning={"effort": reasoning_effort},
            text={"verbosity": verbosity},
        )
        text = (response.output_text or "").strip()
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        if not text:
            raise ValueError("Model returned empty JSON response.")
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Model returned invalid JSON: {text[:300]}") from exc
