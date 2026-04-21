from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from openai import OpenAI

from bsab_kg_qa_en.config.settings_loader import load_settings
from bsab_kg_qa_en.query_rewriting.prompt_templates import (
    QUERY_REWRITE_SYSTEM_PROMPT,
    build_query_rewrite_user_prompt,
)


DEFAULT_MODEL = "gpt-5.4"


@dataclass
class QueryRewriteResult:
    rewritten_question: str
    template_signature: Dict[str, Any]
    entity_slots: Dict[str, Any]
    clarification_needed: bool
    clarification_note: str
    raw_response: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rewritten_question": self.rewritten_question,
            "template_signature": self.template_signature,
            "entity_slots": self.entity_slots,
            "clarification_needed": self.clarification_needed,
            "clarification_note": self.clarification_note,
            "raw_response": self.raw_response,
        }


def _project_root_from_here() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_settings_path() -> Path:
    return _project_root_from_here() / "bsab_kg_qa_en" / "config" / "settings.yaml"


def _strip_fence(text: str) -> str:
    return re.sub(r"^```(?:json)?\s*|\s*```$", "", (text or "").strip(), flags=re.MULTILINE).strip()


def _coerce_result(obj: Dict[str, Any], raw_response: str) -> QueryRewriteResult:
    template_signature = obj.get("template_signature")
    if not isinstance(template_signature, dict):
        template_signature = {}

    entity_slots = obj.get("entity_slots")
    if not isinstance(entity_slots, dict):
        entity_slots = {}

    return QueryRewriteResult(
        rewritten_question=str(obj.get("rewritten_question") or "").strip(),
        template_signature=template_signature,
        entity_slots=entity_slots,
        clarification_needed=bool(obj.get("clarification_needed", False)),
        clarification_note=str(obj.get("clarification_note") or "").strip(),
        raw_response=raw_response,
    )


class QueryRewriter:
    def __init__(
        self,
        base_url: str,
        api_key_env: str = "OPENAI_API_KEY",
        model: str = DEFAULT_MODEL,
        temperature: float = 0.0,
        timeout_s: float = 180.0,
        max_retries: int = 3,
        retry_backoff_s: float = 2.0,
    ):
        api_key = os.getenv(api_key_env)
        if not api_key:
            raise EnvironmentError(f"Missing env var {api_key_env} for query rewriting.")
        self._client = OpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._temperature = temperature
        self._timeout_s = timeout_s
        self._max_retries = max_retries
        self._retry_backoff_s = retry_backoff_s

    @classmethod
    def from_settings(
        cls,
        settings_path: str | Path | None = None,
        model: str = DEFAULT_MODEL,
        temperature: float = 0.0,
        timeout_s: float = 180.0,
        max_retries: int = 3,
        retry_backoff_s: float = 2.0,
    ) -> "QueryRewriter":
        path = Path(settings_path) if settings_path else _default_settings_path()
        settings = load_settings(str(path))
        llm_cfg = settings.get("llm", {})
        base_url = str(llm_cfg.get("base_url") or "https://api.openai.com/v1")
        api_key_env = str(llm_cfg.get("api_key_env") or "OPENAI_API_KEY")
        return cls(
            base_url=base_url,
            api_key_env=api_key_env,
            model=model,
            temperature=temperature,
            timeout_s=timeout_s,
            max_retries=max_retries,
            retry_backoff_s=retry_backoff_s,
        )

    def rewrite(
        self,
        user_query: str,
        schema_notes: Optional[str] = None,
        extra_examples: Optional[Dict[str, Any]] = None,
    ) -> QueryRewriteResult:
        system_prompt = QUERY_REWRITE_SYSTEM_PROMPT
        user_prompt = build_query_rewrite_user_prompt(
            user_query=user_query,
            schema_notes=schema_notes,
            extra_examples=extra_examples,
        )

        last_exc: Optional[Exception] = None
        raw = ""
        for attempt in range(self._max_retries):
            try:
                resp = self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=self._temperature,
                    timeout=self._timeout_s,
                )
                raw = (resp.choices[0].message.content or "").strip()
                clean = _strip_fence(raw)
                obj = json.loads(clean)
                return _coerce_result(obj, raw_response=raw)
            except Exception as exc:
                last_exc = exc
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_backoff_s * (attempt + 1))
                    continue
                break

        if isinstance(last_exc, json.JSONDecodeError):
            raise ValueError(f"Query rewriter did not return valid JSON.\nRaw response:\n{raw}") from last_exc
        raise RuntimeError(f"Query rewrite failed after {self._max_retries} attempts: {type(last_exc).__name__}: {last_exc}") from last_exc


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Schema-aware query rewriter for NL2Cypher.")
    parser.add_argument("query", help="Raw user query to reformulate.")
    parser.add_argument("--settings", default=str(_default_settings_path()), help="Path to settings.yaml")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI model name, default: gpt-5.4")
    parser.add_argument("--temperature", type=float, default=0.0, help="Sampling temperature.")
    parser.add_argument("--schema-notes", default=None, help="Optional extra schema notes.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    rewriter = QueryRewriter.from_settings(
        settings_path=args.settings,
        model=args.model,
        temperature=args.temperature,
    )
    result = rewriter.rewrite(
        user_query=args.query,
        schema_notes=args.schema_notes,
    )
    if args.pretty:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result.to_dict(), ensure_ascii=False))


if __name__ == "__main__":
    main()
