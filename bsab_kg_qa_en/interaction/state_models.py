from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


FlowState = Literal[
    "idle",
    "analyzing",
    "clarifying_entities",
    "clarifying_intent",
    "draft_ready",
    "awaiting_confirmation",
    "executing",
    "completed",
    "error",
]

SlotStatus = Literal["unresolved", "resolved", "manual_input_needed", "skipped"]


@dataclass
class CandidateOption:
    value: str
    label: str
    category: str
    source: str
    score: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SlotState:
    slot_name: str
    category: str
    required: bool
    raw_text: str | None = None
    status: SlotStatus = "unresolved"
    candidates: list[CandidateOption] = field(default_factory=list)
    selected_value: str | None = None
    selected_label: str | None = None
    manual_input: str | None = None
    notes: str | None = None

    def is_ready(self) -> bool:
        return self.status == "resolved" or (not self.required and self.status == "skipped")


@dataclass
class IntentCandidate:
    name: str
    description: str
    confidence: float | None = None
    source: str = "heuristic"
    required_slots: list[str] = field(default_factory=list)


@dataclass
class QueryDraft:
    raw_question: str
    flow_state: FlowState = "idle"
    chat_history: list[dict[str, str]] = field(default_factory=list)
    turn_count: int = 0
    next_question: str | None = None
    schema_rewrite: str | None = None
    fallback_ready: bool = False
    fallback_reason: str | None = None
    intent_candidates: list[IntentCandidate] = field(default_factory=list)
    selected_intent: str | None = None
    slots: dict[str, SlotState] = field(default_factory=dict)
    rewritten_question: str | None = None
    execution_question: str | None = None
    debug: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None
    answer_bundle: dict[str, Any] | None = None

    def unresolved_required_slots(self) -> list[SlotState]:
        return [slot for slot in self.slots.values() if slot.required and not slot.is_ready()]

    def resolved_slots(self) -> list[SlotState]:
        return [slot for slot in self.slots.values() if slot.status == "resolved"]

    def to_summary(self) -> dict[str, Any]:
        return {
            "raw_question": self.raw_question,
            "flow_state": self.flow_state,
            "turn_count": self.turn_count,
            "next_question": self.next_question,
            "schema_rewrite": self.schema_rewrite,
            "fallback_ready": self.fallback_ready,
            "fallback_reason": self.fallback_reason,
            "chat_history": self.chat_history,
            "selected_intent": self.selected_intent,
            "rewritten_question": self.rewritten_question,
            "execution_question": self.execution_question,
            "slots": {
                name: {
                    "category": slot.category,
                    "required": slot.required,
                    "status": slot.status,
                    "raw_text": slot.raw_text,
                    "selected_value": slot.selected_value,
                    "candidate_count": len(slot.candidates),
                }
                for name, slot in self.slots.items()
            },
            "debug": self.debug,
            "error_message": self.error_message,
        }
