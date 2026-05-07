from __future__ import annotations

from bsab_kg_qa_en.interaction.state_models import QueryDraft


class InteractiveQaFlowService:
    """Thin execution bridge between the clarification draft and the Hybrid orchestrator."""

    def __init__(self, clarifier, orchestrator):
        self.clarifier = clarifier
        self.orchestrator = orchestrator

    def execute(self, draft: QueryDraft) -> QueryDraft:
        ok, message = self.clarifier.validate_ready_for_execution(draft)
        if not ok:
            draft.flow_state = "error"
            draft.error_message = message
            return draft

        draft.flow_state = "executing"
        draft = self.clarifier.finalize_execution_question(draft)
        resolved_params = {
            slot.slot_name: slot.selected_value
            for slot in draft.resolved_slots()
            if slot.selected_value not in (None, "")
        }
        try:
            if hasattr(self.orchestrator, "answer_resolved"):
                bundle = self.orchestrator.answer_resolved(
                    draft.raw_question,
                    draft.selected_intent or "",
                    resolved_params,
                )
            else:
                bundle = self.orchestrator.answer(draft.execution_question or draft.raw_question, mode="kg")
            draft.answer_bundle = {
                "mode": bundle.mode,
                "answer": bundle.answer,
                "debug": bundle.debug,
            }
            draft.flow_state = "completed"
            return draft
        except Exception as exc:  # noqa: BLE001
            draft.flow_state = "error"
            draft.error_message = f"{type(exc).__name__}: {exc}"
            return draft
