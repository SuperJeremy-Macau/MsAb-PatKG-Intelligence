from __future__ import annotations

from datetime import datetime
from difflib import get_close_matches
from typing import Any, Dict, List
import json
import re

from bsab_kg_qa_en.interaction.entity_lookup import EntityLookup
from bsab_kg_qa_en.ner import TargetPairResolver
from bsab_kg_qa_en.interaction.state_models import IntentCandidate, QueryDraft, SlotState


class QueryClarifier:
    """GPT-driven clarification layer with Neo4j-backed candidate confirmation."""

    MAX_CLARIFICATION_TURNS = 5
    AUTO_EXECUTE_INTENT_CONFIDENCE = 0.78

    SLOT_NAME_TO_CATEGORY: Dict[str, str] = {
        "assignee": "assignee",
        "origin": "origin",
        "cancer": "cancer",
        "target": "target",
        "target1": "target",
        "target2": "target",
        "tp_name": "target_pair",
        "functional_of_target": "functional_of_target",
        "pathway": "pathway",
        "technologyclass1": "technologyclass1",
        "year": "year",
        "start_year": "year",
        "end_year": "year",
    }

    def __init__(self, runner, registry, ner_service, llm=None):
        self.runner = runner
        self.registry = registry
        self.ner_service = ner_service
        self.llm = llm
        self.entity_lookup = EntityLookup(runner, llm=llm)
        self.tp_resolver = TargetPairResolver(runner)
        self.backend_warning = self.entity_lookup.catalog_error
        self._intent_catalog = self._build_intent_catalog()

    def start(self, raw_question: str) -> QueryDraft:
        draft = QueryDraft(
            raw_question=raw_question,
            flow_state="analyzing",
            chat_history=[{"role": "user", "content": raw_question}],
        )
        return self.analyze_question(draft)

    def handle_user_reply(self, draft: QueryDraft, user_input: str) -> QueryDraft:
        text = (user_input or "").strip()
        if not text:
            return draft
        draft.chat_history.append({"role": "user", "content": text})
        draft.turn_count += 1
        return self.analyze_question(draft)

    def analyze_question(self, draft: QueryDraft) -> QueryDraft:
        try:
            previous_confirmed = {
                slot.slot_name: slot.selected_value
                for slot in draft.resolved_slots()
                if slot.selected_value
            }
            previous_intent = draft.selected_intent
            llm_plan = self._llm_plan_question(draft)
            draft.debug["llm_plan"] = llm_plan
            draft.schema_rewrite = self._clean_hint_value(llm_plan.get("rewritten_query"))
            draft.intent_candidates = self.shortlist_intents(draft.raw_question, llm_plan=llm_plan)
            draft.slots = self._build_initial_slots(draft.raw_question, draft.intent_candidates, llm_plan=llm_plan)
            self._restore_confirmed_slots(draft, previous_confirmed)
            draft.selected_intent = previous_intent
            if draft.selected_intent:
                self._sync_slots_to_selected_intent(draft)
            else:
                self._auto_select_intent_if_confident(draft)
            self._auto_resolve_unique_slots_if_confident(draft)
            draft.debug["slot_hints"] = {
                name: {
                    "raw_text": slot.raw_text,
                    "candidate_count": len(slot.candidates),
                    "status": slot.status,
                }
                for name, slot in draft.slots.items()
            }
            self._advance_flow_state(draft)
            draft.next_question = self._compose_next_question(draft, llm_plan)
            self._sync_assistant_question_history(draft)
            return draft
        except Exception as exc:  # noqa: BLE001
            draft.flow_state = "error"
            draft.error_message = f"{type(exc).__name__}: {exc}"
            return draft

    def shortlist_intents(self, question: str, llm_plan: dict[str, Any] | None = None) -> list[IntentCandidate]:
        q_lower = (question or "").lower()
        plan_candidates = (llm_plan or {}).get("intent_candidates") or []
        plan_map = {}
        for item in plan_candidates:
            if not isinstance(item, dict):
                continue
            name = self._normalize_intent_name(item.get("name"))
            if not name:
                continue
            plan_map[name] = {
                "confidence": float(item.get("confidence") or 0.0),
                "reason": str(item.get("reason") or "").strip(),
            }

        slot_hints = self.extract_slot_hints(question, llm_plan=llm_plan)
        has_target_pair = bool(slot_hints.get("tp_name"))
        has_assignee = bool(slot_hints.get("assignee"))
        has_target = bool(slot_hints.get("target") or slot_hints.get("target1") or slot_hints.get("target2"))
        has_cancer = bool(slot_hints.get("cancer"))
        has_functional = bool(slot_hints.get("functional_of_target"))
        has_pathway = bool(slot_hints.get("pathway"))
        has_technology = bool(slot_hints.get("technologyclass1"))
        has_year = bool(slot_hints.get("year") or slot_hints.get("start_year") or slot_hints.get("end_year"))

        candidates: list[IntentCandidate] = []
        for idef in self.registry.list(only_show=False):
            score = 0.05
            required_slots = self._required_slots_for_intent(idef.params_schema)
            plan_info = plan_map.get(idef.name)
            if plan_info:
                score += 0.65 + min(plan_info["confidence"], 1.0) * 0.2
            if has_target_pair and "tp_name" in idef.params_schema:
                score += 0.18
            if has_assignee and "assignee" in idef.params_schema:
                score += 0.15
            if has_target and any(key in idef.params_schema for key in ("target", "target1", "target2")):
                score += 0.15
            if has_cancer and "cancer" in idef.params_schema:
                score += 0.18
            if has_functional and "functional_of_target" in idef.params_schema:
                score += 0.16
            if has_pathway and "pathway" in idef.params_schema:
                score += 0.16
            if has_technology and "technologyclass1" in idef.params_schema:
                score += 0.16
            if has_year and any(key in idef.params_schema for key in ("year", "start_year", "end_year")):
                score += 0.1
            if "highly expressed" in q_lower and "cancer" in idef.params_schema:
                score += 0.08
            if "breast cancer" in q_lower and "cancer" in idef.params_schema:
                score += 0.05
            if "published in" in q_lower and any(key in idef.params_schema for key in ("year", "start_year", "end_year")):
                score += 0.05

            description = idef.description
            if plan_info and plan_info["reason"]:
                description = f"{idef.description} LLM: {plan_info['reason']}"
            candidates.append(
                IntentCandidate(
                    name=idef.name,
                    description=description,
                    confidence=round(score, 4),
                    source="gpt5.4",
                    required_slots=required_slots,
                )
            )
        candidates.sort(key=lambda item: item.confidence or 0.0, reverse=True)
        return candidates[:5]

    def extract_slot_hints(self, question: str, llm_plan: dict[str, Any] | None = None) -> dict[str, str]:
        hints: dict[str, str] = {}
        q = question or ""
        llm_slots = self._extract_llm_slot_hints(llm_plan)
        hints.update(llm_slots)

        if "tp_name" not in hints:
            try:
                tp = self.tp_resolver.resolve(q)
            except Exception as exc:  # noqa: BLE001
                if self.backend_warning is None:
                    self.backend_warning = f"{type(exc).__name__}: {exc}"
                tp = None
            if tp and tp.tp_name:
                hints["tp_name"] = tp.tp_name
            if tp and tp.targets:
                hints["target1"] = tp.targets[0]
                hints["target2"] = tp.targets[1]

        if "year" not in hints and "start_year" not in hints and "end_year" not in hints:
            year, start_year, end_year = self._extract_time_hints(q)
            if year is not None:
                hints["year"] = str(year)
            if start_year is not None:
                hints["start_year"] = str(start_year)
            if end_year is not None:
                hints["end_year"] = str(end_year)

        if "target" in hints and "target1" not in hints:
            hints["target1"] = hints["target"]

        cleaned = {}
        for key, value in hints.items():
            text = self._clean_hint_value(value)
            if text:
                cleaned[key] = text
        return cleaned

    def resolve_slot_candidates(self, slot_name: str, raw_text: str | None, limit: int = 8) -> list[dict[str, Any]]:
        category = self.SLOT_NAME_TO_CATEGORY.get(slot_name)
        if category == "year":
            return [{"value": raw_text, "label": raw_text, "category": "year", "source": "regex"}] if raw_text else []
        if slot_name == "tp_name":
            return self._resolve_target_pair_candidates(raw_text, limit=limit)
        candidates = self.entity_lookup.search(category or slot_name, raw_text, limit=limit)
        return [
            {
                "value": candidate.value,
                "label": candidate.label,
                "category": candidate.category,
                "source": candidate.source,
                "score": candidate.score,
            }
            for candidate in candidates
        ]

    def apply_slot_selection(self, draft: QueryDraft, slot_name: str, selected_value: str) -> QueryDraft:
        slot = draft.slots[slot_name]
        slot.selected_value = selected_value
        slot.selected_label = selected_value
        slot.status = "resolved"
        draft.chat_history.append({"role": "assistant", "content": f"Confirmed {slot_name}: {selected_value}"})
        if slot_name == "tp_name":
            self._propagate_target_pair_to_targets(draft, selected_value)
        self._resolve_unique_candidate_slots(draft)
        if draft.selected_intent:
            self._sync_slots_to_selected_intent(draft)
        self._advance_flow_state(draft)
        draft.next_question = self._compose_next_question(draft, draft.debug.get("llm_plan", {}))
        self._sync_assistant_question_history(draft)
        return draft

    def apply_manual_slot_input(self, draft: QueryDraft, slot_name: str, manual_text: str) -> QueryDraft:
        slot = draft.slots[slot_name]
        slot.manual_input = manual_text
        slot.raw_text = manual_text
        slot.candidates = [self._candidate_from_dict(item) for item in self.resolve_slot_candidates(slot_name, manual_text)]
        slot.status = "unresolved" if slot.candidates else "manual_input_needed"
        if self._should_auto_resolve_slot(slot_name, slot.candidates):
            slot.selected_value = slot.candidates[0].value
            slot.selected_label = slot.candidates[0].label
            slot.status = "resolved"
            if slot_name == "tp_name":
                self._propagate_target_pair_to_targets(draft, slot.selected_value)
        self._resolve_unique_candidate_slots(draft)
        if draft.selected_intent:
            self._sync_slots_to_selected_intent(draft)
        self._advance_flow_state(draft)
        draft.next_question = self._compose_next_question(draft, draft.debug.get("llm_plan", {}))
        self._sync_assistant_question_history(draft)
        return draft

    def reject_slot_candidates(self, draft: QueryDraft, slot_name: str) -> QueryDraft:
        slot = draft.slots[slot_name]
        slot.status = "manual_input_needed"
        slot.notes = "User rejected current candidates."
        self._advance_flow_state(draft)
        return draft

    def apply_intent_selection(self, draft: QueryDraft, intent_name: str) -> QueryDraft:
        draft.selected_intent = intent_name
        draft.chat_history.append({"role": "assistant", "content": f"Confirmed intent: {intent_name}"})
        self._sync_slots_to_selected_intent(draft)
        self._resolve_unique_candidate_slots(draft)
        self._sync_slots_to_selected_intent(draft)
        self._advance_flow_state(draft)
        draft.next_question = self._compose_next_question(draft, draft.debug.get("llm_plan", {}))
        self._sync_assistant_question_history(draft)
        return draft

    def resolve_unique_candidate_slots(self, draft: QueryDraft) -> QueryDraft:
        changed = self._resolve_unique_candidate_slots(draft)
        if changed:
            if draft.selected_intent:
                self._sync_slots_to_selected_intent(draft)
            self._advance_flow_state(draft)
            draft.next_question = self._compose_next_question(draft, draft.debug.get("llm_plan", {}))
            self._sync_assistant_question_history(draft)
        return draft

    def build_rewritten_question(self, draft: QueryDraft) -> str:
        parts = [draft.schema_rewrite or draft.raw_question.strip()]
        if draft.selected_intent:
            parts.append(f"intent={draft.selected_intent}")

        resolved = {slot.slot_name: slot.selected_value for slot in draft.resolved_slots() if slot.selected_value}
        if resolved.get("tp_name"):
            parts.append(f"target pair {resolved['tp_name']}")
        else:
            target_bits = [resolved.get("target1"), resolved.get("target2"), resolved.get("target")]
            target_bits = [bit for bit in target_bits if bit]
            if target_bits:
                parts.append(f"target {' / '.join(dict.fromkeys(target_bits))}")
        if resolved.get("cancer"):
            parts.append(f"cancer {resolved['cancer']}")
        if resolved.get("assignee"):
            parts.append(f"assignee {resolved['assignee']}")
        if resolved.get("origin"):
            parts.append(f"origin {resolved['origin']}")
        if resolved.get("start_year") and resolved.get("end_year"):
            parts.append(f"between {resolved['start_year']} and {resolved['end_year']}")
        elif resolved.get("year"):
            parts.append(f"in {resolved['year']}")
        return " | ".join(parts)

    def finalize_execution_question(self, draft: QueryDraft) -> QueryDraft:
        draft.execution_question = draft.rewritten_question or self.build_rewritten_question(draft)
        draft.flow_state = "awaiting_confirmation"
        draft.next_question = None
        return draft

    def validate_ready_for_execution(self, draft: QueryDraft) -> tuple[bool, str | None]:
        if not draft.selected_intent:
            return False, "Intent is not confirmed."
        missing = [slot.slot_name for slot in draft.unresolved_required_slots()]
        if missing:
            return False, f"Required slots are unresolved: {', '.join(missing)}"
        unresolved_mentions = [
            slot.slot_name
            for slot in draft.slots.values()
            if (slot.raw_text or slot.manual_input) and slot.status not in {"resolved", "skipped"}
        ]
        if unresolved_mentions:
            return False, f"Recognized entity mentions are unresolved: {', '.join(unresolved_mentions)}"
        return True, None

    def maybe_prepare_fallback(self, draft: QueryDraft) -> QueryDraft:
        if draft.turn_count < self.MAX_CLARIFICATION_TURNS:
            draft.fallback_ready = False
            draft.fallback_reason = None
            return draft
        draft.fallback_ready = True
        draft.fallback_reason = (
            "Too many clarification turns. To save tokens, the system can use the current best rewritten query."
        )
        if not draft.selected_intent and draft.intent_candidates:
            draft.selected_intent = draft.intent_candidates[0].name
            self._sync_slots_to_selected_intent(draft)
        if not draft.rewritten_question:
            draft.rewritten_question = self.build_rewritten_question(draft)
        if not draft.next_question:
            draft.next_question = "I can now use the current best query draft and ask MsAb-PatKG directly."
        return draft

    def _build_initial_slots(
        self,
        question: str,
        intent_candidates: List[IntentCandidate],
        llm_plan: dict[str, Any] | None = None,
    ) -> dict[str, SlotState]:
        hints = self.extract_slot_hints(question, llm_plan=llm_plan)
        required = self._initial_required_slots(intent_candidates, hints)

        slots: dict[str, SlotState] = {}
        for slot_name in sorted(required):
            category = self.SLOT_NAME_TO_CATEGORY.get(slot_name, slot_name)
            raw_text = hints.get(slot_name) or hints.get(category)
            slot = SlotState(slot_name=slot_name, category=category, required=True, raw_text=raw_text)
            if raw_text:
                slot.candidates = [self._candidate_from_dict(item) for item in self.resolve_slot_candidates(slot_name, raw_text)]
                if self._should_auto_resolve_slot(slot_name, slot.candidates):
                    slot.selected_value = slot.candidates[0].value
                    slot.selected_label = slot.candidates[0].label
                    slot.status = "resolved"
                    if slot_name == "tp_name":
                        self._propagate_target_pair_to_targets_from_slot_values(slots, slot.selected_value)
            slots[slot_name] = slot
        self._add_hint_driven_optional_slots(slots, hints)
        return slots

    def _advance_flow_state(self, draft: QueryDraft) -> None:
        unresolved = draft.unresolved_required_slots()
        if unresolved:
            draft.flow_state = "clarifying_entities"
            self.maybe_prepare_fallback(draft)
            return
        if not draft.selected_intent:
            draft.flow_state = "clarifying_intent"
            self.maybe_prepare_fallback(draft)
            return
        draft.rewritten_question = self.build_rewritten_question(draft)
        draft.flow_state = "draft_ready"
        draft.fallback_ready = draft.turn_count >= self.MAX_CLARIFICATION_TURNS

    def _required_slots_for_intent(self, params_schema: Dict[str, Any]) -> list[str]:
        return [name for name in params_schema if name in self.SLOT_NAME_TO_CATEGORY]

    def _initial_required_slots(self, intent_candidates: List[IntentCandidate], hints: Dict[str, str]) -> set[str]:
        required = set()
        for candidate in intent_candidates[:2]:
            required.update(candidate.required_slots)

        for slot_name in (
            "tp_name",
            "target",
            "assignee",
            "origin",
            "cancer",
            "functional_of_target",
            "pathway",
            "technologyclass1",
        ):
            if hints.get(slot_name):
                required.add(slot_name)
        if hints.get("year"):
            required.add("year")
        if hints.get("start_year") or hints.get("end_year"):
            required.update({"start_year", "end_year"})
            required.discard("year")
        return required

    def _add_hint_driven_optional_slots(self, slots: dict[str, SlotState], hints: Dict[str, str]) -> None:
        for slot_name in (
            "tp_name",
            "target",
            "assignee",
            "origin",
            "cancer",
            "functional_of_target",
            "pathway",
            "technologyclass1",
            "year",
            "start_year",
            "end_year",
        ):
            if slot_name in slots:
                continue
            raw_text = hints.get(slot_name)
            if not raw_text:
                continue
            category = self.SLOT_NAME_TO_CATEGORY.get(slot_name, slot_name)
            slot = SlotState(slot_name=slot_name, category=category, required=False, raw_text=raw_text)
            slot.candidates = [self._candidate_from_dict(item) for item in self.resolve_slot_candidates(slot_name, raw_text)]
            if self._should_auto_resolve_slot(slot_name, slot.candidates):
                slot.selected_value = slot.candidates[0].value
                slot.selected_label = slot.candidates[0].label
                slot.status = "resolved"
            slots[slot_name] = slot

    def _sync_slots_to_selected_intent(self, draft: QueryDraft) -> None:
        idef = self.registry.get(draft.selected_intent or "")
        if not idef:
            return
        required_slots = set(self._required_slots_for_intent(idef.params_schema))
        for slot_name, slot in draft.slots.items():
            slot.required = slot_name in required_slots
        for slot_name in required_slots:
            if slot_name not in draft.slots:
                draft.slots[slot_name] = SlotState(
                    slot_name=slot_name,
                    category=self.SLOT_NAME_TO_CATEGORY.get(slot_name, slot_name),
                    required=True,
                )
        if "tp_name" in required_slots:
            tp_slot = draft.slots.get("tp_name")
            if tp_slot and tp_slot.selected_value:
                self._propagate_target_pair_to_targets(draft, tp_slot.selected_value)

    def _auto_select_intent_if_confident(self, draft: QueryDraft) -> None:
        if not draft.intent_candidates:
            return
        top = draft.intent_candidates[0]
        second = draft.intent_candidates[1] if len(draft.intent_candidates) > 1 else None
        if not top.required_slots and not draft.slots and (top.confidence or 0.0) >= 0.5:
            draft.selected_intent = top.name
            self._sync_slots_to_selected_intent(draft)
            return
        if (top.confidence or 0.0) >= 0.62 and (second is None or (top.confidence or 0.0) - (second.confidence or 0.0) >= 0.1):
            draft.selected_intent = top.name
            self._sync_slots_to_selected_intent(draft)

    def _auto_resolve_unique_slots_if_confident(self, draft: QueryDraft) -> None:
        if not self._has_high_confidence_top_intent(draft):
            return
        changed = self._resolve_unique_candidate_slots(draft)
        if changed and draft.selected_intent:
            self._sync_slots_to_selected_intent(draft)

    def _resolve_unique_candidate_slots(self, draft: QueryDraft) -> bool:
        changed = False
        for slot_name, slot in draft.slots.items():
            if slot.status == "resolved":
                continue
            if len(slot.candidates) != 1:
                continue
            candidate = slot.candidates[0]
            slot.selected_value = candidate.value
            slot.selected_label = candidate.label
            slot.status = "resolved"
            if slot_name == "tp_name":
                self._propagate_target_pair_to_targets(draft, candidate.value)
            changed = True
        return changed

    def _has_high_confidence_top_intent(self, draft: QueryDraft) -> bool:
        if not draft.intent_candidates or not draft.selected_intent:
            return False
        top = draft.intent_candidates[0]
        if top.name != draft.selected_intent:
            return False
        return (top.confidence or 0.0) >= self.AUTO_EXECUTE_INTENT_CONFIDENCE

    def should_auto_execute(self, draft: QueryDraft) -> bool:
        if draft.flow_state not in {"draft_ready", "awaiting_confirmation"}:
            return False
        if not self._has_high_confidence_top_intent(draft):
            return False
        for slot in draft.slots.values():
            if not (slot.raw_text or slot.selected_value or slot.manual_input):
                continue
            if slot.status != "resolved":
                return False
            candidate_values = {candidate.value for candidate in slot.candidates}
            if slot.candidates and len(slot.candidates) != 1 and slot.selected_value not in candidate_values:
                return False
        return True

    def _resolve_target_pair_candidates(self, raw_text: str | None, limit: int = 8) -> list[dict[str, Any]]:
        text = (raw_text or "").strip()
        if not text:
            return []

        out: list[dict[str, Any]] = []
        seen = set()

        try:
            resolution = self.tp_resolver.resolve(text)
        except Exception:
            resolution = None
        if resolution and resolution.tp_name:
            out.append(
                {
                    "value": resolution.tp_name,
                    "label": resolution.tp_name,
                    "category": "target_pair",
                    "source": resolution.matched_by,
                    "score": resolution.confidence,
                }
            )
            seen.add(resolution.tp_name)

        pair_query = resolution.raw if resolution and resolution.raw else text
        for candidate in self.entity_lookup.search("target_pair", pair_query, limit=limit):
            if candidate.value in seen:
                continue
            out.append(
                {
                    "value": candidate.value,
                    "label": candidate.label,
                    "category": candidate.category,
                    "source": candidate.source,
                    "score": candidate.score,
                }
            )
            seen.add(candidate.value)
        return out[:limit]

    def _propagate_target_pair_to_targets(self, draft: QueryDraft, tp_name: str) -> None:
        parts = [part.strip() for part in str(tp_name).split("/") if part.strip()]
        if len(parts) != 2:
            return
        for slot_name, value in (("target1", parts[0]), ("target2", parts[1])):
            slot = draft.slots.get(slot_name)
            if slot is None:
                slot = SlotState(slot_name=slot_name, category="target", required=False)
                draft.slots[slot_name] = slot
            slot.raw_text = value
            slot.selected_value = value
            slot.selected_label = value
            slot.status = "resolved"
            slot.candidates = [self._candidate_from_dict(item) for item in self.resolve_slot_candidates(slot_name, value)]

    def _propagate_target_pair_to_targets_from_slot_values(self, slots: dict[str, SlotState], tp_name: str) -> None:
        parts = [part.strip() for part in str(tp_name).split("/") if part.strip()]
        if len(parts) != 2:
            return
        for slot_name, value in (("target1", parts[0]), ("target2", parts[1])):
            if slot_name in slots:
                continue
            slots[slot_name] = SlotState(
                slot_name=slot_name,
                category="target",
                required=False,
                raw_text=value,
                status="resolved",
                selected_value=value,
                selected_label=value,
            )

    def _restore_confirmed_slots(self, draft: QueryDraft, confirmed: dict[str, str]) -> None:
        for slot_name, value in confirmed.items():
            slot = draft.slots.get(slot_name)
            if slot is None:
                slot = SlotState(
                    slot_name=slot_name,
                    category=self.SLOT_NAME_TO_CATEGORY.get(slot_name, slot_name),
                    required=False,
                )
                draft.slots[slot_name] = slot
            slot.raw_text = value
            slot.selected_value = value
            slot.selected_label = value
            slot.status = "resolved"
            if not slot.candidates:
                slot.candidates = [self._candidate_from_dict(item) for item in self.resolve_slot_candidates(slot_name, value)]
        tp_name = confirmed.get("tp_name")
        if tp_name:
            self._propagate_target_pair_to_targets(draft, tp_name)

    def _compose_next_question(self, draft: QueryDraft, llm_plan: dict[str, Any] | None) -> str | None:
        suggested = self._clean_hint_value((llm_plan or {}).get("assistant_question"))
        if draft.flow_state == "clarifying_entities":
            slot = draft.unresolved_required_slots()[0] if draft.unresolved_required_slots() else None
            if slot and suggested:
                return suggested
            if slot:
                label = slot.slot_name.replace("_", " ")
                if slot.candidates:
                    return f"I found a few similar {label} values in the database. Which one did you mean?"
                if slot.raw_text:
                    return f"I interpreted `{slot.raw_text}` as the {label}. Is that what you mean?"
                return f"I still need the {label} before I can finalize the query."
        if draft.flow_state == "clarifying_intent":
            return suggested or "I have enough entities. Which query type matches what you want to ask?"
        if draft.flow_state in {"draft_ready", "awaiting_confirmation"}:
            return "Please confirm the rewritten query so I can run it on MsAb-PatKG."
        return suggested

    def _sync_assistant_question_history(self, draft: QueryDraft) -> None:
        if not draft.next_question:
            return
        if draft.chat_history:
            last = draft.chat_history[-1]
            if last.get("role") == "assistant" and last.get("content") == draft.next_question:
                return
        draft.chat_history.append({"role": "assistant", "content": draft.next_question})

    def _llm_plan_question(self, draft: QueryDraft) -> dict[str, Any]:
        if self.llm is None:
            return {}

        intent_briefs = [
            {
                "name": item["name"],
                "required_slots": item["required_slots"],
                "description": item["description"],
            }
            for item in self._intent_catalog
        ]
        confirmed_slots = {
            slot.slot_name: slot.selected_value
            for slot in draft.resolved_slots()
            if slot.selected_value
        }
        system = (
            "You are a multi-turn clarification planner for a patent intelligence assistant. "
            "Your job is to infer the most likely intent, preserve prior user confirmations, and decide the next question. "
            "Treat the conversation history as authoritative context. Do not forget earlier user answers unless they are explicitly corrected. "
            "Return surface forms only for entity slot_hints. "
            "Do not normalize to database canonical values, abbreviations, or guessed entity names unless the user explicitly typed that exact value. "
            "Do not hallucinate database entities. If an entity is ambiguous, return the surface form from the conversation only. "
            "If the user mentions a company or organization name, place that surface form in slot_hints.assignee. "
            "Do not invent target or assignee entities that are not stated in the conversation. "
            "Return strict JSON only with this structure: "
            '{"intent_candidates":[{"name":"INTENT_NAME","confidence":0.0,"reason":"short reason"}],'
            '"slot_hints":{"assignee":"","origin":"","cancer":"","target":"","target1":"","target2":"","tp_name":"","functional_of_target":"","pathway":"","technologyclass1":"","year":"","start_year":"","end_year":""},'
            '"clarification_focus":["slot_name"],'
            '"rewritten_query":"",'
            '"assistant_question":""}.'
        )
        user = (
            f"Available intents:\n{json.dumps(intent_briefs, ensure_ascii=False)}\n\n"
            f"Original question:\n{draft.raw_question}\n\n"
            f"Conversation history:\n{json.dumps(draft.chat_history, ensure_ascii=False)}\n\n"
            f"Already confirmed slots:\n{json.dumps(confirmed_slots, ensure_ascii=False)}\n\n"
            "Prefer cancer, target pair, and year-range interpretations when the wording suggests highly expressed targets, disease context, or publication windows. "
            "Use slot_hints.functional_of_target for functional target labels like adaptive immune checkpoint or NK cell engagement. "
            "Use slot_hints.pathway for pathway mentions like NRF2 pathway or drug metabolism and excretion. "
            "Use slot_hints.technologyclass1 for technology class mentions like piggybacking or TME remodeling axes. "
            "If enough information is available, produce a natural rewritten query and an assistant question that asks for final confirmation. "
            "Example: if the user says 'breast cancer', keep slot_hints.cancer as 'breast cancer' rather than BRCA. "
            "Example: if the user says 'for company Akeso', keep slot_hints.assignee as 'Akeso' rather than converting it to a canonical company name yourself."
        )
        try:
            return self.llm.respond_json(system=system, user=user, reasoning_effort="medium", verbosity="low")
        except Exception as exc:  # noqa: BLE001
            if self.backend_warning is None:
                self.backend_warning = f"LLM planning unavailable: {type(exc).__name__}: {exc}"
            return {}

    def _extract_llm_slot_hints(self, llm_plan: dict[str, Any] | None) -> dict[str, str]:
        slot_hints = (llm_plan or {}).get("slot_hints")
        if not isinstance(slot_hints, dict):
            return {}
        out: dict[str, str] = {}
        for slot_name in self.SLOT_NAME_TO_CATEGORY:
            value = slot_hints.get(slot_name)
            text = self._clean_hint_value(value)
            if text:
                out[slot_name] = text
        return out

    def _build_intent_catalog(self) -> list[dict[str, Any]]:
        catalog = []
        for idef in self.registry.list(only_show=False):
            catalog.append(
                {
                    "name": idef.name,
                    "description": idef.description,
                    "required_slots": self._required_slots_for_intent(idef.params_schema),
                }
            )
        return catalog

    def _normalize_intent_name(self, value: Any) -> str | None:
        if not value:
            return None
        text = str(value).strip()
        if text in {item["name"] for item in self._intent_catalog}:
            return text
        upper = text.upper()
        names = [item["name"] for item in self._intent_catalog]
        if upper in names:
            return upper
        match = get_close_matches(upper, names, n=1, cutoff=0.75)
        return match[0] if match else None

    def _extract_time_hints(self, question: str) -> tuple[int | None, int | None, int | None]:
        q = (question or "").lower()
        years = re.findall(r"\b(?:19|20)\d{2}\b", question or "")

        m_rel_en = re.search(r"\b(?:last|past)\s+(\d{1,2})\s+years?\b", q)
        if m_rel_en:
            n = int(m_rel_en.group(1))
            if 1 <= n <= 20:
                end_year = datetime.now().year - 1
                start_year = end_year - n + 1
                return None, start_year, end_year

        if len(years) >= 2:
            y1, y2 = int(years[0]), int(years[1])
            return None, min(y1, y2), max(y1, y2)

        if len(years) == 1:
            y = int(years[0])
            if any(k in q for k in ["before", "prior to", "earlier than"]):
                return None, 1900, y - 1
            if any(k in q for k in ["after", "since", "later than"]):
                return None, y, datetime.now().year
            return y, None, None

        return None, None, None

    @staticmethod
    def _clean_hint_value(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        lowered = text.lower()
        banned = {
            "unknown", "none", "null", "n/a", "not sure", "unspecified",
            "i", "we", "you", "want", "know", "show", "tell", "about",
        }
        if lowered in banned:
            return None
        return text

    @staticmethod
    def _should_auto_resolve_slot(slot_name: str, candidates: list[Any]) -> bool:
        if len(candidates) != 1:
            return False
        return slot_name in {"year", "start_year", "end_year"}

    @staticmethod
    def _candidate_from_dict(item: dict[str, Any]):
        from bsab_kg_qa_en.interaction.state_models import CandidateOption

        return CandidateOption(
            value=str(item["value"]),
            label=str(item.get("label") or item["value"]),
            category=str(item.get("category") or ""),
            source=str(item.get("source") or "unknown"),
            score=item.get("score"),
        )
