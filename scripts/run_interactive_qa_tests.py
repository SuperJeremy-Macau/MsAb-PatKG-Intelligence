import json
import re
from pathlib import Path

from streamlit.testing.v1 import AppTest


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "bsab_kg_qa_en" / "app" / "app_interactive.py"


def new_app():
    at = AppTest.from_file(str(APP_PATH))
    at.run(timeout=120)
    return at


def click_button(at: AppTest, key: str):
    for button in at.button:
        if getattr(button, "key", None) == key:
            button.click()
            at.run(timeout=300)
            return
    raise RuntimeError(f"Button not found: {key}")


def submit_chat(at: AppTest, text: str, timeout: int = 300):
    at.chat_input[0].set_value(text)
    at.run(timeout=timeout)


def current_draft(at: AppTest):
    return at.session_state.filtered_state.get("interactive_draft")


def markdown_values(at: AppTest):
    return [item.value for item in at.markdown]


def caption_values(at: AppTest):
    return [item.value for item in at.caption]


def entity_examples_test():
    at = new_app()
    click_button(at, "guide_focus_Targets")
    md = markdown_values(at)
    captions = caption_values(at)
    heading = next((value for value in md if value.startswith("**Targets: showing up to 50 real names")), "")
    values_line = ""
    if heading:
        idx = md.index(heading)
        if idx + 1 < len(md):
            values_line = md[idx + 1]
    values = re.findall(r"`([^`]+)`", values_line)
    return {
        "flow_state": getattr(current_draft(at), "flow_state", None),
        "heading": heading,
        "displayed_count": len(values),
        "first_values": values[:10],
        "overflow_notice": any("More than 50 entities exist in this category." in value for value in captions),
        "passed": current_draft(at) is None and len(values) == 50,
    }


def scenario_test(button_key: str, scenario_name: str):
    at = new_app()
    click_button(at, button_key)
    draft = current_draft(at)
    debug = (draft.answer_bundle or {}).get("debug", {}) if draft else {}
    answer = (draft.answer_bundle or {}).get("answer", "") if draft else ""
    top_row = (debug.get("graph_results") or [{}])[0]
    return {
        "scenario": scenario_name,
        "button_key": button_key,
        "flow_state": getattr(draft, "flow_state", None),
        "intent": getattr(draft, "selected_intent", None),
        "mode": debug.get("mode"),
        "next_question": getattr(draft, "next_question", None),
        "top_row": top_row,
        "answer_snippet": answer[:320],
        "passed": getattr(draft, "flow_state", None) == "completed" and bool(answer),
    }


def auto_execute_test():
    at = new_app()
    submit_chat(at, "Which targets show high expression in breast cancer?")
    draft = current_draft(at)
    answer = (draft.answer_bundle or {}).get("answer", "")
    debug = (draft.answer_bundle or {}).get("debug", {})
    return {
        "flow_state": draft.flow_state,
        "turn_count": draft.turn_count,
        "intent": draft.selected_intent,
        "slots": {
            name: {
                "status": slot.status,
                "raw_text": slot.raw_text,
                "selected_value": slot.selected_value,
                "candidate_count": len(slot.candidates),
            }
            for name, slot in draft.slots.items()
        },
        "rows": debug.get("rows"),
        "answer_has_total": f"Total targets found: **{debug.get('rows')}**" in answer,
        "passed": draft.flow_state == "completed" and draft.turn_count == 0,
    }


def confirm_boundary_test():
    at = new_app()
    submit_chat(at, "How many patents did Merck file on PD-1?")
    draft1 = current_draft(at)
    initial_flow = draft1.flow_state
    initial_next_question = draft1.next_question
    before = {
        name: {
            "status": slot.status,
            "raw_text": slot.raw_text,
            "selected_value": slot.selected_value,
            "candidate_count": len(slot.candidates),
        }
        for name, slot in draft1.slots.items()
    }
    click_button(at, "use_candidate_assignee_0")
    draft2 = current_draft(at)
    after_pick = {
        name: {
            "status": slot.status,
            "raw_text": slot.raw_text,
            "selected_value": slot.selected_value,
            "candidate_count": len(slot.candidates),
        }
        for name, slot in draft2.slots.items()
    }
    if draft2.flow_state == "completed":
        draft3 = draft2
    else:
        submit_chat(at, "yes, run it")
        draft3 = current_draft(at)
    answer = (draft3.answer_bundle or {}).get("answer", "")
    debug = (draft3.answer_bundle or {}).get("debug", {})
    graph_results = debug.get("graph_results") or []
    patent_count = None
    if graph_results and "patent_count" in graph_results[0]:
        patent_count = graph_results[0]["patent_count"]
    extra_confirmation_issue = (
        draft2.flow_state == "clarifying_entities"
        and after_pick.get("target", {}).get("candidate_count") == 1
        and after_pick.get("target", {}).get("status") != "resolved"
    )
    return {
        "initial_flow": initial_flow,
        "initial_next_question": initial_next_question,
        "initial_slots": before,
        "after_pick_flow": draft2.flow_state,
        "after_pick_next_question": draft2.next_question,
        "after_pick_slots": after_pick,
        "final_flow": draft3.flow_state,
        "final_intent": draft3.selected_intent,
        "final_patent_count": patent_count,
        "answer_has_patent_count": patent_count is not None and f"**Patent count:** **{patent_count}**" in answer,
        "extra_confirmation_issue": extra_confirmation_issue,
        "passed": initial_flow == "clarifying_entities" and draft2.flow_state == "completed",
    }


def fidelity_test():
    at = new_app()
    click_button(at, "scenario_0_0_0")
    draft = current_draft(at)
    answer = (draft.answer_bundle or {}).get("answer", "")
    debug = (draft.answer_bundle or {}).get("debug", {})
    top_rows = debug.get("graph_results") or []
    first = top_rows[0] if top_rows else {}
    tenth = top_rows[9] if len(top_rows) >= 10 else {}
    checks = {
        "first_pair_in_answer": bool(first) and first.get("target_pair", "") in answer,
        "first_count_in_answer": bool(first) and str(first.get("patent_count")) in answer,
        "tenth_pair_in_answer": bool(tenth) and tenth.get("target_pair", "") in answer,
        "tenth_count_in_answer": bool(tenth) and str(tenth.get("patent_count")) in answer,
    }
    return {
        "intent": draft.selected_intent,
        "rows": debug.get("rows"),
        "top_row": first,
        "tenth_row": tenth,
        "checks": checks,
        "passed": all(checks.values()),
    }


def scenario_followup_test(button_key: str, followups: list[str]):
    at = new_app()
    click_button(at, button_key)
    states = []
    draft = current_draft(at)
    states.append(
        {
            "step": "after_click",
            "flow_state": getattr(draft, "flow_state", None),
            "intent": getattr(draft, "selected_intent", None),
            "next_question": getattr(draft, "next_question", None),
            "slots": {
                name: {
                    "status": slot.status,
                    "raw_text": slot.raw_text,
                    "selected_value": slot.selected_value,
                    "candidate_count": len(slot.candidates),
                }
                for name, slot in getattr(draft, "slots", {}).items()
            },
        }
    )
    for step_idx, followup in enumerate(followups, start=1):
        if getattr(draft, "flow_state", None) == "completed":
            break
        submit_chat(at, followup, timeout=420)
        draft = current_draft(at)
        states.append(
            {
                "step": f"after_followup_{step_idx}",
                "user_reply": followup,
                "flow_state": getattr(draft, "flow_state", None),
                "intent": getattr(draft, "selected_intent", None),
                "next_question": getattr(draft, "next_question", None),
                "slots": {
                    name: {
                        "status": slot.status,
                        "raw_text": slot.raw_text,
                        "selected_value": slot.selected_value,
                        "candidate_count": len(slot.candidates),
                    }
                    for name, slot in getattr(draft, "slots", {}).items()
                },
                "answer_snippet": ((draft.answer_bundle or {}).get("answer", ""))[:260] if getattr(draft, "answer_bundle", None) else "",
            }
        )
    return states


def main():
    report = {
        "entity_examples": entity_examples_test(),
        "scenarios": [
            scenario_test("scenario_0_0_0", "Target-combination inspiration"),
            scenario_test("scenario_0_1_0", "Patent coverage and competitive crowding"),
            scenario_test("scenario_2_0_0", "Company scouting for licensing/investment"),
            scenario_test("scenario_2_1_0", "Assignee-origin macro-competition"),
        ],
        "auto_execute": auto_execute_test(),
        "confirm_boundary": confirm_boundary_test(),
        "answer_fidelity": fidelity_test(),
        "scenario_followups": {
            "patent_coverage_yes_run": scenario_followup_test("scenario_0_1_0", ["yes, run it"]),
            "macro_competition_yes_yes_run": scenario_followup_test("scenario_2_1_0", ["yes", "yes, run it"]),
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
