from __future__ import annotations

from html import escape
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "figures"
EDGE = Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")


def _text(x: int, y: int, text: str, cls: str = "body") -> str:
    return f'<text x="{x}" y="{y}" class="{cls}">{escape(text)}</text>'


def _multiline_text(x: int, y: int, lines: list[str], cls: str = "body", line_gap: int = 24) -> str:
    parts = [f'<text x="{x}" y="{y}" class="{cls}">']
    for idx, line in enumerate(lines):
        dy = 0 if idx == 0 else line_gap
        parts.append(f'<tspan x="{x}" dy="{dy}">{escape(line)}</tspan>')
    parts.append("</text>")
    return "".join(parts)


def _box(x: int, y: int, w: int, h: int, fill: str, stroke: str, title: str, lines: list[str]) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill}" stroke="{stroke}" stroke-width="2"/>'
        + _text(x + 18, y + 34, title, "boxh")
        + _multiline_text(x + 18, y + 64, lines, "body")
    )


def _svg_shell(title: str, subtitle: str, body: str, width: int = 1800, height: int = 1380) -> str:
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <defs>
    <style>
      .title {{ font: 700 30px 'Segoe UI', Arial, sans-serif; fill: #15293f; }}
      .subtitle {{ font: 400 18px 'Segoe UI', Arial, sans-serif; fill: #4d647a; }}
      .section {{ font: 700 22px 'Segoe UI', Arial, sans-serif; fill: #17324d; }}
      .boxh {{ font: 700 17px 'Segoe UI', Arial, sans-serif; fill: #17324d; }}
      .body {{ font: 400 14px 'Segoe UI', Arial, sans-serif; fill: #24384f; }}
      .small {{ font: 400 13px 'Segoe UI', Arial, sans-serif; fill: #53687b; }}
      .badge {{ font: 700 15px 'Segoe UI', Arial, sans-serif; fill: #ffffff; }}
    </style>
    <marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L10,5 L0,10 z" fill="#4a6987"/>
    </marker>
  </defs>
  <rect x="0" y="0" width="{width}" height="{height}" fill="#f5f8fb"/>
  <rect x="24" y="24" width="{width-48}" height="{height-48}" rx="24" fill="#ffffff" stroke="#d7e3ee" stroke-width="2"/>
  <text x="64" y="84" class="title">{escape(title)}</text>
  <text x="64" y="116" class="subtitle">{escape(subtitle)}</text>
  {body}
</svg>"""


def figure_tissue_case() -> str:
    body = []
    body.append(_box(64, 150, 1670, 86, "#eef6ff", "#bfd6ec", "Question", [
        "For target-pair combinations involving the Tissue injury & regeneration category,",
        "which company was the earliest discloser?"
    ]))

    body.append('<rect x="64" y="270" width="810" height="1010" rx="22" fill="#fbfdff" stroke="#d7e3ee" stroke-width="2"/>')
    body.append('<rect x="96" y="296" width="250" height="44" rx="12" fill="#1f4e79"/>')
    body.append(_text(140, 325, "Path A: Hybrid-Intent", "badge"))

    body.append(_box(96, 370, 330, 108, "#fff5e9", "#e5c48b", "Step 1. Intent routing", [
        "The router tries to map the full question",
        "to one complete executable intent template."
    ]))
    body.append(_box(470, 370, 360, 108, "#eef8ef", "#b7d6bc", "Step 2. Chosen intent", [
        "FIRST_ASSIGNEE_BY_FUNCTION",
        "This already encodes constraint + operation + output."
    ]))
    body.append('<line x1="426" y1="424" x2="470" y2="424" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')

    body.append(_box(96, 526, 330, 120, "#edf5ff", "#b7d0ee", "Step 3. Slot filling", [
        "Extract slot: functional_of_target",
        "Value = Tissue injury & regeneration"
    ]))
    body.append(_box(470, 526, 360, 176, "#f7f9fc", "#cfd9e4", "Step 4. Execute preset Cypher", [
        "MATCH Functional_of_Target <- FUNCTIONED_AS - Target",
        "<- HAS_TARGET - TargetPair <- HAS_TARGET_PAIR - Patent",
        "- HAS_ASSIGNEE -> Assignee",
        "MATCH Patent - PUBLISHED_IN -> Year",
        "Aggregate by assignee, find min(year), rank earliest first"
    ]))
    body.append('<line x1="426" y1="586" x2="470" y2="586" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')

    body.append(_box(96, 748, 734, 132, "#eef8ef", "#b7d6bc", "Step 5. Neo4j result", [
        "assignee = MERRIMACK PHARMACEUTICALS INC",
        "first_year = 2023",
        "patent_count = 1"
    ]))
    body.append(_box(96, 918, 734, 124, "#eef6ff", "#bfd6ec", "Step 6. Answer synthesis", [
        "The answer generator summarizes the graph result:",
        "Earliest discloser = MERRIMACK PHARMACEUTICALS INC"
    ]))
    body.append(_text(96, 1086, "Hybrid conclusion: same answer as Query-Frame in this case, but it reached it by choosing a complete intent first.", "small"))

    body.append('<rect x="926" y="270" width="808" height="1010" rx="22" fill="#fbfdff" stroke="#d7e3ee" stroke-width="2"/>')
    body.append('<rect x="958" y="296" width="300" height="44" rx="12" fill="#1f4e79"/>')
    body.append(_text(1000, 325, "Path B: True Query-Frame Slot", "badge"))

    body.append(_box(958, 370, 330, 108, "#eef8ef", "#b7d6bc", "Step 1. Macro family selection", [
        "Select a high-level business structure family,",
        "not a full executable intent."
    ]))
    body.append(_box(1332, 370, 360, 108, "#f2f8ff", "#bdd5ef", "Step 2. Selected macro family", [
        "MF06_assignee_first_discloser_identification",
        "Assignee first-discloser identification"
    ]))
    body.append('<line x1="1288" y1="424" x2="1332" y2="424" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')

    body.append(_box(958, 526, 330, 174, "#fff5e9", "#e5c48b", "Step 3. 4-dimension structure classification", [
        "constraint = function",
        "operation = first_discloser",
        "output = assignee",
        "time = all_time"
    ]))
    body.append(_box(1332, 526, 360, 144, "#edf5ff", "#b7d0ee", "Step 4. Skeleton selection", [
        "SKELETON_FUNCTION__FIRST_DISCLOSER__ASSIGNEE__ALL_TIME",
        "The skeleton is the executable frame unit."
    ]))
    body.append('<line x1="1288" y1="600" x2="1332" y2="600" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')

    body.append(_box(958, 728, 330, 120, "#f7f9fc", "#cfd9e4", "Step 5. Slot filling", [
        "Extract slot: functional_of_target",
        "Value = Tissue injury & regeneration"
    ]))
    body.append(_box(1332, 728, 360, 152, "#f7f9fc", "#cfd9e4", "Step 6. Execute skeleton Cypher", [
        "Run the same functional first-discloser logic,",
        "but via an explicit structure -> skeleton pipeline."
    ]))
    body.append('<line x1="1288" y1="788" x2="1332" y2="788" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')

    body.append(_box(958, 918, 734, 124, "#eef8ef", "#b7d6bc", "Step 7. Neo4j result + answer", [
        "assignee = MERRIMACK PHARMACEUTICALS INC, first_year = 2023, patent_count = 1",
        "Same final answer, but generated through a structure-first decision path."
    ]))
    body.append(_text(958, 1086, "Query-Frame conclusion: same result, different reasoning path. This is a positive alignment case.", "small"))

    body.append(_box(64, 1298, 1670, 52, "#f7f9fc", "#d7e3ee", "Takeaway", [
        "Hybrid-Intent chooses a full intent template first; Query-Frame first decomposes the question into structure, then selects an executable skeleton."
    ]))
    return _svg_shell(
        "Case Study 1. Same Answer, Different Decision Path",
        "Hybrid-Intent vs True Query-Frame Slot on a functional-category first-discloser question",
        "".join(body),
    )


def figure_divergent_case() -> str:
    body = []
    body.append(_box(64, 150, 1670, 86, "#eef6ff", "#bfd6ec", "Question", [
        "For the Tumor-Intrinsic Control TechnologyClass1 category,",
        "rank the target pairs by patent count and return the top 10."
    ]))

    body.append('<rect x="64" y="270" width="810" height="1010" rx="22" fill="#fbfdff" stroke="#d7e3ee" stroke-width="2"/>')
    body.append('<rect x="96" y="296" width="250" height="44" rx="12" fill="#1f4e79"/>')
    body.append(_text(140, 325, "Path A: Hybrid-Intent", "badge"))
    body.append(_box(96, 370, 330, 108, "#eef8ef", "#b7d6bc", "Step 1. Question understanding", [
        "Hybrid interprets the phrase as a",
        "TechnologyClass1-constrained ranking request."
    ]))
    body.append(_box(470, 370, 360, 108, "#edf5ff", "#b7d0ee", "Step 2. Extracted parameter", [
        "techClass = Tumor-Intrinsic Control",
        "The route stays on the intended business axis."
    ]))
    body.append('<line x1="426" y1="424" x2="470" y2="424" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')
    body.append(_box(96, 526, 734, 176, "#f7f9fc", "#cfd9e4", "Step 3. Execute ranking logic", [
        "The route runs a ranking-style Cypher over TechnologyClass1-constrained target pairs",
        "and returns a non-empty top-10 list.",
        "Top rows in the answer: EGFR/c-Met (219), EGFR/HER3 (158), HER2/HER2 (155)"
    ]))
    body.append(_box(96, 748, 734, 140, "#eef8ef", "#b7d6bc", "Observed answer", [
        "Hybrid returns a populated ranking answer.",
        "It remains on the intended task: TechnologyClass1 + patent-count ranking."
    ]))
    body.append(_text(96, 936, "Hybrid conclusion: structurally aligned with the question; returns a meaningful top-10 ranking.", "small"))

    body.append('<rect x="926" y="270" width="808" height="1010" rx="22" fill="#fbfdff" stroke="#d7e3ee" stroke-width="2"/>')
    body.append('<rect x="958" y="296" width="300" height="44" rx="12" fill="#1f4e79"/>')
    body.append(_text(1000, 325, "Path B: True Query-Frame Slot", "badge"))
    body.append(_box(958, 370, 330, 120, "#fff5e9", "#e5c48b", "Step 1. Macro family selection", [
        "MF11_targetpair_combination_profiling",
        "The route already drifts away from ranking."
    ]))
    body.append(_box(1332, 370, 360, 156, "#fff5e9", "#e5c48b", "Step 2. Selected skeleton", [
        "SKELETON_PATHWAY__MEMBER_LOOKUP__OTHER__ALL_TIME",
        "This is the wrong executable structure:",
        "pathway member lookup, not TechnologyClass1 ranking."
    ]))
    body.append('<line x1="1288" y1="430" x2="1332" y2="430" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')
    body.append(_box(958, 574, 330, 156, "#fbeff1", "#e0b6be", "Step 3. Wrong slot binding", [
        "pathway = Ficolins bind to repetitive carbohydrate structures...",
        "start_year = 10",
        "end_year = 10"
    ]))
    body.append(_box(1332, 574, 360, 176, "#fbeff1", "#e0b6be", "Step 4. Execute wrong Cypher", [
        "MATCH Pathway ... WHERE y.year >= 10 AND y.year <= 10",
        "RETURN pathway, patent_count",
        "This query is unrelated to TechnologyClass1 ranking."
    ]))
    body.append('<line x1="1288" y1="652" x2="1332" y2="652" stroke="#4a6987" stroke-width="3.5" marker-end="url(#arrow)"/>')
    body.append(_box(958, 796, 734, 140, "#fbeff1", "#e0b6be", "Observed answer", [
        "Query-Frame returns an empty-result message.",
        "The failure is caused by structure misclassification, not by lack of a valid ranking skeleton in the system."
    ]))
    body.append(_text(958, 984, "Query-Frame conclusion: this is a failure case where the route chooses the wrong structure and therefore the wrong executable skeleton.", "small"))

    body.append(_box(64, 1298, 1670, 52, "#f7f9fc", "#d7e3ee", "Takeaway", [
        "This case makes the difference visible: structure-first routing is powerful only when the macro family and structure dimensions are classified correctly."
    ]))
    return _svg_shell(
        "Case Study 2. Different Structure, Different Answer",
        "A divergence case: TechnologyClass1 ranking question where the two routes follow different execution structures",
        "".join(body),
    )


def write_svg_and_pdf(stem: str, svg_text: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    svg_path = OUT / f"{stem}.svg"
    html_path = OUT / f"{stem}.html"
    pdf_path = OUT / f"{stem}.pdf"
    svg_path.write_text(svg_text, encoding="utf-8")
    html_path.write_text(
        "<!doctype html><html><head><meta charset='utf-8'><style>body{margin:0;background:#fff;}svg{width:100%;height:auto;display:block;}</style></head><body>"
        + svg_text
        + "</body></html>",
        encoding="utf-8",
    )
    if EDGE.exists():
        try:
            subprocess.run(
                [
                    str(EDGE),
                    "--headless",
                    f"--print-to-pdf={pdf_path}",
                    "--disable-gpu",
                    html_path.as_uri(),
                ],
                check=False,
                timeout=180,
            )
        except subprocess.TimeoutExpired:
            print(f"PDF render timed out for {stem}, SVG/HTML were still written.")


def main() -> None:
    write_svg_and_pdf("queryframe_case_tissue_first_discloser_en", figure_tissue_case())
    write_svg_and_pdf("queryframe_case_technologyclass_divergence_en", figure_divergent_case())
    print("Rendered case figures to", OUT)


if __name__ == "__main__":
    main()
