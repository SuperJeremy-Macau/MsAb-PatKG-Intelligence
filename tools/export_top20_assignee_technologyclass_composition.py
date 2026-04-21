from __future__ import annotations

import argparse
import csv
import html
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

from neo4j import GraphDatabase
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bsab_kg_qa_en.config import load_settings


TOP_N = 20
CANONICAL_LABELS = {
    "Piggbacking": "Piggybacking",
    "B-cell related functional combiantion": "B-cell related functional combination",
    "Tumor‑Microenvironment (TME) Remodeling Axes": "Tumor-Microenvironment (TME) Remodeling Axes",
    "Tumor–Microenvironment (TME) Remodeling Axes": "Tumor-Microenvironment (TME) Remodeling Axes",
    "T-cell Activation – Signal-2-Related Mechanisms": "T-cell Activation - Signal-2-Related Mechanisms",
    "T-cell Activation – Signal-3-Related Mechanisms": "T-cell Activation - Signal-3-Related Mechanisms",
}
TECH_CLASS_ORDER = [
    "Trans-Bridging Immune Engagers",
    "T-cell Activation - Signal-2-Related Mechanisms",
    "Tumor-Intrinsic Control",
    "Other functional combination",
    "Cytokine related functional combination",
    "Other Immune-Checkpoint-Related Mechanisms",
    "Angiogenesis related functional combination",
    "T-cell Activation - Signal-3-Related Mechanisms",
    "Piggybacking",
    "Tumor-Microenvironment (TME) Remodeling Axes",
    "B-cell related functional combination",
    "Mast cell related functional combination",
]
COLORS = {
    "Trans-Bridging Immune Engagers": "#1F4E79",
    "T-cell Activation - Signal-2-Related Mechanisms": "#4F81BD",
    "Tumor-Intrinsic Control": "#C55A11",
    "Other functional combination": "#9E480E",
    "Cytokine related functional combination": "#548235",
    "Other Immune-Checkpoint-Related Mechanisms": "#8064A2",
    "Angiogenesis related functional combination": "#2F75B5",
    "T-cell Activation - Signal-3-Related Mechanisms": "#BF9000",
    "Piggybacking": "#5B9BD5",
    "Tumor-Microenvironment (TME) Remodeling Axes": "#70AD47",
    "B-cell related functional combination": "#264478",
    "Mast cell related functional combination": "#9966CC",
}


def resolve_font_path(bold: bool = False) -> str | None:
    candidates = [
        Path("C:/Windows/Fonts/timesbd.ttf" if bold else "C:/Windows/Fonts/times.ttf"),
        Path("C:/Windows/Fonts/times.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return str(path)
    return None


def font(size: int, bold: bool = False):
    path = resolve_font_path(bold)
    if path:
        return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def normalize_tech_class(label: str) -> str:
    cleaned = str(label).strip().replace("\u2011", "-").replace("\u2013", "-")
    cleaned = " ".join(cleaned.split())
    return CANONICAL_LABELS.get(cleaned, cleaned)


def shorten_assignee(name: str) -> str:
    s = str(name).strip()
    replacements = {
        " PHARMACEUTICALS ": " Pharma ",
        " PHARMACEUTICAL ": " Pharma ",
        " BIOPHARMACEUTICALS ": " Biopharma ",
        " BIOPHARMA ": " Biopharma ",
        " HOLDING ": " ",
        " COMPANY ": " Co. ",
        " CO.": " Co.",
        " CORPORATION": " Corp.",
        " INCORPORATED": " Inc.",
        " INC.": " Inc.",
        " LIMITED": " Ltd.",
        " LTD.": " Ltd.",
        " PLC": " PLC",
        " SA": " SA",
    }
    padded = f" {s} "
    for old, new in replacements.items():
        padded = padded.replace(old, new)
    s = " ".join(padded.split()).strip()
    s = re.sub(r"\s*&\s*", " & ", s)
    return s


def fetch_data(settings_path: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    cfg = load_settings(settings_path)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    top_assignee_query = """
    MATCH (a:Assignee)<-[:HAS_ASSIGNEE]-(p:Patent)
    RETURN a.name AS assignee, count(DISTINCT p) AS patent_count
    ORDER BY patent_count DESC, assignee ASC
    LIMIT $top_n
    """

    all_assignee_summary_query = """
    MATCH (a:Assignee)<-[:HAS_ASSIGNEE]-(p:Patent)
    OPTIONAL MATCH (a)<-[:HAS_ASSIGNEE]-(:Patent)-[:HAS_TARGET_PAIR]->(tp:TargetPair)
    OPTIONAL MATCH (tp)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    RETURN
      a.name AS assignee,
      count(DISTINCT p) AS patent_count,
      count(DISTINCT tp) AS distinct_targetpair_count,
      count(DISTINCT CASE WHEN tc IS NOT NULL THEN tp END) AS classified_targetpair_count,
      count(DISTINCT tc) AS technology_class_count
    ORDER BY patent_count DESC, assignee ASC
    """

    composition_query = """
    MATCH (a:Assignee)<-[:HAS_ASSIGNEE]-(:Patent)-[:HAS_TARGET_PAIR]->(tp:TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    RETURN
      a.name AS assignee,
      tc.name AS technology_class,
      count(DISTINCT tp) AS targetpair_count
    ORDER BY assignee ASC, targetpair_count DESC, technology_class ASC
    """

    try:
        with driver.session(database=neo["database"]) as session:
            top_rows = [dict(r) for r in session.run(top_assignee_query, {"top_n": TOP_N})]
            summary_rows = [dict(r) for r in session.run(all_assignee_summary_query)]
            composition_rows = [dict(r) for r in session.run(composition_query)]
    finally:
        driver.close()

    top_assignees = [str(r["assignee"]) for r in top_rows]
    top_lookup = {str(r["assignee"]): idx for idx, r in enumerate(top_rows)}

    summary_data = []
    for row in summary_rows:
        assignee = str(row["assignee"])
        summary_data.append(
            {
                "assignee": assignee,
                "assignee_display": shorten_assignee(assignee),
                "patent_count": int(row["patent_count"]),
                "distinct_targetpair_count": int(row["distinct_targetpair_count"]),
                "classified_targetpair_count": int(row["classified_targetpair_count"]),
                "technology_class_count": int(row["technology_class_count"]),
                "is_top20": 1 if assignee in top_lookup else 0,
                "top20_rank_by_patents": (top_lookup[assignee] + 1) if assignee in top_lookup else "",
            }
        )

    full_comp = []
    tmp = defaultdict(int)
    for row in composition_rows:
        assignee = str(row["assignee"])
        tech = normalize_tech_class(str(row["technology_class"]))
        tmp[(assignee, tech)] += int(row["targetpair_count"])

    totals_by_assignee = defaultdict(int)
    for (assignee, _tech), count in tmp.items():
        totals_by_assignee[assignee] += count

    for (assignee, tech), count in sorted(tmp.items(), key=lambda item: (item[0][0], -item[1], item[0][1])):
        total = totals_by_assignee[assignee]
        full_comp.append(
            {
                "assignee": assignee,
                "assignee_display": shorten_assignee(assignee),
                "technology_class": tech,
                "targetpair_count": count,
                "share_pct_within_assignee": round(100.0 * count / total, 2) if total else 0.0,
                "is_top20": 1 if assignee in top_lookup else 0,
            }
        )

    return summary_data, full_comp


def build_top20_outputs(summary_rows: list[dict[str, object]], full_comp: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    top20_summary = [row for row in summary_rows if int(row["is_top20"]) == 1]
    top20_summary.sort(key=lambda row: int(row["top20_rank_by_patents"]))

    comp_lookup = defaultdict(dict)
    for row in full_comp:
        if int(row["is_top20"]) != 1:
            continue
        comp_lookup[str(row["assignee"])][str(row["technology_class"])] = row

    top20_comp = []
    for row in top20_summary:
        assignee = str(row["assignee"])
        for tech in TECH_CLASS_ORDER:
            existing = comp_lookup.get(assignee, {}).get(tech)
            if existing:
                top20_comp.append(existing)
            else:
                top20_comp.append(
                    {
                        "assignee": assignee,
                        "assignee_display": str(row["assignee_display"]),
                        "technology_class": tech,
                        "targetpair_count": 0,
                        "share_pct_within_assignee": 0.0,
                        "is_top20": 1,
                    }
                )
    return top20_summary, top20_comp


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.multiline_textbbox((0, 0), text, font=text_font, spacing=4)
    return box[2] - box[0], box[3] - box[1]


def wrap_label(draw: ImageDraw.ImageDraw, text: str, max_width: int, text_font) -> str:
    words = text.split(" ")
    lines = []
    current = ""
    for word in words:
        trial = word if not current else current + " " + word
        if measure(draw, trial, text_font)[0] <= max_width:
            current = trial
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return "\n".join(lines)


def create_png(top20_summary: list[dict[str, object]], top20_comp: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    shares = defaultdict(dict)
    for row in top20_comp:
        shares[str(row["assignee"])][str(row["technology_class"])] = float(row["share_pct_within_assignee"])

    width, height = 4800, 2800
    plot_left, plot_right = 1120, 3020
    plot_top, plot_bottom = 150, 2350
    step = (plot_bottom - plot_top) / len(top20_summary)
    bar_h = step * 0.62

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    axis_font = font(38, True)
    label_font = font(27, False)
    tick_font = font(24, False)
    legend_font = font(25, False)
    note_font = font(22, False)

    for pct in range(0, 101, 20):
        x = plot_left + (pct / 100.0) * (plot_right - plot_left)
        draw.line((x, plot_top, x, plot_bottom), fill="#E3E3E3", width=2)
        label = f"{pct}%"
        w, h = measure(draw, label, tick_font)
        draw.text((x - w / 2, plot_bottom + 18), label, font=tick_font, fill="#404040")

    draw.line((plot_left, plot_top, plot_left, plot_bottom), fill="#303030", width=4)
    draw.line((plot_left, plot_bottom, plot_right, plot_bottom), fill="#303030", width=4)

    for idx, row in enumerate(top20_summary):
        assignee = str(row["assignee"])
        y = plot_top + idx * step + step / 2
        y0, y1 = y - bar_h / 2, y + bar_h / 2
        x_cursor = plot_left
        for tech in TECH_CLASS_ORDER:
            share = shares[assignee].get(tech, 0.0)
            if share <= 0:
                continue
            w = (share / 100.0) * (plot_right - plot_left)
            draw.rectangle((x_cursor, y0, x_cursor + w, y1), fill=COLORS[tech], outline="white", width=1)
            x_cursor += w

        label = f"{int(row['top20_rank_by_patents'])}. {str(row['assignee_display'])}"
        label = wrap_label(draw, label, 1040, label_font)
        box = draw.multiline_textbbox((0, 0), label, font=label_font, spacing=3)
        draw.multiline_text((plot_left - 30 - (box[2] - box[0]), y - (box[3] - box[1]) / 2), label, font=label_font, fill="#202020", spacing=3)
        meta_text = f"{int(row['patent_count']):,} patents | {int(row['classified_targetpair_count']):,} classified target pairs"
        draw.text((plot_right + 18, y - measure(draw, meta_text, tick_font)[1] / 2), meta_text, font=tick_font, fill="#404040")

    draw.text((plot_left + 700, plot_bottom + 88), "Technology-class composition of distinct target pairs", font=axis_font, fill="#202020")

    legend_x = 3660
    legend_y = 220
    for idx, tech in enumerate(TECH_CLASS_ORDER):
        y = legend_y + idx * 92
        draw.rounded_rectangle((legend_x, y, legend_x + 34, y + 34), radius=5, fill=COLORS[tech])
        legend_label = tech.replace(" - ", " -\n") if len(tech) > 34 else tech
        draw.multiline_text((legend_x + 54, y - 1), legend_label, font=legend_font, fill="#202020", spacing=3)

    note = (
        "Top 20 assignees ranked by distinct patent count. Each bar is normalized to 100% within assignee and "
        "shows the share of classified distinct target pairs by TechnologyClass1."
    )
    draw.multiline_text((70, height - 70), note, font=note_font, fill="#666666", spacing=4)
    image.save(output_path, dpi=(600, 600))


def create_svg(top20_summary: list[dict[str, object]], top20_comp: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    shares = defaultdict(dict)
    for row in top20_comp:
        shares[str(row["assignee"])][str(row["technology_class"])] = float(row["share_pct_within_assignee"])

    width, height = 4800, 2800
    plot_left, plot_right = 1120, 3020
    plot_top, plot_bottom = 150, 2350
    step = (plot_bottom - plot_top) / len(top20_summary)
    bar_h = step * 0.62

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]

    for pct in range(0, 101, 20):
        x = plot_left + (pct / 100.0) * (plot_right - plot_left)
        parts.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_bottom}" stroke="#E3E3E3" stroke-width="2"/>')
        parts.append(f'<text x="{x:.1f}" y="{plot_bottom + 46}" font-family="Times New Roman" font-size="24" fill="#404040" text-anchor="middle">{pct}%</text>')

    parts.extend(
        [
            f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
            f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
        ]
    )

    for idx, row in enumerate(top20_summary):
        assignee = str(row["assignee"])
        y = plot_top + idx * step + step / 2
        y0 = y - bar_h / 2
        x_cursor = plot_left
        for tech in TECH_CLASS_ORDER:
            share = shares[assignee].get(tech, 0.0)
            if share <= 0:
                continue
            w = (share / 100.0) * (plot_right - plot_left)
            parts.append(f'<rect x="{x_cursor:.1f}" y="{y0:.1f}" width="{w:.1f}" height="{bar_h:.1f}" fill="{COLORS[tech]}" stroke="white" stroke-width="1"/>')
            x_cursor += w
        assignee_label = f"{int(row['top20_rank_by_patents'])}. {str(row['assignee_display'])}"
        meta_label = f"{int(row['patent_count']):,} patents | {int(row['classified_targetpair_count']):,} classified target pairs"
        parts.append(f'<text x="{plot_left - 30}" y="{y + 8:.1f}" font-family="Times New Roman" font-size="27" fill="#202020" text-anchor="end">{html.escape(assignee_label)}</text>')
        parts.append(f'<text x="{plot_right + 18:.1f}" y="{y + 8:.1f}" font-family="Times New Roman" font-size="24" fill="#404040">{html.escape(meta_label)}</text>')

    parts.append(f'<text x="{plot_left + 700}" y="{plot_bottom + 92}" font-family="Times New Roman" font-size="38" font-weight="bold" fill="#202020">Technology-class composition of distinct target pairs</text>')

    legend_x = 3660
    legend_y = 220
    for idx, tech in enumerate(TECH_CLASS_ORDER):
        y = legend_y + idx * 92
        parts.append(f'<rect x="{legend_x}" y="{y}" width="34" height="34" rx="5" ry="5" fill="{COLORS[tech]}"/>')
        parts.append(f'<text x="{legend_x + 54}" y="{y + 28}" font-family="Times New Roman" font-size="25" fill="#202020">{html.escape(tech)}</text>')

    note = (
        "Top 20 assignees ranked by distinct patent count. Each bar is normalized to 100% within assignee and "
        "shows the share of classified distinct target pairs by TechnologyClass1."
    )
    parts.append(f'<text x="70" y="{height - 38}" font-family="Times New Roman" font-size="22" fill="#666666">{html.escape(note)}</text>')
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Top 20 assignee table and technology-class composition chart.")
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--summary-full-csv", default="docs/bsab_patent_landscape/data/assignee_summary_full.csv")
    parser.add_argument("--composition-full-csv", default="docs/bsab_patent_landscape/data/assignee_technologyclass_targetpair_composition_full.csv")
    parser.add_argument("--top20-summary-csv", default="docs/bsab_patent_landscape/data/top20_assignee_table.csv")
    parser.add_argument("--top20-composition-csv", default="docs/bsab_patent_landscape/data/top20_assignee_technologyclass_targetpair_composition.csv")
    parser.add_argument("--png-output", default="docs/bsab_patent_landscape/figures/top20_assignee_technologyclass_composition.png")
    parser.add_argument("--svg-output", default="docs/bsab_patent_landscape/figures/top20_assignee_technologyclass_composition.svg")
    args = parser.parse_args()

    summary_rows, full_comp = fetch_data(args.settings)
    top20_summary, top20_comp = build_top20_outputs(summary_rows, full_comp)

    write_csv(ROOT / args.summary_full_csv, summary_rows)
    write_csv(ROOT / args.composition_full_csv, full_comp)
    write_csv(ROOT / args.top20_summary_csv, top20_summary)
    write_csv(ROOT / args.top20_composition_csv, top20_comp)

    create_png(top20_summary, top20_comp, ROOT / args.png_output)
    create_svg(top20_summary, top20_comp, ROOT / args.svg_output)

    print(f"Full assignee summary CSV exported to: {ROOT / args.summary_full_csv}")
    print(f"Full assignee composition CSV exported to: {ROOT / args.composition_full_csv}")
    print(f"Top20 summary CSV exported to: {ROOT / args.top20_summary_csv}")
    print(f"Top20 composition CSV exported to: {ROOT / args.top20_composition_csv}")
    print(f"PNG exported to: {ROOT / args.png_output}")
    print(f"SVG exported to: {ROOT / args.svg_output}")


if __name__ == "__main__":
    main()
