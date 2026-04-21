from __future__ import annotations

import argparse
import csv
import html
import math
import sys
from pathlib import Path

from neo4j import GraphDatabase
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bsab_kg_qa_en.config import load_settings


CANONICAL_LABELS = {
    "Piggbacking": "Piggybacking",
    "B-cell related functional combiantion": "B-cell related functional combination",
    "Tumor‑Microenvironment (TME) Remodeling Axes": "Tumor-Microenvironment (TME) Remodeling Axes",
    "Tumor–Microenvironment (TME) Remodeling Axes": "Tumor-Microenvironment (TME) Remodeling Axes",
    "T-cell Activation – Signal-2-Related Mechanisms": "T-cell Activation - Signal-2-Related Mechanisms",
    "T-cell Activation – Signal-3-Related Mechanisms": "T-cell Activation - Signal-3-Related Mechanisms",
}

EXCLUDED_CLASSES = {"Extending PK/PD"}

PALETTE = {
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


def normalize_label(label: str) -> str:
    cleaned = str(label).strip().replace("\u2011", "-").replace("\u2013", "-")
    cleaned = " ".join(cleaned.split())
    return CANONICAL_LABELS.get(cleaned, cleaned)


def fetch_rows(settings_path: str, start_year: int) -> tuple[list[dict[str, int | str]], dict[str, int]]:
    cfg = load_settings(settings_path)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    query = """
    MATCH (p:Patent)-[:PUBLISHED_IN]->(y:Year)
    MATCH (p)-[:HAS_TARGET_PAIR]->(:TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    WHERE y.year >= $start_year
    RETURN y.year AS year, tc.name AS technology_class, count(DISTINCT p) AS patent_count
    ORDER BY year ASC, patent_count DESC, technology_class ASC
    """

    coverage_query = """
    MATCH (p:Patent)-[:PUBLISHED_IN]->(y:Year)
    WHERE y.year >= $start_year
    OPTIONAL MATCH (p)-[:HAS_TARGET_PAIR]->(:TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    RETURN
      y.year AS year,
      count(DISTINCT p) AS total_patents,
      count(DISTINCT CASE WHEN tc IS NOT NULL THEN p END) AS classified_patents
    ORDER BY year ASC
    """

    try:
        with driver.session(database=neo["database"]) as session:
            raw = [dict(r) for r in session.run(query, {"start_year": start_year})]
            coverage_rows = [dict(r) for r in session.run(coverage_query, {"start_year": start_year})]
    finally:
        driver.close()

    merged: dict[tuple[int, str], int] = {}
    for row in raw:
        year = int(row["year"])
        label = normalize_label(str(row["technology_class"]))
        if label in EXCLUDED_CLASSES:
            continue
        key = (year, label)
        merged[key] = merged.get(key, 0) + int(row["patent_count"])

    rows = [
        {"year": year, "technology_class": label, "patent_count": count}
        for (year, label), count in sorted(merged.items(), key=lambda item: (item[0][0], item[0][1]))
    ]
    coverage = {int(r["year"]): int(r["classified_patents"]) for r in coverage_rows}
    total_patents = {int(r["year"]): int(r["total_patents"]) for r in coverage_rows}
    meta = {
        "start_year": start_year,
        "end_year": max(coverage) if coverage else start_year,
        "classified_patents_total": sum(coverage.values()),
        "total_patents_total": sum(total_patents.values()),
    }
    return rows, meta


def build_period_rows(
    yearly_rows: list[dict[str, int | str]],
    start_year: int,
    end_year: int,
    span: int,
) -> tuple[list[dict[str, int | float | str]], list[str]]:
    counts: dict[tuple[int, str], int] = {}
    class_totals: dict[str, int] = {}

    for row in yearly_rows:
        year = int(row["year"])
        period_start = start_year + ((year - start_year) // span) * span
        label = str(row["technology_class"])
        count = int(row["patent_count"])
        counts[(period_start, label)] = counts.get((period_start, label), 0) + count
        class_totals[label] = class_totals.get(label, 0) + count

    class_order = [label for label, _ in sorted(class_totals.items(), key=lambda item: (-item[1], item[0].lower()))]

    output: list[dict[str, int | float | str]] = []
    for period_start in range(start_year, end_year + 1, span):
        period_end = min(period_start + span - 1, end_year)
        period_label = str(period_start) if span == 1 else f"{period_start}-{period_end}"
        total_assignments = sum(counts.get((period_start, label), 0) for label in class_order)
        if total_assignments == 0:
            continue
        for label in class_order:
            patent_count = counts.get((period_start, label), 0)
            share_pct = round(100.0 * patent_count / total_assignments, 2)
            output.append(
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "period_label": period_label,
                    "technology_class": label,
                    "patent_count": patent_count,
                    "total_assignments": total_assignments,
                    "share_pct": share_pct,
                }
            )
    return output, class_order


def write_csv(rows: list[dict[str, int | float | str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=text_font)
    return box[2] - box[0], box[3] - box[1]


def draw_centered(draw: ImageDraw.ImageDraw, x: float, y: float, text: str, text_font, fill: str) -> None:
    w, h = measure(draw, text, text_font)
    draw.text((x - w / 2, y - h / 2), text, font=text_font, fill=fill)


def create_stacked_png(
    rows: list[dict[str, int | float | str]],
    class_order: list[str],
    output_path: Path,
    title: str,
    note: str,
    highlight_last: bool = True,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    periods = []
    for row in rows:
        label = str(row["period_label"])
        if label not in periods:
            periods.append(label)

    counts = {(str(r["period_label"]), str(r["technology_class"])): float(r["share_pct"]) for r in rows}

    width = 3400 if len(periods) > 10 else 3000
    height = 2200
    plot_left = 230
    plot_right = width - 1020
    plot_top = 210
    plot_bottom = 1750

    title_font = font(68, True)
    axis_font = font(40, True)
    tick_font = font(28, False)
    legend_font = font(27, False)
    note_font = font(24, False)

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    draw_centered(draw, width / 2, 70, title, title_font, "#202020")

    if highlight_last and periods:
        step = (plot_right - plot_left) / len(periods)
        x0 = plot_left + (len(periods) - 1) * step
        draw.rectangle((x0, plot_top, plot_right, plot_bottom), fill="#F3F3F3")

    for pct in range(0, 101, 20):
        y = plot_bottom - (pct / 100.0) * (plot_bottom - plot_top)
        draw.line((plot_left, y, plot_right, y), fill="#E0E0E0", width=2)
        label = f"{pct}%"
        w, h = measure(draw, label, tick_font)
        draw.text((plot_left - 28 - w, y - h / 2), label, font=tick_font, fill="#404040")

    draw.line((plot_left, plot_top, plot_left, plot_bottom), fill="#404040", width=4)
    draw.line((plot_left, plot_bottom, plot_right, plot_bottom), fill="#404040", width=4)

    step = (plot_right - plot_left) / len(periods)
    bar_width = step * 0.68
    for idx, period in enumerate(periods):
        x0 = plot_left + idx * step + (step - bar_width) / 2
        x1 = x0 + bar_width
        y_cursor = plot_bottom
        for label in reversed(class_order):
            share = counts.get((period, label), 0.0)
            if share <= 0:
                continue
            height_px = (share / 100.0) * (plot_bottom - plot_top)
            y0 = y_cursor - height_px
            draw.rectangle((x0, y0, x1, y_cursor), fill=PALETTE.get(label, "#A5A5A5"), outline="white", width=1)
            y_cursor = y0
        draw_centered(draw, x0 + bar_width / 2, plot_bottom + 48, period, tick_font, "#404040")

    draw_centered(draw, (plot_left + plot_right) / 2, plot_bottom + 118, "Publication year", axis_font, "#202020")
    draw.text((28, (plot_top + plot_bottom) / 2 - 45), "Share of patent publications", font=axis_font, fill="#202020")

    legend_x = plot_right + 70
    legend_y = plot_top + 20
    for label in class_order:
        draw.rounded_rectangle((legend_x, legend_y, legend_x + 30, legend_y + 30), radius=4, fill=PALETTE.get(label, "#A5A5A5"))
        draw.multiline_text((legend_x + 48, legend_y - 1), label.replace(" - ", " -\n"), font=legend_font, fill="#202020", spacing=3)
        legend_y += 112 if " - " in label or len(label) > 34 else 62

    draw.multiline_text((70, height - 78), note, font=note_font, fill="#666666", spacing=4)
    image.save(output_path, dpi=(600, 600))


def svg_text(x: float, y: float, text: str, size: int, fill: str, anchor: str = "start", weight: str = "normal") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Times New Roman" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{html.escape(text)}</text>'
    )


def create_stacked_svg(
    rows: list[dict[str, int | float | str]],
    class_order: list[str],
    output_path: Path,
    title: str,
    note: str,
    highlight_last: bool = True,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    periods = []
    for row in rows:
        label = str(row["period_label"])
        if label not in periods:
            periods.append(label)

    counts = {(str(r["period_label"]), str(r["technology_class"])): float(r["share_pct"]) for r in rows}

    width = 3400 if len(periods) > 10 else 3000
    height = 2200
    plot_left = 230
    plot_right = width - 1020
    plot_top = 210
    plot_bottom = 1750
    step = (plot_right - plot_left) / len(periods)
    bar_width = step * 0.68

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        svg_text(width / 2, 90, title, 56, "#202020", anchor="middle", weight="bold"),
    ]

    if highlight_last and periods:
        x0 = plot_left + (len(periods) - 1) * step
        parts.append(f'<rect x="{x0:.1f}" y="{plot_top}" width="{plot_right - x0:.1f}" height="{plot_bottom - plot_top}" fill="#F3F3F3"/>')

    for pct in range(0, 101, 20):
        y = plot_bottom - (pct / 100.0) * (plot_bottom - plot_top)
        parts.append(f'<line x1="{plot_left}" y1="{y:.1f}" x2="{plot_right}" y2="{y:.1f}" stroke="#E0E0E0" stroke-width="2"/>')
        parts.append(svg_text(plot_left - 32, y + 10, f"{pct}%", 28, "#404040", anchor="end"))

    parts.extend(
        [
            f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" stroke="#404040" stroke-width="4"/>',
            f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" stroke="#404040" stroke-width="4"/>',
        ]
    )

    for idx, period in enumerate(periods):
        x0 = plot_left + idx * step + (step - bar_width) / 2
        x1 = x0 + bar_width
        y_cursor = plot_bottom
        for label in reversed(class_order):
            share = counts.get((period, label), 0.0)
            if share <= 0:
                continue
            height_px = (share / 100.0) * (plot_bottom - plot_top)
            y0 = y_cursor - height_px
            parts.append(
                f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{bar_width:.1f}" height="{height_px:.1f}" fill="{PALETTE.get(label, "#A5A5A5")}" stroke="white" stroke-width="1"/>'
            )
            y_cursor = y0
        parts.append(svg_text(x0 + bar_width / 2, plot_bottom + 56, period, 28, "#404040", anchor="middle"))

    parts.append(svg_text((plot_left + plot_right) / 2, plot_bottom + 130, "Publication year", 38, "#202020", anchor="middle", weight="bold"))
    parts.append(svg_text(28, (plot_top + plot_bottom) / 2, "Share of patent publications", 38, "#202020", weight="bold"))

    legend_x = plot_right + 70
    legend_y = plot_top + 20
    for label in class_order:
        parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="30" height="30" rx="4" ry="4" fill="{PALETTE.get(label, "#A5A5A5")}"/>')
        lines = label.replace(" - ", " -\n").split("\n")
        for line_idx, line in enumerate(lines):
            parts.append(svg_text(legend_x + 48, legend_y + 24 + line_idx * 30, line, 27, "#202020"))
        legend_y += 112 if len(lines) > 1 or len(label) > 34 else 62

    parts.append(svg_text(70, height - 40, note, 24, "#666666"))
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export 2010+ technology-class share trends and generate yearly/biennial stacked charts."
    )
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument(
        "--yearly-csv-output",
        default="docs/bsab_patent_landscape/data/technology_class_share_yearly_2010plus.csv",
    )
    parser.add_argument(
        "--biennial-csv-output",
        default="docs/bsab_patent_landscape/data/technology_class_share_biennial_2010plus.csv",
    )
    parser.add_argument(
        "--yearly-png-output",
        default="docs/bsab_patent_landscape/figures/technology_class_share_yearly_2010plus.png",
    )
    parser.add_argument(
        "--yearly-svg-output",
        default="docs/bsab_patent_landscape/figures/technology_class_share_yearly_2010plus.svg",
    )
    parser.add_argument(
        "--biennial-png-output",
        default="docs/bsab_patent_landscape/figures/technology_class_share_biennial_2010plus.png",
    )
    parser.add_argument(
        "--biennial-svg-output",
        default="docs/bsab_patent_landscape/figures/technology_class_share_biennial_2010plus.svg",
    )
    args = parser.parse_args()

    yearly_raw_rows, meta = fetch_rows(args.settings, args.start_year)
    end_year = meta["end_year"]

    yearly_rows, class_order = build_period_rows(yearly_raw_rows, args.start_year, end_year, span=1)
    biennial_rows, _ = build_period_rows(yearly_raw_rows, args.start_year, end_year, span=2)

    yearly_csv_path = ROOT / args.yearly_csv_output
    biennial_csv_path = ROOT / args.biennial_csv_output
    yearly_png_path = ROOT / args.yearly_png_output
    yearly_svg_path = ROOT / args.yearly_svg_output
    biennial_png_path = ROOT / args.biennial_png_output
    biennial_svg_path = ROOT / args.biennial_svg_output

    write_csv(yearly_rows, yearly_csv_path)
    write_csv(biennial_rows, biennial_csv_path)

    note_yearly = (
        f"Normalized to 100% within each year from {args.start_year}; "
        "Extending PK/PD excluded; shares reflect class assignments among classified patents; "
        f"{end_year} is partial year."
    )
    note_biennial = (
        f"Normalized to 100% within each 2-year bin from {args.start_year}; "
        "Extending PK/PD excluded; shares reflect class assignments among classified patents; "
        f"{max(args.start_year, end_year - 1)}-{end_year} includes partial latest-year data."
    )

    create_stacked_png(
        yearly_rows,
        class_order,
        yearly_png_path,
        "Technology class composition over time (annual, 100% stacked)",
        note_yearly,
    )
    create_stacked_svg(
        yearly_rows,
        class_order,
        yearly_svg_path,
        "Technology class composition over time (annual, 100% stacked)",
        note_yearly,
    )
    create_stacked_png(
        biennial_rows,
        class_order,
        biennial_png_path,
        "Technology class composition over time (2-year bins, 100% stacked)",
        note_biennial,
    )
    create_stacked_svg(
        biennial_rows,
        class_order,
        biennial_svg_path,
        "Technology class composition over time (2-year bins, 100% stacked)",
        note_biennial,
    )

    print(f"Yearly CSV exported to: {yearly_csv_path}")
    print(f"Biennial CSV exported to: {biennial_csv_path}")
    print(f"Yearly PNG exported to: {yearly_png_path}")
    print(f"Yearly SVG exported to: {yearly_svg_path}")
    print(f"Biennial PNG exported to: {biennial_png_path}")
    print(f"Biennial SVG exported to: {biennial_svg_path}")


if __name__ == "__main__":
    main()
