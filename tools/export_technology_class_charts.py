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


BAR_W = 3200
BAR_H = 2300
PIE_W = 2600
PIE_H = 2200


CANONICAL_LABELS = {
    "Piggbacking": "Piggybacking",
    "B-cell related functional combiantion": "B-cell related functional combination",
    "Tumor‑Microenvironment (TME) Remodeling Axes": "Tumor-Microenvironment (TME) Remodeling Axes",
    "Tumor–Microenvironment (TME) Remodeling Axes": "Tumor-Microenvironment (TME) Remodeling Axes",
    "T-cell Activation – Signal-2-Related Mechanisms": "T-cell Activation - Signal-2-Related Mechanisms",
    "T-cell Activation – Signal-3-Related Mechanisms": "T-cell Activation - Signal-3-Related Mechanisms",
}


PALETTE = [
    "#1F4E79",
    "#4F81BD",
    "#C55A11",
    "#9E480E",
    "#548235",
    "#8064A2",
    "#2F75B5",
    "#BF9000",
    "#5B9BD5",
    "#70AD47",
    "#264478",
    "#9966CC",
    "#A5A5A5",
    "#7F6000",
]


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


def fetch_technology_class_rows(settings_path: str) -> tuple[list[dict[str, float | int | str]], dict[str, int]]:
    cfg = load_settings(settings_path)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    patent_class_query = """
    MATCH (p:Patent)-[:HAS_TARGET_PAIR]->(:TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    RETURN tc.name AS technology_class, count(DISTINCT p) AS patent_count
    ORDER BY patent_count DESC, technology_class ASC
    """

    coverage_query = """
    MATCH (p:Patent)
    OPTIONAL MATCH (p)-[:HAS_TARGET_PAIR]->(:TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    RETURN
      count(DISTINCT p) AS total_patents,
      count(DISTINCT CASE WHEN tc IS NOT NULL THEN p END) AS classified_patents
    """

    multi_class_query = """
    MATCH (p:Patent)-[:HAS_TARGET_PAIR]->(:TargetPair)-[:HAS_TECHNOLOGY_CLASS1]->(tc:TechnologyClass1)
    WITH p, count(DISTINCT tc) AS class_count
    RETURN class_count, count(*) AS patents
    ORDER BY class_count
    """

    try:
        with driver.session(database=neo["database"]) as session:
            raw_rows = [dict(r) for r in session.run(patent_class_query)]
            coverage = dict(session.run(coverage_query).single())
            multi_class_rows = [dict(r) for r in session.run(multi_class_query)]
    finally:
        driver.close()

    merged: dict[str, int] = {}
    for row in raw_rows:
        label = normalize_label(str(row["technology_class"]))
        merged[label] = merged.get(label, 0) + int(row["patent_count"])

    total_assignments = sum(merged.values())
    classified_patents = int(coverage["classified_patents"])
    total_patents = int(coverage["total_patents"])
    multi_class_patents = sum(int(r["patents"]) for r in multi_class_rows if int(r["class_count"]) > 1)

    rows: list[dict[str, float | int | str]] = []
    for label, patent_count in sorted(merged.items(), key=lambda item: (-item[1], item[0].lower())):
        rows.append(
            {
                "technology_class": label,
                "patent_count": patent_count,
                "assignment_share_pct": round(100.0 * patent_count / total_assignments, 2),
                "classified_patent_share_pct": round(100.0 * patent_count / classified_patents, 2),
            }
        )

    meta = {
        "total_patents": total_patents,
        "classified_patents": classified_patents,
        "total_assignments": total_assignments,
        "multi_class_patents": multi_class_patents,
    }
    return rows, meta


def write_csv(rows: list[dict[str, float | int | str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=text_font)
    return box[2] - box[0], box[3] - box[1]


def draw_text(draw: ImageDraw.ImageDraw, xy: tuple[float, float], text: str, text_font, fill: str, spacing: int = 6) -> None:
    draw.multiline_text(xy, text, font=text_font, fill=fill, spacing=spacing)


def wrap_label(draw: ImageDraw.ImageDraw, text: str, max_width: int, text_font) -> str:
    words = text.split(" ")
    lines: list[str] = []
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


def create_bar_chart_png(rows: list[dict[str, float | int | str]], meta: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    margin_left = 930
    margin_right = 240
    margin_top = 210
    margin_bottom = 170
    plot_left = margin_left
    plot_right = BAR_W - margin_right
    plot_top = margin_top
    plot_bottom = BAR_H - margin_bottom

    image = Image.new("RGB", (BAR_W, BAR_H), "white")
    draw = ImageDraw.Draw(image)

    title_font = font(66, True)
    axis_font = font(40, True)
    label_font = font(31, False)
    tick_font = font(28, False)
    value_font = font(28, False)
    note_font = font(24, False)

    max_value = max(int(r["patent_count"]) for r in rows)
    tick_step = 1000 if max_value > 4000 else 500
    axis_max = int(math.ceil(max_value / tick_step) * tick_step)

    draw_text(draw, (BAR_W / 2 - 850, 40), "Technology class distribution by patent publications", title_font, "#202020")

    for value in range(0, axis_max + tick_step, tick_step):
        x = plot_left + (value / axis_max) * (plot_right - plot_left)
        draw.line((x, plot_top, x, plot_bottom), fill="#E0E0E0", width=2)
        label = f"{value:,}"
        w, h = measure(draw, label, tick_font)
        draw.text((x - w / 2, plot_bottom + 18), label, font=tick_font, fill="#404040")

    draw.line((plot_left, plot_top, plot_left, plot_bottom), fill="#404040", width=4)
    draw.line((plot_left, plot_bottom, plot_right, plot_bottom), fill="#404040", width=4)

    bar_gap = (plot_bottom - plot_top) / len(rows)
    bar_height = bar_gap * 0.64

    for idx, row in enumerate(rows):
        y_center = plot_top + idx * bar_gap + bar_gap / 2
        y0 = y_center - bar_height / 2
        y1 = y_center + bar_height / 2
        value = int(row["patent_count"])
        x1 = plot_left + (value / axis_max) * (plot_right - plot_left)
        color = PALETTE[idx % len(PALETTE)]
        draw.rounded_rectangle((plot_left, y0, x1, y1), radius=10, fill=color)

        wrapped = wrap_label(draw, str(row["technology_class"]), margin_left - 80, label_font)
        tw, th = measure(draw, wrapped.split("\n")[0], label_font)
        box = draw.multiline_textbbox((0, 0), wrapped, font=label_font, spacing=4)
        draw.multiline_text((margin_left - 30 - (box[2] - box[0]), y_center - (box[3] - box[1]) / 2), wrapped, font=label_font, fill="#202020", spacing=4)

        share = float(row["assignment_share_pct"])
        value_text = f"{value:,} ({share:.1f}%)"
        draw.text((x1 + 16, y_center - measure(draw, value_text, value_font)[1] / 2), value_text, font=value_font, fill="#202020")

    draw_text(draw, (plot_left + 480, BAR_H - 90), "Patent publications", axis_font, "#202020")
    note = (
        f"Classified patents: {meta['classified_patents']:,} / {meta['total_patents']:,}; "
        f"multi-class patents: {meta['multi_class_patents']:,}. "
        "Percentages use class-assignment totals as denominator."
    )
    draw_text(draw, (80, BAR_H - 45), note, note_font, "#666666")

    image.save(output_path, dpi=(600, 600))


def pie_segments(rows: list[dict[str, float | int | str]]) -> list[tuple[str, int, float, str]]:
    total = sum(int(r["patent_count"]) for r in rows)
    segments: list[tuple[str, int, float, str]] = []
    for idx, row in enumerate(rows):
        value = int(row["patent_count"])
        pct = 100.0 * value / total
        segments.append((str(row["technology_class"]), value, pct, PALETTE[idx % len(PALETTE)]))
    return segments


def create_pie_chart_png(rows: list[dict[str, float | int | str]], meta: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    image = Image.new("RGB", (PIE_W, PIE_H), "white")
    draw = ImageDraw.Draw(image)

    title_font = font(62, True)
    legend_font = font(28, False)
    note_font = font(24, False)

    draw_text(draw, (PIE_W / 2 - 650, 40), "Technology class share of patent publications", title_font, "#202020")

    cx, cy = 820, 1110
    radius = 620
    bbox = (cx - radius, cy - radius, cx + radius, cy + radius)
    start_angle = -90.0

    segments = pie_segments(rows)
    for label, value, pct, color in segments:
        end_angle = start_angle + pct * 3.6
        draw.pieslice(bbox, start=start_angle, end=end_angle, fill=color, outline="white", width=4)
        if pct >= 4.0:
            mid = math.radians((start_angle + end_angle) / 2)
            tx = cx + math.cos(mid) * radius * 0.62
            ty = cy + math.sin(mid) * radius * 0.62
            pct_text = f"{pct:.1f}%"
            w, h = measure(draw, pct_text, legend_font)
            draw.text((tx - w / 2, ty - h / 2), pct_text, font=legend_font, fill="white")
        start_angle = end_angle

    legend_x = 1500
    legend_y = 250
    for idx, (label, value, pct, color) in enumerate(segments):
        y = legend_y + idx * 118
        draw.rounded_rectangle((legend_x, y, legend_x + 34, y + 34), radius=5, fill=color)
        text = f"{label}\n{value:,} ({pct:.1f}%)"
        draw.multiline_text((legend_x + 56, y - 3), text, font=legend_font, fill="#202020", spacing=4)

    note = (
        f"Percentages are based on {meta['total_assignments']:,} class assignments across "
        f"{meta['classified_patents']:,} classified patents; {meta['multi_class_patents']:,} patents belong to multiple classes."
    )
    draw_text(draw, (80, PIE_H - 55), note, note_font, "#666666")
    image.save(output_path, dpi=(600, 600))


def svg_text(x: float, y: float, text: str, size: int, fill: str, anchor: str = "start", weight: str = "normal") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Times New Roman" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{html.escape(text)}</text>'
    )


def create_bar_chart_svg(rows: list[dict[str, float | int | str]], meta: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    margin_left = 930
    margin_right = 240
    margin_top = 210
    margin_bottom = 170
    plot_left = margin_left
    plot_right = BAR_W - margin_right
    plot_top = margin_top
    plot_bottom = BAR_H - margin_bottom
    max_value = max(int(r["patent_count"]) for r in rows)
    tick_step = 1000 if max_value > 4000 else 500
    axis_max = int(math.ceil(max_value / tick_step) * tick_step)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{BAR_W}" height="{BAR_H}" viewBox="0 0 {BAR_W} {BAR_H}">',
        '<rect width="100%" height="100%" fill="white"/>',
        svg_text(BAR_W / 2, 100, "Technology class distribution by patent publications", 56, "#202020", anchor="middle", weight="bold"),
    ]

    for value in range(0, axis_max + tick_step, tick_step):
        x = plot_left + (value / axis_max) * (plot_right - plot_left)
        parts.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_bottom}" stroke="#E0E0E0" stroke-width="2"/>')
        parts.append(svg_text(x, plot_bottom + 52, f"{value:,}", 28, "#404040", anchor="middle"))

    parts.extend(
        [
            f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" stroke="#404040" stroke-width="4"/>',
            f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" stroke="#404040" stroke-width="4"/>',
        ]
    )

    bar_gap = (plot_bottom - plot_top) / len(rows)
    bar_height = bar_gap * 0.64
    for idx, row in enumerate(rows):
        y_center = plot_top + idx * bar_gap + bar_gap / 2
        y0 = y_center - bar_height / 2
        value = int(row["patent_count"])
        x1 = plot_left + (value / axis_max) * (plot_right - plot_left)
        color = PALETTE[idx % len(PALETTE)]
        parts.append(
            f'<rect x="{plot_left}" y="{y0:.1f}" width="{x1 - plot_left:.1f}" height="{bar_height:.1f}" rx="10" ry="10" fill="{color}"/>'
        )
        label_lines = str(row["technology_class"]).replace(" - ", " -\n").split("\n")
        text_y = y_center - 10 * (len(label_lines) - 1)
        for line_idx, line in enumerate(label_lines):
            parts.append(svg_text(margin_left - 30, text_y + line_idx * 34, line, 28, "#202020", anchor="end"))
        parts.append(svg_text(x1 + 16, y_center + 10, f"{value:,} ({float(row['assignment_share_pct']):.1f}%)", 28, "#202020"))

    parts.append(svg_text(plot_left + 580, BAR_H - 90, "Patent publications", 38, "#202020", weight="bold"))
    note = (
        f"Classified patents: {meta['classified_patents']:,} / {meta['total_patents']:,}; "
        f"multi-class patents: {meta['multi_class_patents']:,}. Percentages use class-assignment totals as denominator."
    )
    parts.append(svg_text(80, BAR_H - 40, note, 24, "#666666"))
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def create_pie_chart_svg(rows: list[dict[str, float | int | str]], meta: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cx, cy = 820, 1110
    radius = 620
    segments = pie_segments(rows)
    start_angle = -90.0

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{PIE_W}" height="{PIE_H}" viewBox="0 0 {PIE_W} {PIE_H}">',
        '<rect width="100%" height="100%" fill="white"/>',
        svg_text(PIE_W / 2, 100, "Technology class share of patent publications", 54, "#202020", anchor="middle", weight="bold"),
    ]

    for label, value, pct, color in segments:
        end_angle = start_angle + pct * 3.6
        x1 = cx + radius * math.cos(math.radians(start_angle))
        y1 = cy + radius * math.sin(math.radians(start_angle))
        x2 = cx + radius * math.cos(math.radians(end_angle))
        y2 = cy + radius * math.sin(math.radians(end_angle))
        large_arc = 1 if end_angle - start_angle > 180 else 0
        path = (
            f'M {cx},{cy} L {x1:.1f},{y1:.1f} '
            f'A {radius},{radius} 0 {large_arc},1 {x2:.1f},{y2:.1f} Z'
        )
        parts.append(f'<path d="{path}" fill="{color}" stroke="white" stroke-width="4"/>')
        if pct >= 4.0:
            mid = math.radians((start_angle + end_angle) / 2)
            tx = cx + math.cos(mid) * radius * 0.62
            ty = cy + math.sin(mid) * radius * 0.62
            parts.append(svg_text(tx, ty + 10, f"{pct:.1f}%", 28, "white", anchor="middle"))
        start_angle = end_angle

    legend_x = 1500
    legend_y = 250
    for idx, (label, value, pct, color) in enumerate(segments):
        y = legend_y + idx * 118
        parts.append(f'<rect x="{legend_x}" y="{y}" width="34" height="34" rx="5" ry="5" fill="{color}"/>')
        parts.append(svg_text(legend_x + 56, y + 24, label, 28, "#202020"))
        parts.append(svg_text(legend_x + 56, y + 58, f"{value:,} ({pct:.1f}%)", 28, "#202020"))

    note = (
        f"Percentages are based on {meta['total_assignments']:,} class assignments across "
        f"{meta['classified_patents']:,} classified patents; {meta['multi_class_patents']:,} patents belong to multiple classes."
    )
    parts.append(svg_text(80, PIE_H - 40, note, 24, "#666666"))
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export technology-class counts from Neo4j and generate bar/pie figures."
    )
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument(
        "--csv-output",
        default="docs/bsab_patent_landscape/data/technology_class_patent_counts.csv",
    )
    parser.add_argument(
        "--bar-png-output",
        default="docs/bsab_patent_landscape/figures/technology_class_bar.png",
    )
    parser.add_argument(
        "--bar-svg-output",
        default="docs/bsab_patent_landscape/figures/technology_class_bar.svg",
    )
    parser.add_argument(
        "--pie-png-output",
        default="docs/bsab_patent_landscape/figures/technology_class_pie.png",
    )
    parser.add_argument(
        "--pie-svg-output",
        default="docs/bsab_patent_landscape/figures/technology_class_pie.svg",
    )
    args = parser.parse_args()

    rows, meta = fetch_technology_class_rows(args.settings)

    csv_path = ROOT / args.csv_output
    bar_png_path = ROOT / args.bar_png_output
    bar_svg_path = ROOT / args.bar_svg_output
    pie_png_path = ROOT / args.pie_png_output
    pie_svg_path = ROOT / args.pie_svg_output

    write_csv(rows, csv_path)
    create_bar_chart_png(rows, meta, bar_png_path)
    create_bar_chart_svg(rows, meta, bar_svg_path)
    create_pie_chart_png(rows, meta, pie_png_path)
    create_pie_chart_svg(rows, meta, pie_svg_path)

    print(f"CSV exported to: {csv_path}")
    print(f"Bar PNG exported to: {bar_png_path}")
    print(f"Bar SVG exported to: {bar_svg_path}")
    print(f"Pie PNG exported to: {pie_png_path}")
    print(f"Pie SVG exported to: {pie_svg_path}")


if __name__ == "__main__":
    main()
