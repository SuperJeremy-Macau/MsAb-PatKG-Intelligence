from __future__ import annotations

import argparse
import csv
import html
import sys
from collections import defaultdict
from pathlib import Path

from neo4j import GraphDatabase
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bsab_kg_qa_en.config import load_settings


ORIGIN_ORDER = ["US", "EU", "China", "JP", "UK", "KR", "Other", "Personal"]
PLOT_ORDER = ["US", "EU", "China", "JP", "UK", "KR", "Other"]
COLORS = {
    "US": "#1F4E79",
    "EU": "#4F81BD",
    "China": "#C55A11",
    "JP": "#548235",
    "UK": "#8064A2",
    "KR": "#2F75B5",
    "Other": "#A5A5A5",
    "Personal": "#BF9000",
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


def fetch_origin_year(settings_path: str, start_year: int) -> list[dict[str, object]]:
    cfg = load_settings(settings_path)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    query = """
    MATCH (p:Patent)-[:PUBLISHED_IN]->(y:Year)
    MATCH (p)-[:HAS_ASSIGNEE]->(:Assignee)-[:ORIGIN_FROM]->(o:Origin)
    WHERE y.year >= $start_year
    RETURN y.year AS year, o.name AS origin, count(DISTINCT p) AS patent_count
    ORDER BY year ASC, patent_count DESC, origin ASC
    """

    try:
        with driver.session(database=neo["database"]) as session:
            rows = [dict(r) for r in session.run(query, {"start_year": start_year})]
    finally:
        driver.close()

    return [
        {"year": int(r["year"]), "origin": str(r["origin"]), "patent_count": int(r["patent_count"])}
        for r in rows
    ]


def build_biennial_rows(rows: list[dict[str, object]], start_year: int) -> list[dict[str, object]]:
    by_period_origin: dict[tuple[int, str], int] = defaultdict(int)
    end_year = max(int(r["year"]) for r in rows)

    for row in rows:
        year = int(row["year"])
        origin = str(row["origin"])
        period_start = start_year + ((year - start_year) // 2) * 2
        by_period_origin[(period_start, origin)] += int(row["patent_count"])

    output: list[dict[str, object]] = []
    for period_start in range(start_year, end_year + 1, 2):
        period_end = min(period_start + 1, end_year)
        period_label = f"{period_start}-{period_end}"
        total_all = sum(by_period_origin.get((period_start, origin), 0) for origin in ORIGIN_ORDER)
        total_excl_personal = sum(by_period_origin.get((period_start, origin), 0) for origin in PLOT_ORDER)
        if total_all == 0:
            continue
        for origin in ORIGIN_ORDER:
            patent_count = by_period_origin.get((period_start, origin), 0)
            output.append(
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "period_label": period_label,
                    "origin": origin,
                    "patent_count": patent_count,
                    "total_patents_all_origins": total_all,
                    "share_pct_all_origins": round(100.0 * patent_count / total_all, 2),
                    "total_patents_excl_personal": total_excl_personal,
                    "share_pct_excl_personal_plot": round(
                        100.0 * patent_count / total_excl_personal, 2
                    ) if origin != "Personal" and total_excl_personal > 0 else 0.0,
                    "is_in_plot": 0 if origin == "Personal" else 1,
                }
            )
    return output


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.multiline_textbbox((0, 0), text, font=text_font, spacing=4)
    return box[2] - box[0], box[3] - box[1]


def draw_centered(draw: ImageDraw.ImageDraw, x: float, y: float, text: str, text_font, fill: str) -> None:
    w, h = measure(draw, text, text_font)
    draw.multiline_text((x - w / 2, y - h / 2), text, font=text_font, fill=fill, spacing=4)


def create_png(rows: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_rows = [row for row in rows if int(row["is_in_plot"]) == 1]
    periods = []
    shares = {}
    for row in plot_rows:
        period = str(row["period_label"])
        origin = str(row["origin"])
        if period not in periods:
            periods.append(period)
        shares[(period, origin)] = float(row["share_pct_excl_personal_plot"])

    width, height = 3200, 2100
    plot_left, plot_right = 170, 2200
    plot_top, plot_bottom = 150, 1680
    step = (plot_right - plot_left) / len(periods)
    bar_width = step * 0.68

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    axis_font = font(38, True)
    tick_font = font(28, False)
    legend_font = font(30, False)
    note_font = font(23, False)

    last_x0 = plot_left + (len(periods) - 1) * step
    draw.rectangle((last_x0, plot_top, plot_right, plot_bottom), fill="#F5F5F5")

    for pct in range(0, 101, 20):
        y = plot_bottom - (pct / 100.0) * (plot_bottom - plot_top)
        draw.line((plot_left, y, plot_right, y), fill="#E2E2E2", width=2)
        label = f"{pct}%"
        w, h = measure(draw, label, tick_font)
        draw.text((plot_left - 22 - w, y - h / 2), label, font=tick_font, fill="#404040")

    draw.line((plot_left, plot_top, plot_left, plot_bottom), fill="#303030", width=4)
    draw.line((plot_left, plot_bottom, plot_right, plot_bottom), fill="#303030", width=4)

    for idx, period in enumerate(periods):
        x0 = plot_left + idx * step + (step - bar_width) / 2
        x1 = x0 + bar_width
        y_cursor = plot_bottom
        for origin in reversed(PLOT_ORDER):
            share = shares.get((period, origin), 0.0)
            if share <= 0:
                continue
            h = (share / 100.0) * (plot_bottom - plot_top)
            y0 = y_cursor - h
            draw.rectangle((x0, y0, x1, y_cursor), fill=COLORS[origin], outline="white", width=1)
            y_cursor = y0
        draw_centered(draw, x0 + bar_width / 2, plot_bottom + 48, period, tick_font, "#404040")

    draw_centered(draw, (plot_left + plot_right) / 2, plot_bottom + 118, "Publication year", axis_font, "#202020")
    draw.text((18, (plot_top + plot_bottom) / 2 - 25), "Share", font=axis_font, fill="#202020")

    legend_x = 2360
    legend_y = 270
    for idx, origin in enumerate(PLOT_ORDER):
        y = legend_y + idx * 86
        draw.rounded_rectangle((legend_x, y, legend_x + 34, y + 34), radius=5, fill=COLORS[origin])
        draw.text((legend_x + 56, y + 2), origin, font=legend_font, fill="#202020")

    note = (
        "Two-year 100% stacked bars from 2010. CSV includes all origins, but the plot excludes Personal "
        "and re-normalizes each bar to 100%. 2024-2025 includes partial latest-year data."
    )
    draw.multiline_text((70, height - 70), note, font=note_font, fill="#666666", spacing=4)
    image.save(output_path, dpi=(600, 600))


def svg_text(x: float, y: float, text: str, size: int, fill: str, anchor: str = "start", weight: str = "normal") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Times New Roman" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{html.escape(text)}</text>'
    )


def create_svg(rows: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_rows = [row for row in rows if int(row["is_in_plot"]) == 1]
    periods = []
    shares = {}
    for row in plot_rows:
        period = str(row["period_label"])
        origin = str(row["origin"])
        if period not in periods:
            periods.append(period)
        shares[(period, origin)] = float(row["share_pct_excl_personal_plot"])

    width, height = 3200, 2100
    plot_left, plot_right = 170, 2200
    plot_top, plot_bottom = 150, 1680
    step = (plot_right - plot_left) / len(periods)
    bar_width = step * 0.68

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]

    last_x0 = plot_left + (len(periods) - 1) * step
    parts.append(f'<rect x="{last_x0:.1f}" y="{plot_top}" width="{plot_right - last_x0:.1f}" height="{plot_bottom - plot_top}" fill="#F5F5F5"/>')

    for pct in range(0, 101, 20):
        y = plot_bottom - (pct / 100.0) * (plot_bottom - plot_top)
        parts.append(f'<line x1="{plot_left}" y1="{y:.1f}" x2="{plot_right}" y2="{y:.1f}" stroke="#E2E2E2" stroke-width="2"/>')
        parts.append(svg_text(plot_left - 22, y + 10, f"{pct}%", 28, "#404040", anchor="end"))

    parts.extend(
        [
            f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
            f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
        ]
    )

    for idx, period in enumerate(periods):
        x0 = plot_left + idx * step + (step - bar_width) / 2
        y_cursor = plot_bottom
        for origin in reversed(PLOT_ORDER):
            share = shares.get((period, origin), 0.0)
            if share <= 0:
                continue
            h = (share / 100.0) * (plot_bottom - plot_top)
            y0 = y_cursor - h
            parts.append(f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{bar_width:.1f}" height="{h:.1f}" fill="{COLORS[origin]}" stroke="white" stroke-width="1"/>')
            y_cursor = y0
        parts.append(svg_text(x0 + bar_width / 2, plot_bottom + 56, period, 28, "#404040", anchor="middle"))

    parts.append(svg_text((plot_left + plot_right) / 2, plot_bottom + 125, "Publication year", 38, "#202020", anchor="middle", weight="bold"))
    parts.append(svg_text(18, (plot_top + plot_bottom) / 2, "Share", 38, "#202020", weight="bold"))

    legend_x = 2360
    legend_y = 270
    for idx, origin in enumerate(PLOT_ORDER):
        y = legend_y + idx * 86
        parts.append(f'<rect x="{legend_x}" y="{y}" width="34" height="34" rx="5" ry="5" fill="{COLORS[origin]}"/>')
        parts.append(svg_text(legend_x + 56, y + 28, origin, 30, "#202020"))

    note = (
        "Two-year 100% stacked bars from 2010. CSV includes all origins, but the plot excludes Personal "
        "and re-normalizes each bar to 100%. 2024-2025 includes partial latest-year data."
    )
    parts.append(svg_text(70, height - 42, note, 23, "#666666"))
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export biennial origin shares from 2010 and generate a 100% stacked bar chart.")
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--csv-output", default="docs/bsab_patent_landscape/data/origin_share_biennial_2010plus_full.csv")
    parser.add_argument("--png-output", default="docs/bsab_patent_landscape/figures/origin_share_biennial_2010plus.png")
    parser.add_argument("--svg-output", default="docs/bsab_patent_landscape/figures/origin_share_biennial_2010plus.svg")
    args = parser.parse_args()

    yearly_rows = fetch_origin_year(args.settings, args.start_year)
    biennial_rows = build_biennial_rows(yearly_rows, args.start_year)

    csv_path = ROOT / args.csv_output
    png_path = ROOT / args.png_output
    svg_path = ROOT / args.svg_output

    write_csv(csv_path, biennial_rows)
    create_png(biennial_rows, png_path)
    create_svg(biennial_rows, svg_path)

    print(f"CSV exported to: {csv_path}")
    print(f"PNG exported to: {png_path}")
    print(f"SVG exported to: {svg_path}")


if __name__ == "__main__":
    main()
