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


CANVAS_W = 3200
CANVAS_H = 2100
PLOT_LEFT = 300
PLOT_RIGHT = 2860
PLOT_TOP = 210
PLOT_BOTTOM = 1540


def resolve_font_path(bold: bool = False) -> str | None:
    candidates = [
        Path("C:/Windows/Fonts/timesbd.ttf" if bold else "C:/Windows/Fonts/times.ttf"),
        Path("C:/Windows/Fonts/times.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return str(path)
    return None


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    path = resolve_font_path(bold)
    if path:
        return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def fetch_yearly_counts(settings_path: str) -> list[dict[str, float | int]]:
    cfg = load_settings(settings_path)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    patent_query = """
    MATCH (p:Patent)-[:PUBLISHED_IN]->(y:Year)
    RETURN y.year AS year, count(DISTINCT p) AS patent_count
    ORDER BY year
    """

    family_query = """
    MATCH (f:Family)-[:HAS_PATENT]->(p:Patent)-[:PUBLISHED_IN]->(y:Year)
    WITH f, min(y.year) AS year
    RETURN year, count(f) AS family_count
    ORDER BY year
    """

    try:
        with driver.session(database=neo["database"]) as session:
            patent_rows = [dict(r) for r in session.run(patent_query)]
            family_rows = [dict(r) for r in session.run(family_query)]
    finally:
        driver.close()

    patent_map = {int(r["year"]): int(r["patent_count"]) for r in patent_rows}
    family_map = {int(r["year"]): int(r["family_count"]) for r in family_rows}
    years = list(range(min(min(patent_map), min(family_map)), max(max(patent_map), max(family_map)) + 1))

    rows: list[dict[str, float | int]] = []
    cumulative_patents = 0
    cumulative_families = 0
    last_year = years[-1]

    for year in years:
        patent_count = patent_map.get(year, 0)
        family_count = family_map.get(year, 0)
        cumulative_patents += patent_count
        cumulative_families += family_count
        rows.append(
            {
                "year": year,
                "patent_count": patent_count,
                "family_count": family_count,
                "cumulative_patent_count": cumulative_patents,
                "cumulative_family_count": cumulative_families,
                "is_partial_year": 1 if year == last_year else 0,
            }
        )

    add_centered_moving_average(rows, "patent_count", "patent_count_ma3", window=3)
    add_centered_moving_average(rows, "family_count", "family_count_ma3", window=3)
    return rows


def add_centered_moving_average(
    rows: list[dict[str, float | int]], source_key: str, target_key: str, window: int = 3
) -> None:
    values = [float(row[source_key]) for row in rows]
    half = window // 2
    for idx, row in enumerate(rows):
        lo = max(0, idx - half)
        hi = min(len(values), idx + half + 1)
        row[target_key] = round(sum(values[lo:hi]) / (hi - lo), 2)


def write_csv(rows: list[dict[str, float | int]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def choose_tick_step(max_value: int, target_ticks: int = 5) -> int:
    if max_value <= 0:
        return 1
    rough = max_value / target_ticks
    magnitude = 10 ** int(math.floor(math.log10(rough)))
    for factor in (1, 2, 5, 10):
        step = magnitude * factor
        if step >= rough:
            return int(step)
    return int(magnitude * 10)


def map_x(year: int, start_year: int, end_year: int) -> float:
    span = max(1, end_year - start_year)
    return PLOT_LEFT + ((year - start_year) / span) * (PLOT_RIGHT - PLOT_LEFT)


def map_y(value: float, max_value: int) -> float:
    return PLOT_BOTTOM - (value / max_value) * (PLOT_BOTTOM - PLOT_TOP)


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=text_font)
    return box[2] - box[0], box[3] - box[1]


def draw_centered_text(draw: ImageDraw.ImageDraw, x: float, y: float, text: str, text_font, fill: str) -> None:
    w, h = measure(draw, text, text_font)
    draw.text((x - w / 2, y - h / 2), text, font=text_font, fill=fill)


def create_png(rows: list[dict[str, float | int]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    years = [int(row["year"]) for row in rows]
    patents = [int(row["patent_count"]) for row in rows]
    families = [int(row["family_count"]) for row in rows]
    patent_ma3 = [float(row["patent_count_ma3"]) for row in rows]

    start_year = years[0]
    end_year = years[-1]
    left_max = max(patents)
    right_max = max(families)
    left_step = choose_tick_step(int(left_max * 1.08))
    right_step = choose_tick_step(int(right_max * 1.12))
    left_axis_max = int(math.ceil(left_max / left_step) * left_step)
    right_axis_max = int(math.ceil(right_max / right_step) * right_step)

    patent_bar_color = "#CBD5DF"
    patent_line_color = "#1F4E79"
    family_line_color = "#C97933"
    axis_color = "#404040"
    grid_color = "#D7D7D7"
    partial_fill = "#F2F2F2"
    text_color = "#202020"

    title_font = font(72, bold=True)
    axis_font = font(42, bold=True)
    tick_font = font(32, bold=False)
    legend_font = font(34, bold=False)
    note_font = font(28, bold=False)
    annot_font = font(30, bold=False)

    image = Image.new("RGB", (CANVAS_W, CANVAS_H), "white")
    draw = ImageDraw.Draw(image)

    partial_x0 = map_x(end_year - 0.5, start_year, end_year)
    partial_x1 = map_x(end_year + 0.5, start_year, end_year)
    draw.rectangle((partial_x0, PLOT_TOP, partial_x1, PLOT_BOTTOM), fill=partial_fill)

    for value in range(0, left_axis_max + left_step, left_step):
        y = map_y(value, left_axis_max)
        draw.line((PLOT_LEFT, y, PLOT_RIGHT, y), fill=grid_color, width=2)
        label = f"{value:,}"
        w, h = measure(draw, label, tick_font)
        draw.text((PLOT_LEFT - 30 - w, y - h / 2), label, font=tick_font, fill=axis_color)

    draw.line((PLOT_LEFT, PLOT_TOP, PLOT_LEFT, PLOT_BOTTOM), fill=axis_color, width=4)
    draw.line((PLOT_RIGHT, PLOT_TOP, PLOT_RIGHT, PLOT_BOTTOM), fill=axis_color, width=4)
    draw.line((PLOT_LEFT, PLOT_BOTTOM, PLOT_RIGHT, PLOT_BOTTOM), fill=axis_color, width=4)

    year_step = 4
    for year in range(start_year, end_year + 1, year_step):
        x = map_x(year, start_year, end_year)
        draw.line((x, PLOT_BOTTOM, x, PLOT_BOTTOM + 18), fill=axis_color, width=3)
        draw_centered_text(draw, x, PLOT_BOTTOM + 55, str(year), tick_font, axis_color)
    if (end_year - start_year) % year_step != 0:
        x = map_x(end_year, start_year, end_year)
        draw.line((x, PLOT_BOTTOM, x, PLOT_BOTTOM + 18), fill=axis_color, width=3)
        draw_centered_text(draw, x, PLOT_BOTTOM + 55, str(end_year), tick_font, axis_color)

    bar_width = ((PLOT_RIGHT - PLOT_LEFT) / max(1, len(years) - 1)) * 0.72
    for year, value in zip(years, patents):
        x = map_x(year, start_year, end_year)
        y = map_y(value, left_axis_max)
        draw.rectangle((x - bar_width / 2, y, x + bar_width / 2, PLOT_BOTTOM), fill=patent_bar_color)

    patent_points = [(map_x(year, start_year, end_year), map_y(value, left_axis_max)) for year, value in zip(years, patent_ma3)]
    for idx in range(1, len(patent_points)):
        draw.line((*patent_points[idx - 1], *patent_points[idx]), fill=patent_line_color, width=8)

    family_points = [(map_x(year, start_year, end_year), map_y(value, right_axis_max)) for year, value in zip(years, families)]
    for idx in range(1, len(family_points)):
        draw.line((*family_points[idx - 1], *family_points[idx]), fill=family_line_color, width=6)
    for x, y in family_points:
        draw.ellipse((x - 8, y - 8, x + 8, y + 8), outline=family_line_color, fill="white", width=4)

    for value in range(0, right_axis_max + right_step, right_step):
        y = map_y(value, right_axis_max)
        label = f"{value:,}"
        draw.text((PLOT_RIGHT + 20, y - measure(draw, label, tick_font)[1] / 2), label, font=tick_font, fill=axis_color)

    draw_centered_text(draw, (PLOT_LEFT + PLOT_RIGHT) / 2, 90, "Overall trend of patent publications and patent families", title_font, text_color)
    draw_centered_text(draw, (PLOT_LEFT + PLOT_RIGHT) / 2, PLOT_BOTTOM + 135, "Publication year", axis_font, text_color)

    draw.text((70, (PLOT_TOP + PLOT_BOTTOM) / 2 - 190), "Patent", font=axis_font, fill=text_color)
    draw.text((70, (PLOT_TOP + PLOT_BOTTOM) / 2 - 130), "publications", font=axis_font, fill=text_color)
    draw.text((PLOT_RIGHT + 140, (PLOT_TOP + PLOT_BOTTOM) / 2 - 190), "New patent", font=axis_font, fill=text_color)
    draw.text((PLOT_RIGHT + 140, (PLOT_TOP + PLOT_BOTTOM) / 2 - 130), "families", font=axis_font, fill=text_color)

    legend_x = PLOT_LEFT + 30
    legend_y = 120
    draw.rectangle((legend_x, legend_y, legend_x + 70, legend_y + 26), fill=patent_bar_color)
    draw.text((legend_x + 95, legend_y - 8), "Annual patent publications", font=legend_font, fill=text_color)
    draw.line((legend_x + 660, legend_y + 13, legend_x + 735, legend_y + 13), fill=patent_line_color, width=8)
    draw.text((legend_x + 760, legend_y - 8), "3-year moving average", font=legend_font, fill=text_color)
    draw.line((legend_x + 1300, legend_y + 13, legend_x + 1375, legend_y + 13), fill=family_line_color, width=6)
    draw.ellipse((legend_x + 1330, legend_y + 5, legend_x + 1346, legend_y + 21), outline=family_line_color, fill="white", width=3)
    draw.text((legend_x + 1400, legend_y - 8), "New patent families", font=legend_font, fill=text_color)

    peak_patent_idx = max(range(len(patents)), key=lambda idx: patents[idx])
    peak_family_idx = max(range(len(families)), key=lambda idx: families[idx])

    peak_px = map_x(years[peak_patent_idx], start_year, end_year)
    peak_py = map_y(patent_ma3[peak_patent_idx], left_axis_max)
    draw.line((peak_px, peak_py, peak_px - 220, peak_py - 110), fill=patent_line_color, width=3)
    draw.text(
        (peak_px - 470, peak_py - 190),
        f"Peak publications\n{years[peak_patent_idx]}: {patents[peak_patent_idx]:,}",
        font=annot_font,
        fill=patent_line_color,
        spacing=8,
    )

    peak_fx = map_x(years[peak_family_idx], start_year, end_year)
    peak_fy = map_y(families[peak_family_idx], right_axis_max)
    draw.line((peak_fx, peak_fy, peak_fx - 230, peak_fy - 130), fill=family_line_color, width=3)
    draw.text(
        (peak_fx - 510, peak_fy - 220),
        f"Peak new families\n{years[peak_family_idx]}: {families[peak_family_idx]:,}",
        font=annot_font,
        fill=family_line_color,
        spacing=8,
    )

    draw.text((partial_x0 + 18, PLOT_TOP + 30), "Partial year", font=note_font, fill="#6D6D6D")
    draw.text(
        (PLOT_LEFT, PLOT_BOTTOM + 230),
        "Family counts are assigned to the first observed publication year for each patent family.",
        font=note_font,
        fill="#666666",
    )

    image.save(output_path, dpi=(600, 600))


def svg_text(x: float, y: float, value: str, size: int, fill: str, anchor: str = "start", weight: str = "normal") -> str:
    safe = html.escape(value)
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Times New Roman" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{safe}</text>'
    )


def create_svg(rows: list[dict[str, float | int]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    years = [int(row["year"]) for row in rows]
    patents = [int(row["patent_count"]) for row in rows]
    families = [int(row["family_count"]) for row in rows]
    patent_ma3 = [float(row["patent_count_ma3"]) for row in rows]

    start_year = years[0]
    end_year = years[-1]
    left_max = max(patents)
    right_max = max(families)
    left_step = choose_tick_step(int(left_max * 1.08))
    right_step = choose_tick_step(int(right_max * 1.12))
    left_axis_max = int(math.ceil(left_max / left_step) * left_step)
    right_axis_max = int(math.ceil(right_max / right_step) * right_step)

    patent_bar_color = "#CBD5DF"
    patent_line_color = "#1F4E79"
    family_line_color = "#C97933"
    axis_color = "#404040"
    grid_color = "#D7D7D7"
    partial_fill = "#F2F2F2"
    text_color = "#202020"

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{CANVAS_W}" height="{CANVAS_H}" viewBox="0 0 {CANVAS_W} {CANVAS_H}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]

    partial_x0 = map_x(end_year - 0.5, start_year, end_year)
    partial_x1 = map_x(end_year + 0.5, start_year, end_year)
    parts.append(
        f'<rect x="{partial_x0:.1f}" y="{PLOT_TOP}" width="{partial_x1 - partial_x0:.1f}" '
        f'height="{PLOT_BOTTOM - PLOT_TOP}" fill="{partial_fill}"/>'
    )

    for value in range(0, left_axis_max + left_step, left_step):
        y = map_y(value, left_axis_max)
        parts.append(
            f'<line x1="{PLOT_LEFT}" y1="{y:.1f}" x2="{PLOT_RIGHT}" y2="{y:.1f}" stroke="{grid_color}" stroke-width="2"/>'
        )
        parts.append(svg_text(PLOT_LEFT - 40, y + 10, f"{value:,}", 30, axis_color, anchor="end"))

    parts.extend(
        [
            f'<line x1="{PLOT_LEFT}" y1="{PLOT_TOP}" x2="{PLOT_LEFT}" y2="{PLOT_BOTTOM}" stroke="{axis_color}" stroke-width="4"/>',
            f'<line x1="{PLOT_RIGHT}" y1="{PLOT_TOP}" x2="{PLOT_RIGHT}" y2="{PLOT_BOTTOM}" stroke="{axis_color}" stroke-width="4"/>',
            f'<line x1="{PLOT_LEFT}" y1="{PLOT_BOTTOM}" x2="{PLOT_RIGHT}" y2="{PLOT_BOTTOM}" stroke="{axis_color}" stroke-width="4"/>',
        ]
    )

    year_step = 4
    tick_years = list(range(start_year, end_year + 1, year_step))
    if tick_years[-1] != end_year:
        tick_years.append(end_year)
    for year in tick_years:
        x = map_x(year, start_year, end_year)
        parts.append(f'<line x1="{x:.1f}" y1="{PLOT_BOTTOM}" x2="{x:.1f}" y2="{PLOT_BOTTOM + 18}" stroke="{axis_color}" stroke-width="3"/>')
        parts.append(svg_text(x, PLOT_BOTTOM + 62, str(year), 30, axis_color, anchor="middle"))

    bar_width = ((PLOT_RIGHT - PLOT_LEFT) / max(1, len(years) - 1)) * 0.72
    for year, value in zip(years, patents):
        x = map_x(year, start_year, end_year)
        y = map_y(value, left_axis_max)
        parts.append(
            f'<rect x="{x - bar_width / 2:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
            f'height="{PLOT_BOTTOM - y:.1f}" fill="{patent_bar_color}"/>'
        )

    patent_points = " ".join(f"{map_x(year, start_year, end_year):.1f},{map_y(value, left_axis_max):.1f}" for year, value in zip(years, patent_ma3))
    family_points = " ".join(f"{map_x(year, start_year, end_year):.1f},{map_y(value, right_axis_max):.1f}" for year, value in zip(years, families))
    parts.append(f'<polyline fill="none" stroke="{patent_line_color}" stroke-width="8" points="{patent_points}"/>')
    parts.append(f'<polyline fill="none" stroke="{family_line_color}" stroke-width="6" points="{family_points}"/>')

    for year, value in zip(years, families):
        x = map_x(year, start_year, end_year)
        y = map_y(value, right_axis_max)
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="8" fill="white" stroke="{family_line_color}" stroke-width="4"/>')

    for value in range(0, right_axis_max + right_step, right_step):
        y = map_y(value, right_axis_max)
        parts.append(svg_text(PLOT_RIGHT + 20, y + 10, f"{value:,}", 30, axis_color))

    parts.append(svg_text((PLOT_LEFT + PLOT_RIGHT) / 2, 100, "Overall trend of patent publications and patent families", 56, text_color, anchor="middle", weight="bold"))
    parts.append(svg_text((PLOT_LEFT + PLOT_RIGHT) / 2, PLOT_BOTTOM + 150, "Publication year", 38, text_color, anchor="middle", weight="bold"))
    parts.append(svg_text(110, (PLOT_TOP + PLOT_BOTTOM) / 2 - 140, "Patent", 38, text_color, weight="bold"))
    parts.append(svg_text(110, (PLOT_TOP + PLOT_BOTTOM) / 2 - 84, "publications", 38, text_color, weight="bold"))
    parts.append(svg_text(PLOT_RIGHT + 150, (PLOT_TOP + PLOT_BOTTOM) / 2 - 140, "New patent", 38, text_color, weight="bold"))
    parts.append(svg_text(PLOT_RIGHT + 150, (PLOT_TOP + PLOT_BOTTOM) / 2 - 84, "families", 38, text_color, weight="bold"))

    legend_x = PLOT_LEFT + 30
    legend_y = 110
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="70" height="26" fill="{patent_bar_color}"/>')
    parts.append(svg_text(legend_x + 95, legend_y + 22, "Annual patent publications", 32, text_color))
    parts.append(f'<line x1="{legend_x + 660}" y1="{legend_y + 13}" x2="{legend_x + 735}" y2="{legend_y + 13}" stroke="{patent_line_color}" stroke-width="8"/>')
    parts.append(svg_text(legend_x + 760, legend_y + 22, "3-year moving average", 32, text_color))
    parts.append(f'<line x1="{legend_x + 1300}" y1="{legend_y + 13}" x2="{legend_x + 1375}" y2="{legend_y + 13}" stroke="{family_line_color}" stroke-width="6"/>')
    parts.append(f'<circle cx="{legend_x + 1338}" cy="{legend_y + 13}" r="8" fill="white" stroke="{family_line_color}" stroke-width="3"/>')
    parts.append(svg_text(legend_x + 1400, legend_y + 22, "New patent families", 32, text_color))

    peak_patent_idx = max(range(len(patents)), key=lambda idx: patents[idx])
    peak_family_idx = max(range(len(families)), key=lambda idx: families[idx])
    peak_px = map_x(years[peak_patent_idx], start_year, end_year)
    peak_py = map_y(patent_ma3[peak_patent_idx], left_axis_max)
    peak_fx = map_x(years[peak_family_idx], start_year, end_year)
    peak_fy = map_y(families[peak_family_idx], right_axis_max)

    parts.append(f'<line x1="{peak_px:.1f}" y1="{peak_py:.1f}" x2="{peak_px - 220:.1f}" y2="{peak_py - 110:.1f}" stroke="{patent_line_color}" stroke-width="3"/>')
    parts.append(svg_text(peak_px - 470, peak_py - 170, "Peak publications", 30, patent_line_color))
    parts.append(svg_text(peak_px - 470, peak_py - 126, f"{years[peak_patent_idx]}: {patents[peak_patent_idx]:,}", 30, patent_line_color))

    parts.append(f'<line x1="{peak_fx:.1f}" y1="{peak_fy:.1f}" x2="{peak_fx - 230:.1f}" y2="{peak_fy - 130:.1f}" stroke="{family_line_color}" stroke-width="3"/>')
    parts.append(svg_text(peak_fx - 510, peak_fy - 200, "Peak new families", 30, family_line_color))
    parts.append(svg_text(peak_fx - 510, peak_fy - 156, f"{years[peak_family_idx]}: {families[peak_family_idx]:,}", 30, family_line_color))

    parts.append(svg_text(partial_x0 + 18, PLOT_TOP + 40, "Partial year", 28, "#6D6D6D"))
    parts.append(
        svg_text(
            PLOT_LEFT,
            PLOT_BOTTOM + 250,
            "Family counts are assigned to the first observed publication year for each patent family.",
            28,
            "#666666",
        )
    )

    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export annual patent/family trend from Neo4j and create publication-style figures."
    )
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--csv-output", default="docs/bsab_patent_landscape/data/overall_patent_family_trend.csv")
    parser.add_argument(
        "--png-output",
        default="docs/bsab_patent_landscape/figures/overall_patent_family_trend.png",
    )
    parser.add_argument(
        "--svg-output",
        default="docs/bsab_patent_landscape/figures/overall_patent_family_trend.svg",
    )
    args = parser.parse_args()

    rows = fetch_yearly_counts(args.settings)

    csv_path = ROOT / args.csv_output
    png_path = ROOT / args.png_output
    svg_path = ROOT / args.svg_output

    write_csv(rows, csv_path)
    create_png(rows, png_path)
    create_svg(rows, svg_path)

    print(f"CSV exported to: {csv_path}")
    print(f"PNG exported to: {png_path}")
    print(f"SVG exported to: {svg_path}")


if __name__ == "__main__":
    main()
