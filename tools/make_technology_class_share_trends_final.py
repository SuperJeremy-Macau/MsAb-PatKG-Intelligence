from __future__ import annotations

import argparse
import csv
import html
from collections import defaultdict
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]

MERGE_THRESHOLD_PCT = 2.0

DISPLAY_LABELS = {
    "Trans-Bridging Immune Engagers": "Trans-bridging immune engagers",
    "T-cell Activation - Signal-2-Related Mechanisms": "Signal-2 T-cell activation",
    "Tumor-Intrinsic Control": "Tumor-intrinsic control",
    "Other functional combination": "Other functional combinations",
    "Cytokine related functional combination": "Cytokine-related combinations",
    "Other Immune-Checkpoint-Related Mechanisms": "Other checkpoint-related",
    "Angiogenesis related functional combination": "Angiogenesis-related",
    "T-cell Activation - Signal-3-Related Mechanisms": "Signal-3 T-cell activation",
    "Piggybacking": "Piggybacking",
    "Tumor-Microenvironment (TME) Remodeling Axes": "TME remodeling",
    "B-cell related functional combination": "B-cell-related",
    "Mast cell related functional combination": "Mast-cell-related",
    "Other": "Other",
}

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
    "Other": "#B7B7B7",
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


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def aggregate_small_classes(rows: list[dict[str, str]], threshold_pct: float) -> tuple[list[dict[str, object]], list[str], list[str]]:
    total_by_class: dict[str, int] = defaultdict(int)
    counts: dict[tuple[str, str], int] = {}
    totals_by_period: dict[str, int] = {}
    period_bounds: dict[str, tuple[int, int]] = {}

    for row in rows:
        label = row["technology_class"]
        period = row["period_label"]
        count = int(row["patent_count"])
        total_by_class[label] += count
        counts[(period, label)] = count
        totals_by_period[period] = int(row["total_assignments"])
        period_bounds[period] = (int(row["period_start"]), int(row["period_end"]))

    grand_total = sum(total_by_class.values())
    keep_classes = [
        label
        for label, count in sorted(total_by_class.items(), key=lambda item: (-item[1], item[0].lower()))
        if 100.0 * count / grand_total >= threshold_pct
    ]
    merged_classes = [label for label in total_by_class if label not in keep_classes]
    class_order = keep_classes + (["Other"] if merged_classes else [])

    output: list[dict[str, object]] = []
    for period, (period_start, period_end) in sorted(period_bounds.items(), key=lambda item: item[1]):
        total_assignments = totals_by_period[period]
        other_count = sum(counts.get((period, label), 0) for label in merged_classes)
        for label in keep_classes:
            patent_count = counts.get((period, label), 0)
            output.append(
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "period_label": period,
                    "technology_class": label,
                    "display_label": DISPLAY_LABELS.get(label, label),
                    "patent_count": patent_count,
                    "total_assignments": total_assignments,
                    "share_pct": round(100.0 * patent_count / total_assignments, 2),
                }
            )
        if merged_classes:
            output.append(
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "period_label": period,
                    "technology_class": "Other",
                    "display_label": DISPLAY_LABELS["Other"],
                    "patent_count": other_count,
                    "total_assignments": total_assignments,
                    "share_pct": round(100.0 * other_count / total_assignments, 2),
                }
            )

    return output, class_order, merged_classes


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.multiline_textbbox((0, 0), text, font=text_font, spacing=4)
    return box[2] - box[0], box[3] - box[1]


def draw_centered(draw: ImageDraw.ImageDraw, x: float, y: float, text: str, text_font, fill: str) -> None:
    w, h = measure(draw, text, text_font)
    draw.multiline_text((x - w / 2, y - h / 2), text, font=text_font, fill=fill, spacing=4)


def create_final_png(
    rows: list[dict[str, object]],
    class_order: list[str],
    output_path: Path,
    note: str,
    shade_last: bool = True,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    periods = []
    for row in rows:
        label = str(row["period_label"])
        if label not in periods:
            periods.append(label)

    shares = {(str(r["period_label"]), str(r["technology_class"])): float(r["share_pct"]) for r in rows}

    width = 4200
    height = 2100
    plot_left = 180
    plot_right = 2920
    plot_top = 140
    plot_bottom = 1680

    axis_font = font(38, True)
    tick_font = font(28, False)
    legend_font = font(28, False)
    note_font = font(23, False)

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    if shade_last and periods:
        step = (plot_right - plot_left) / len(periods)
        x0 = plot_left + (len(periods) - 1) * step
        draw.rectangle((x0, plot_top, plot_right, plot_bottom), fill="#F5F5F5")

    for pct in range(0, 101, 20):
        y = plot_bottom - (pct / 100.0) * (plot_bottom - plot_top)
        draw.line((plot_left, y, plot_right, y), fill="#E2E2E2", width=2)
        label = f"{pct}%"
        w, h = measure(draw, label, tick_font)
        draw.text((plot_left - 24 - w, y - h / 2), label, font=tick_font, fill="#404040")

    draw.line((plot_left, plot_top, plot_left, plot_bottom), fill="#303030", width=4)
    draw.line((plot_left, plot_bottom, plot_right, plot_bottom), fill="#303030", width=4)

    step = (plot_right - plot_left) / len(periods)
    bar_width = step * 0.68

    for idx, period in enumerate(periods):
        x0 = plot_left + idx * step + (step - bar_width) / 2
        x1 = x0 + bar_width
        y_cursor = plot_bottom
        for label in reversed(class_order):
            share = shares.get((period, label), 0.0)
            if share <= 0:
                continue
            h = (share / 100.0) * (plot_bottom - plot_top)
            y0 = y_cursor - h
            draw.rectangle((x0, y0, x1, y_cursor), fill=PALETTE[label], outline="white", width=1)
            y_cursor = y0
        draw_centered(draw, x0 + bar_width / 2, plot_bottom + 45, period, tick_font, "#404040")

    draw_centered(draw, (plot_left + plot_right) / 2, plot_bottom + 115, "Publication year", axis_font, "#202020")
    draw.text((18, (plot_top + plot_bottom) / 2 - 25), "Share", font=axis_font, fill="#202020")

    legend_x = 3050
    legend_y = 170
    for idx, label in enumerate(class_order):
        y = legend_y + idx * 118
        draw.rounded_rectangle((legend_x, y, legend_x + 34, y + 34), radius=5, fill=PALETTE[label])
        draw.multiline_text(
            (legend_x + 54, y - 1),
            DISPLAY_LABELS.get(label, label).replace(" ", "\n", 1) if len(DISPLAY_LABELS.get(label, label)) > 28 and "Other" not in label else DISPLAY_LABELS.get(label, label),
            font=legend_font,
            fill="#202020",
            spacing=2,
        )

    draw.multiline_text((70, height - 70), note, font=note_font, fill="#666666", spacing=4)
    image.save(output_path, dpi=(600, 600))


def svg_text(x: float, y: float, text: str, size: int, fill: str, anchor: str = "start", weight: str = "normal") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Times New Roman" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{html.escape(text)}</text>'
    )


def create_final_svg(
    rows: list[dict[str, object]],
    class_order: list[str],
    output_path: Path,
    note: str,
    shade_last: bool = True,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    periods = []
    for row in rows:
        label = str(row["period_label"])
        if label not in periods:
            periods.append(label)

    shares = {(str(r["period_label"]), str(r["technology_class"])): float(r["share_pct"]) for r in rows}

    width = 4200
    height = 2100
    plot_left = 180
    plot_right = 2920
    plot_top = 140
    plot_bottom = 1680
    step = (plot_right - plot_left) / len(periods)
    bar_width = step * 0.68

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]

    if shade_last and periods:
        x0 = plot_left + (len(periods) - 1) * step
        parts.append(f'<rect x="{x0:.1f}" y="{plot_top}" width="{plot_right - x0:.1f}" height="{plot_bottom - plot_top}" fill="#F5F5F5"/>')

    for pct in range(0, 101, 20):
        y = plot_bottom - (pct / 100.0) * (plot_bottom - plot_top)
        parts.append(f'<line x1="{plot_left}" y1="{y:.1f}" x2="{plot_right}" y2="{y:.1f}" stroke="#E2E2E2" stroke-width="2"/>')
        parts.append(svg_text(plot_left - 24, y + 10, f"{pct}%", 28, "#404040", anchor="end"))

    parts.extend(
        [
            f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
            f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
        ]
    )

    for idx, period in enumerate(periods):
        x0 = plot_left + idx * step + (step - bar_width) / 2
        y_cursor = plot_bottom
        for label in reversed(class_order):
            share = shares.get((period, label), 0.0)
            if share <= 0:
                continue
            h = (share / 100.0) * (plot_bottom - plot_top)
            y0 = y_cursor - h
            parts.append(
                f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{bar_width:.1f}" height="{h:.1f}" fill="{PALETTE[label]}" stroke="white" stroke-width="1"/>'
            )
            y_cursor = y0
        parts.append(svg_text(x0 + bar_width / 2, plot_bottom + 54, period, 28, "#404040", anchor="middle"))

    parts.append(svg_text((plot_left + plot_right) / 2, plot_bottom + 125, "Publication year", 38, "#202020", anchor="middle", weight="bold"))
    parts.append(svg_text(18, (plot_top + plot_bottom) / 2, "Share", 38, "#202020", weight="bold"))

    legend_x = 3050
    legend_y = 170
    for idx, label in enumerate(class_order):
        y = legend_y + idx * 118
        parts.append(f'<rect x="{legend_x}" y="{y}" width="34" height="34" rx="5" ry="5" fill="{PALETTE[label]}"/>')
        display = DISPLAY_LABELS.get(label, label)
        if len(display) > 28 and "Other" not in display:
            first, rest = display.split(" ", 1)
            lines = [first, rest]
        else:
            lines = [display]
        for line_idx, line in enumerate(lines):
            parts.append(svg_text(legend_x + 54, y + 25 + line_idx * 30, line, 28, "#202020"))

    parts.append(svg_text(70, height - 42, note, 23, "#666666"))
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create publication-ready final technology-class share trend figures.")
    parser.add_argument(
        "--yearly-input",
        default="docs/bsab_patent_landscape/data/technology_class_share_yearly_2010plus.csv",
    )
    parser.add_argument(
        "--biennial-input",
        default="docs/bsab_patent_landscape/data/technology_class_share_biennial_2010plus.csv",
    )
    parser.add_argument(
        "--yearly-output-csv",
        default="docs/bsab_patent_landscape/data/technology_class_share_yearly_2010plus_final.csv",
    )
    parser.add_argument(
        "--biennial-output-csv",
        default="docs/bsab_patent_landscape/data/technology_class_share_biennial_2010plus_final.csv",
    )
    parser.add_argument(
        "--yearly-output-png",
        default="docs/bsab_patent_landscape/figures/technology_class_share_yearly_2010plus_final.png",
    )
    parser.add_argument(
        "--yearly-output-svg",
        default="docs/bsab_patent_landscape/figures/technology_class_share_yearly_2010plus_final.svg",
    )
    parser.add_argument(
        "--biennial-output-png",
        default="docs/bsab_patent_landscape/figures/technology_class_share_biennial_2010plus_final.png",
    )
    parser.add_argument(
        "--biennial-output-svg",
        default="docs/bsab_patent_landscape/figures/technology_class_share_biennial_2010plus_final.svg",
    )
    parser.add_argument("--merge-threshold-pct", type=float, default=MERGE_THRESHOLD_PCT)
    args = parser.parse_args()

    yearly_rows = read_csv(ROOT / args.yearly_input)
    biennial_rows = read_csv(ROOT / args.biennial_input)

    yearly_final_rows, yearly_order, merged_classes = aggregate_small_classes(yearly_rows, args.merge_threshold_pct)
    biennial_final_rows, biennial_order, _ = aggregate_small_classes(biennial_rows, args.merge_threshold_pct)

    write_csv(ROOT / args.yearly_output_csv, yearly_final_rows)
    write_csv(ROOT / args.biennial_output_csv, biennial_final_rows)

    merged_text = ", ".join(DISPLAY_LABELS.get(label, label) for label in merged_classes) if merged_classes else "none"
    yearly_note = (
        f"Annual 100% stacked bars from 2010. Classes below {args.merge_threshold_pct:.1f}% overall share were merged into Other "
        f"({merged_text}); 2025 is partial year."
    )
    biennial_note = (
        f"Two-year 100% stacked bars from 2010. Classes below {args.merge_threshold_pct:.1f}% overall share were merged into Other "
        f"({merged_text}); 2024-2025 includes partial latest-year data."
    )

    create_final_png(yearly_final_rows, yearly_order, ROOT / args.yearly_output_png, yearly_note)
    create_final_svg(yearly_final_rows, yearly_order, ROOT / args.yearly_output_svg, yearly_note)
    create_final_png(biennial_final_rows, biennial_order, ROOT / args.biennial_output_png, biennial_note)
    create_final_svg(biennial_final_rows, biennial_order, ROOT / args.biennial_output_svg, biennial_note)

    print(f"Yearly final CSV exported to: {ROOT / args.yearly_output_csv}")
    print(f"Biennial final CSV exported to: {ROOT / args.biennial_output_csv}")
    print(f"Yearly final PNG exported to: {ROOT / args.yearly_output_png}")
    print(f"Yearly final SVG exported to: {ROOT / args.yearly_output_svg}")
    print(f"Biennial final PNG exported to: {ROOT / args.biennial_output_png}")
    print(f"Biennial final SVG exported to: {ROOT / args.biennial_output_svg}")


if __name__ == "__main__":
    main()
