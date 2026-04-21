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


TOP_N_BAR = 15
TOP_N_CHORD = 8
MAX_CHORD_EDGES = 12

DISPLAY_LABELS = {
    "Malignant_Cell_Surface_Target": "Malignant cell surface",
    "T_Cell_Engagement_Target": "T-cell engagement",
    "Adaptive_Immune_Checkpoint_Target": "Adaptive immune checkpoint",
    "Growth_Factor_Receptor_Signaling_Target": "Growth factor receptor signaling",
    "Co_Stimulatory_Signaling_Axis_Target": "Co-stimulatory signaling axis",
    "TME_Remodeling_Target": "TME remodeling",
    "B_Cell_Lineage_Target": "B-cell lineage",
    "Cytokine_Ligand_Target": "Cytokine ligand",
    "Angiogenesis_Vascular_Target": "Angiogenesis / vascular",
    "Innate_Immune_Checkpoint_Target": "Innate immune checkpoint",
    "Fibrosis_ECM_Remodeling_Target": "Fibrosis / ECM remodeling",
    "Transcytosis_Shuttle_Target": "Transcytosis shuttle",
    "Stromal_Cell_Surface_Target": "Stromal cell surface",
    "Metabolic_Immune_Modulator_Target": "Metabolic immune modulator",
    "Hemostasis_Coagulation_Pathway_Target": "Hemostasis / coagulation",
    "NK_Cell_Engagement_Target": "NK-cell engagement",
    "Treg_Modulator_Target": "Treg modulator",
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
    "#8C564B",
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


def display_label(raw: str) -> str:
    return DISPLAY_LABELS.get(raw, raw.replace("_", " "))


def fetch_data(settings_path: str) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, int]]:
    cfg = load_settings(settings_path)
    neo = cfg["neo4j"]
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))

    function_query = """
    MATCH (p:Patent)-[:HAS_TARGET_PAIR]->(tp:TargetPair)-[:HAS_TARGET]->(:Target)-[:FUNCTIONED_AS]->(f:Functional_of_Target)
    RETURN
      f.name AS function_raw,
      count(DISTINCT p) AS patent_count,
      count(DISTINCT tp) AS targetpair_count
    ORDER BY patent_count DESC, function_raw ASC
    """

    pair_query = """
    MATCH (p:Patent)-[:HAS_TARGET_PAIR]->(tp:TargetPair)-[:HAS_TARGET]->(:Target)-[:FUNCTIONED_AS]->(f:Functional_of_Target)
    WITH p, tp, collect(DISTINCT f.name) AS fs
    WHERE size(fs) >= 2
    UNWIND range(0, size(fs) - 2) AS i
    UNWIND range(i + 1, size(fs) - 1) AS j
    WITH
      p,
      tp,
      CASE WHEN fs[i] < fs[j] THEN fs[i] ELSE fs[j] END AS source_raw,
      CASE WHEN fs[i] < fs[j] THEN fs[j] ELSE fs[i] END AS target_raw
    RETURN
      source_raw,
      target_raw,
      count(DISTINCT tp) AS targetpair_count,
      count(DISTINCT p) AS patent_count
    ORDER BY targetpair_count DESC, patent_count DESC, source_raw ASC, target_raw ASC
    """

    total_query = """
    MATCH (p:Patent)-[:HAS_TARGET_PAIR]->(:TargetPair)-[:HAS_TARGET]->(:Target)-[:FUNCTIONED_AS]->(:Functional_of_Target)
    RETURN count(DISTINCT p) AS patents_with_function
    """

    try:
        with driver.session(database=neo["database"]) as session:
            function_rows = [dict(r) for r in session.run(function_query)]
            pair_rows = [dict(r) for r in session.run(pair_query)]
            meta = dict(session.run(total_query).single())
    finally:
        driver.close()

    patents_with_function = int(meta["patents_with_function"])
    total_function_assignments = sum(int(r["patent_count"]) for r in function_rows)

    full_functions: list[dict[str, object]] = []
    for row in function_rows:
        patent_count = int(row["patent_count"])
        targetpair_count = int(row["targetpair_count"])
        raw = str(row["function_raw"])
        full_functions.append(
            {
                "function_raw": raw,
                "function_display": display_label(raw),
                "patent_count": patent_count,
                "targetpair_count": targetpair_count,
                "share_of_patents_pct": round(100.0 * patent_count / patents_with_function, 2),
                "share_of_assignments_pct": round(100.0 * patent_count / total_function_assignments, 2),
            }
        )

    full_pairs: list[dict[str, object]] = []
    for row in pair_rows:
        source_raw = str(row["source_raw"])
        target_raw = str(row["target_raw"])
        full_pairs.append(
            {
                "source_raw": source_raw,
                "source_display": display_label(source_raw),
                "target_raw": target_raw,
                "target_display": display_label(target_raw),
                "targetpair_count": int(row["targetpair_count"]),
                "patent_count": int(row["patent_count"]),
            }
        )

    return full_functions, full_pairs, {
        "patents_with_function": patents_with_function,
        "total_function_assignments": total_function_assignments,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def select_bar_subset(function_rows: list[dict[str, object]], top_n: int) -> list[dict[str, object]]:
    return function_rows[:top_n]


def select_chord_subset(
    function_rows: list[dict[str, object]],
    pair_rows: list[dict[str, object]],
    top_n_nodes: int,
    max_edges: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    node_rows = function_rows[:top_n_nodes]
    selected = {str(r["function_raw"]) for r in node_rows}
    edge_rows = [
        row for row in pair_rows
        if str(row["source_raw"]) in selected and str(row["target_raw"]) in selected
    ][:max_edges]
    connected = {str(r["source_raw"]) for r in edge_rows} | {str(r["target_raw"]) for r in edge_rows}
    node_rows = [row for row in node_rows if str(row["function_raw"]) in connected]
    return node_rows, edge_rows


def measure(draw: ImageDraw.ImageDraw, text: str, text_font) -> tuple[int, int]:
    box = draw.multiline_textbbox((0, 0), text, font=text_font, spacing=4)
    return box[2] - box[0], box[3] - box[1]


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


def create_bar_png(rows: list[dict[str, object]], meta: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 3200, 2100
    plot_left, plot_right = 1050, 2920
    plot_top, plot_bottom = 150, 1730
    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    axis_font = font(38, True)
    label_font = font(29, False)
    tick_font = font(26, False)
    value_font = font(28, False)
    note_font = font(22, False)

    max_value = max(int(r["patent_count"]) for r in rows)
    tick_step = 2000 if max_value > 7000 else 1000
    axis_max = int(math.ceil(max_value / tick_step) * tick_step)

    for value in range(0, axis_max + tick_step, tick_step):
        x = plot_left + (value / axis_max) * (plot_right - plot_left)
        draw.line((x, plot_top, x, plot_bottom), fill="#E3E3E3", width=2)
        label = f"{value:,}"
        w, h = measure(draw, label, tick_font)
        draw.text((x - w / 2, plot_bottom + 18), label, font=tick_font, fill="#404040")

    draw.line((plot_left, plot_top, plot_left, plot_bottom), fill="#303030", width=4)
    draw.line((plot_left, plot_bottom, plot_right, plot_bottom), fill="#303030", width=4)

    gap = (plot_bottom - plot_top) / len(rows)
    bar_h = gap * 0.62
    for idx, row in enumerate(rows):
        y = plot_top + idx * gap + gap / 2
        y0, y1 = y - bar_h / 2, y + bar_h / 2
        x1 = plot_left + (int(row["patent_count"]) / axis_max) * (plot_right - plot_left)
        color = PALETTE[idx % len(PALETTE)]
        draw.rounded_rectangle((plot_left, y0, x1, y1), radius=8, fill=color)
        label = wrap_label(draw, str(row["function_display"]), 920, label_font)
        box = draw.multiline_textbbox((0, 0), label, font=label_font, spacing=3)
        draw.multiline_text((plot_left - 28 - (box[2] - box[0]), y - (box[3] - box[1]) / 2), label, font=label_font, fill="#202020", spacing=3)
        share = float(row["share_of_patents_pct"])
        text = f"{int(row['patent_count']):,} ({share:.1f}%)"
        draw.text((x1 + 14, y - measure(draw, text, value_font)[1] / 2), text, font=value_font, fill="#202020")

    draw.text((plot_left + 640, plot_bottom + 90), "Patent publications", font=axis_font, fill="#202020")
    note = (
        f"Top {len(rows)} functions by distinct patent count. Full dataset contains 55 functions. "
        f"Percentages use {meta['patents_with_function']:,} patents with functional annotation as denominator."
    )
    draw.multiline_text((70, height - 60), note, font=note_font, fill="#666666", spacing=4)
    image.save(output_path, dpi=(600, 600))


def create_bar_svg(rows: list[dict[str, object]], meta: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 3200, 2100
    plot_left, plot_right = 1050, 2920
    plot_top, plot_bottom = 150, 1730
    max_value = max(int(r["patent_count"]) for r in rows)
    tick_step = 2000 if max_value > 7000 else 1000
    axis_max = int(math.ceil(max_value / tick_step) * tick_step)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]

    for value in range(0, axis_max + tick_step, tick_step):
        x = plot_left + (value / axis_max) * (plot_right - plot_left)
        parts.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_bottom}" stroke="#E3E3E3" stroke-width="2"/>')
        parts.append(f'<text x="{x:.1f}" y="{plot_bottom + 46}" font-family="Times New Roman" font-size="26" fill="#404040" text-anchor="middle">{value:,}</text>')

    parts.extend(
        [
            f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
            f'<line x1="{plot_left}" y1="{plot_bottom}" x2="{plot_right}" y2="{plot_bottom}" stroke="#303030" stroke-width="4"/>',
        ]
    )

    gap = (plot_bottom - plot_top) / len(rows)
    bar_h = gap * 0.62
    for idx, row in enumerate(rows):
        y = plot_top + idx * gap + gap / 2
        y0 = y - bar_h / 2
        x1 = plot_left + (int(row["patent_count"]) / axis_max) * (plot_right - plot_left)
        color = PALETTE[idx % len(PALETTE)]
        parts.append(f'<rect x="{plot_left}" y="{y0:.1f}" width="{x1 - plot_left:.1f}" height="{bar_h:.1f}" rx="8" ry="8" fill="{color}"/>')
        parts.append(f'<text x="{plot_left - 28}" y="{y + 8:.1f}" font-family="Times New Roman" font-size="29" fill="#202020" text-anchor="end">{html.escape(str(row["function_display"]))}</text>')
        parts.append(f'<text x="{x1 + 14:.1f}" y="{y + 8:.1f}" font-family="Times New Roman" font-size="28" fill="#202020">{int(row["patent_count"]):,} ({float(row["share_of_patents_pct"]):.1f}%)</text>')

    parts.append(f'<text x="{plot_left + 640}" y="{plot_bottom + 95}" font-family="Times New Roman" font-size="38" font-weight="bold" fill="#202020">Patent publications</text>')
    note = (
        f"Top {len(rows)} functions by distinct patent count. Full dataset contains 55 functions. "
        f"Percentages use {meta['patents_with_function']:,} patents with functional annotation as denominator."
    )
    parts.append(f'<text x="70" y="{height - 36}" font-family="Times New Roman" font-size="22" fill="#666666">{html.escape(note)}</text>')
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def bezier_points(p0, p1, p2, p3, steps=64):
    pts = []
    for i in range(steps + 1):
        t = i / steps
        x = (
            (1 - t) ** 3 * p0[0]
            + 3 * (1 - t) ** 2 * t * p1[0]
            + 3 * (1 - t) * t ** 2 * p2[0]
            + t ** 3 * p3[0]
        )
        y = (
            (1 - t) ** 3 * p0[1]
            + 3 * (1 - t) ** 2 * t * p1[1]
            + 3 * (1 - t) * t ** 2 * p2[1]
            + t ** 3 * p3[1]
        )
        pts.append((x, y))
    return pts


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = color.lstrip("#")
    return tuple(int(color[i:i + 2], 16) for i in (0, 2, 4))


def blend_colors(color_a: str, color_b: str, weight: float = 0.5) -> str:
    ra, ga, ba = hex_to_rgb(color_a)
    rb, gb, bb = hex_to_rgb(color_b)
    r = int(ra * (1 - weight) + rb * weight)
    g = int(ga * (1 - weight) + gb * weight)
    b = int(ba * (1 - weight) + bb * weight)
    return f"#{r:02X}{g:02X}{b:02X}"


def polar_point(center, radius, angle_deg):
    angle = math.radians(angle_deg)
    return (center[0] + math.cos(angle) * radius, center[1] + math.sin(angle) * radius)


def arc_points(center, radius, start_deg, end_deg, steps=24):
    if end_deg < start_deg:
        start_deg, end_deg = end_deg, start_deg
    if abs(end_deg - start_deg) < 1e-6:
        return [polar_point(center, radius, start_deg)]
    return [polar_point(center, radius, start_deg + (end_deg - start_deg) * i / steps) for i in range(steps + 1)]


def ribbon_polygon(source_span, target_span, center, radius, control_radius, steps_arc=14, steps_curve=48):
    s0, s1 = source_span
    t0, t1 = target_span
    source_arc = arc_points(center, radius, s0, s1, steps_arc)
    target_arc = arc_points(center, radius, t0, t1, steps_arc)

    c1 = polar_point(center, control_radius, s1)
    c2 = polar_point(center, control_radius, t0)
    curve_1 = bezier_points(source_arc[-1], c1, c2, target_arc[0], steps_curve)

    c3 = polar_point(center, control_radius, t1)
    c4 = polar_point(center, control_radius, s0)
    curve_2 = bezier_points(target_arc[-1], c3, c4, source_arc[0], steps_curve)

    polygon = source_arc + curve_1[1:] + list(reversed(target_arc))[1:] + curve_2[1:]
    return polygon


def chord_layout(nodes: list[dict[str, object]], edges: list[dict[str, object]]):
    node_order = [str(n["function_raw"]) for n in nodes]
    node_totals = {raw: 0 for raw in node_order}
    for edge in edges:
        node_totals[str(edge["source_raw"])] += int(edge["targetpair_count"])
        node_totals[str(edge["target_raw"])] += int(edge["targetpair_count"])

    total_weight = sum(node_totals.values()) or 1
    gap_deg = 4.0
    total_angle = 360.0 - gap_deg * len(node_order)
    start = -90.0
    node_spans = {}
    for raw in node_order:
        span = total_angle * node_totals[raw] / total_weight
        node_spans[raw] = (start, start + span)
        start += span + gap_deg

    edge_alloc = {}
    for raw in node_order:
        incident = []
        for edge in edges:
            if str(edge["source_raw"]) == raw:
                other = str(edge["target_raw"])
                incident.append((node_order.index(other), edge, "source"))
            elif str(edge["target_raw"]) == raw:
                other = str(edge["source_raw"])
                incident.append((node_order.index(other), edge, "target"))
        incident.sort(key=lambda item: item[0])
        span_start, span_end = node_spans[raw]
        cursor = span_start
        node_total = node_totals[raw] or 1
        for _, edge, side in incident:
            weight = int(edge["targetpair_count"])
            delta = (span_end - span_start) * weight / node_total
            edge_alloc[(id(edge), side)] = (cursor, cursor + delta)
            cursor += delta

    return node_spans, edge_alloc, node_totals


def create_chord_png(nodes: list[dict[str, object]], edges: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 3200, 2300
    center = (1060, 1100)
    outer_r = 620
    ring_w = 72
    image = Image.new("RGBA", (width, height), (255, 255, 255, 255))
    draw = ImageDraw.Draw(image, "RGBA")

    label_font = font(27, False)
    note_font = font(22, False)
    count_font = font(26, False)
    inner_r = outer_r - ring_w
    ribbon_r = inner_r - 18
    control_r = 170

    node_spans, edge_alloc, node_totals = chord_layout(nodes, edges)
    color_map = {}
    for idx, node in enumerate(nodes):
        raw = str(node["function_raw"])
        color_map[raw] = PALETTE[idx % len(PALETTE)]

    for edge in reversed(edges):
        source_raw = str(edge["source_raw"])
        target_raw = str(edge["target_raw"])
        source_span = edge_alloc[(id(edge), "source")]
        target_span = edge_alloc[(id(edge), "target")]
        polygon = ribbon_polygon(source_span, target_span, center, ribbon_r, control_r)
        fill = blend_colors(color_map[source_raw], color_map[target_raw], 0.5)
        draw.polygon(polygon, fill=(*hex_to_rgb(fill), 120))

    for raw, (start_angle, end_angle) in node_spans.items():
        draw.pieslice(
            (center[0] - outer_r, center[1] - outer_r, center[0] + outer_r, center[1] + outer_r),
            start=start_angle,
            end=end_angle,
            fill=color_map[raw],
            outline="white",
            width=4,
        )
        draw.pieslice(
            (center[0] - inner_r, center[1] - inner_r, center[0] + inner_r, center[1] + inner_r),
            start=start_angle,
            end=end_angle,
            fill=(255, 255, 255, 255),
            outline="white",
            width=1,
        )

        mid = (start_angle + end_angle) / 2
        label_pt = polar_point(center, outer_r + 90, mid)
        count_pt = polar_point(center, outer_r + 36, mid)
        align = "left" if math.cos(math.radians(mid)) >= 0 else "right"
        display = next(str(n["function_display"]) for n in nodes if str(n["function_raw"]) == raw)
        label_w, label_h = measure(draw, display, label_font)
        count_w, count_h = measure(draw, f"{node_totals[raw]}", count_font)
        label_x = label_pt[0] if align == "left" else label_pt[0] - label_w
        count_x = count_pt[0] - count_w / 2
        draw.text((count_x, count_pt[1] - count_h / 2), f"{node_totals[raw]}", font=count_font, fill="#202020")
        draw.multiline_text((label_x, label_pt[1] - label_h / 2), display, font=label_font, fill=color_map[raw], spacing=3)

    legend_x = 1930
    legend_y = 250
    for idx, node in enumerate(nodes):
        raw = str(node["function_raw"])
        y = legend_y + idx * 140
        color = color_map[raw]
        draw.rounded_rectangle((legend_x, y, legend_x + 36, y + 36), radius=5, fill=color)
        total_incident = node_totals[raw]
        pct = 100.0 * total_incident / (sum(node_totals.values()) or 1)
        text = f"{node['function_display']}\n{total_incident:,} pair-links ({pct:.1f}%)"
        draw.multiline_text((legend_x + 56, y - 2), text, font=label_font, fill="#202020", spacing=4)

    note = (
        f"Weighted chord diagram for the top {len(nodes)} functions. "
        f"Ribbon width and sector width both encode distinct target-pair counts within the selected top {len(edges)} combinations."
    )
    draw.multiline_text((80, height - 60), note, font=note_font, fill="#666666", spacing=4)
    Image.alpha_composite(Image.new("RGBA", (width, height), (255, 255, 255, 255)), image).convert("RGB").save(output_path, dpi=(600, 600))


def create_chord_svg(nodes: list[dict[str, object]], edges: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 3200, 2300
    center = (1060, 1100)
    outer_r = 620
    ring_w = 72
    inner_r = outer_r - ring_w
    ribbon_r = inner_r - 18
    control_r = 170
    node_spans, edge_alloc, node_totals = chord_layout(nodes, edges)
    color_map = {}

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]

    for idx, node in enumerate(nodes):
        raw = str(node["function_raw"])
        color_map[raw] = PALETTE[idx % len(PALETTE)]

    for edge in reversed(edges):
        s = str(edge["source_raw"])
        t = str(edge["target_raw"])
        source_span = edge_alloc[(id(edge), "source")]
        target_span = edge_alloc[(id(edge), "target")]
        fill = blend_colors(color_map[s], color_map[t], 0.5)
        parts.append(
            f'<path d="{ribbon_path_svg(source_span, target_span, center, ribbon_r, control_r)}" '
            f'fill="{fill}" fill-opacity="0.45" stroke="none"/>'
        )

    for raw, (start_angle, end_angle) in node_spans.items():
        parts.append(
            f'<path d="{donut_segment_path(center, outer_r, inner_r, start_angle, end_angle)}" fill="{color_map[raw]}" stroke="white" stroke-width="3"/>'
        )
        mid = (start_angle + end_angle) / 2
        label_pt = polar_point(center, outer_r + 90, mid)
        count_pt = polar_point(center, outer_r + 38, mid)
        anchor = "start" if math.cos(math.radians(mid)) >= 0 else "end"
        display = next(str(n["function_display"]) for n in nodes if str(n["function_raw"]) == raw)
        parts.append(f'<text x="{count_pt[0]:.1f}" y="{count_pt[1] + 8:.1f}" font-family="Times New Roman" font-size="26" fill="#202020" text-anchor="middle">{node_totals[raw]}</text>')
        parts.append(f'<text x="{label_pt[0]:.1f}" y="{label_pt[1] + 8:.1f}" font-family="Times New Roman" font-size="27" fill="{color_map[raw]}" text-anchor="{anchor}">{html.escape(display)}</text>')

    legend_x = 1930
    legend_y = 250
    for idx, node in enumerate(nodes):
        raw = str(node["function_raw"])
        y = legend_y + idx * 140
        total_incident = node_totals[raw]
        pct = 100.0 * total_incident / (sum(node_totals.values()) or 1)
        parts.append(f'<rect x="{legend_x}" y="{y}" width="36" height="36" rx="5" ry="5" fill="{color_map[raw]}"/>')
        parts.append(f'<text x="{legend_x + 56}" y="{y + 26}" font-family="Times New Roman" font-size="27" fill="#202020">{html.escape(str(node["function_display"]))}</text>')
        parts.append(f'<text x="{legend_x + 56}" y="{y + 60}" font-family="Times New Roman" font-size="27" fill="#202020">{total_incident:,} pair-links ({pct:.1f}%)</text>')

    note = (
        f"Weighted chord diagram for the top {len(nodes)} functions. "
        f"Ribbon width and sector width both encode distinct target-pair counts within the selected top {len(edges)} combinations."
    )
    parts.append(f'<text x="80" y="{height - 34}" font-family="Times New Roman" font-size="22" fill="#666666">{html.escape(note)}</text>')
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def donut_segment_path(center, outer_r, inner_r, start_deg, end_deg):
    start = math.radians(start_deg)
    end = math.radians(end_deg)
    x1 = center[0] + math.cos(start) * outer_r
    y1 = center[1] + math.sin(start) * outer_r
    x2 = center[0] + math.cos(end) * outer_r
    y2 = center[1] + math.sin(end) * outer_r
    x3 = center[0] + math.cos(end) * inner_r
    y3 = center[1] + math.sin(end) * inner_r
    x4 = center[0] + math.cos(start) * inner_r
    y4 = center[1] + math.sin(start) * inner_r
    large = 1 if (end_deg - start_deg) > 180 else 0
    return (
        f"M {x1:.1f},{y1:.1f} "
        f"A {outer_r},{outer_r} 0 {large},1 {x2:.1f},{y2:.1f} "
        f"L {x3:.1f},{y3:.1f} "
        f"A {inner_r},{inner_r} 0 {large},0 {x4:.1f},{y4:.1f} Z"
    )


def ribbon_path_svg(source_span, target_span, center, radius, control_radius):
    s0, s1 = source_span
    t0, t1 = target_span
    s0p = polar_point(center, radius, s0)
    s1p = polar_point(center, radius, s1)
    t0p = polar_point(center, radius, t0)
    t1p = polar_point(center, radius, t1)
    c1 = polar_point(center, control_radius, s1)
    c2 = polar_point(center, control_radius, t0)
    c3 = polar_point(center, control_radius, t1)
    c4 = polar_point(center, control_radius, s0)
    large_s = 1 if (s1 - s0) > 180 else 0
    large_t = 1 if (t1 - t0) > 180 else 0
    return (
        f"M {s0p[0]:.1f},{s0p[1]:.1f} "
        f"A {radius},{radius} 0 {large_s},1 {s1p[0]:.1f},{s1p[1]:.1f} "
        f"C {c1[0]:.1f},{c1[1]:.1f} {c2[0]:.1f},{c2[1]:.1f} {t0p[0]:.1f},{t0p[1]:.1f} "
        f"A {radius},{radius} 0 {large_t},1 {t1p[0]:.1f},{t1p[1]:.1f} "
        f"C {c3[0]:.1f},{c3[1]:.1f} {c4[0]:.1f},{c4[1]:.1f} {s0p[0]:.1f},{s0p[1]:.1f} Z"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Functional_of_Target data and generate bar/chord-like figures.")
    parser.add_argument("--settings", default="bsab_kg_qa_en/config/settings.yaml")
    parser.add_argument("--functions-csv", default="docs/bsab_patent_landscape/data/functional_of_target_patent_counts_full.csv")
    parser.add_argument("--pairs-csv", default="docs/bsab_patent_landscape/data/functional_of_target_pair_counts_full.csv")
    parser.add_argument("--bar-subset-csv", default="docs/bsab_patent_landscape/data/functional_of_target_bar_subset.csv")
    parser.add_argument("--chord-nodes-csv", default="docs/bsab_patent_landscape/data/functional_of_target_chord_nodes_subset.csv")
    parser.add_argument("--chord-edges-csv", default="docs/bsab_patent_landscape/data/functional_of_target_chord_edges_subset.csv")
    parser.add_argument("--bar-png", default="docs/bsab_patent_landscape/figures/functional_of_target_bar.png")
    parser.add_argument("--bar-svg", default="docs/bsab_patent_landscape/figures/functional_of_target_bar.svg")
    parser.add_argument("--chord-png", default="docs/bsab_patent_landscape/figures/functional_of_target_chord.png")
    parser.add_argument("--chord-svg", default="docs/bsab_patent_landscape/figures/functional_of_target_chord.svg")
    args = parser.parse_args()

    full_functions, full_pairs, meta = fetch_data(args.settings)
    bar_subset = select_bar_subset(full_functions, TOP_N_BAR)
    chord_nodes, chord_edges = select_chord_subset(full_functions, full_pairs, TOP_N_CHORD, MAX_CHORD_EDGES)

    write_csv(ROOT / args.functions_csv, full_functions)
    write_csv(ROOT / args.pairs_csv, full_pairs)
    write_csv(ROOT / args.bar_subset_csv, bar_subset)
    write_csv(ROOT / args.chord_nodes_csv, chord_nodes)
    write_csv(ROOT / args.chord_edges_csv, chord_edges)

    create_bar_png(bar_subset, meta, ROOT / args.bar_png)
    create_bar_svg(bar_subset, meta, ROOT / args.bar_svg)
    create_chord_png(chord_nodes, chord_edges, ROOT / args.chord_png)
    create_chord_svg(chord_nodes, chord_edges, ROOT / args.chord_svg)

    print(f"Full function CSV exported to: {ROOT / args.functions_csv}")
    print(f"Full pair CSV exported to: {ROOT / args.pairs_csv}")
    print(f"Bar subset CSV exported to: {ROOT / args.bar_subset_csv}")
    print(f"Chord node CSV exported to: {ROOT / args.chord_nodes_csv}")
    print(f"Chord edge CSV exported to: {ROOT / args.chord_edges_csv}")
    print(f"Bar PNG exported to: {ROOT / args.bar_png}")
    print(f"Bar SVG exported to: {ROOT / args.bar_svg}")
    print(f"Chord PNG exported to: {ROOT / args.chord_png}")
    print(f"Chord SVG exported to: {ROOT / args.chord_svg}")


if __name__ == "__main__":
    main()
