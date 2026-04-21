from __future__ import annotations

from pathlib import Path
from typing import Iterable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from PIL import ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "generated_ppt_assets"
PNG_PATH = OUT_DIR / "three_current_paths.png"
SVG_PATH = OUT_DIR / "three_current_paths.svg"

W, H = 2000, 1180
BG = "#F6F8FB"
NAVY = "#1D2B44"
MUTED = "#4F607A"
LINE = "#8FA2BD"
ARROW = "#6E7F99"

COLORS = {
    "data": ("#DCEAFB", "#8FA9CF"),
    "auto": ("#FFF0BF", "#D1B870"),
    "hybrid": ("#DDF4E4", "#8CB89A"),
    "frame": ("#F6D9EA", "#C89DB8"),
    "output": ("#E4D9FA", "#A693D2"),
    "callout": ("#FFE4CB", "#D7A26E"),
}


def _font(candidates: Iterable[str], size: int):
    from PIL import ImageFont

    for name in candidates:
        path = Path("C:/Windows/Fonts") / name
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


def rounded_box(draw, xy: Tuple[int, int, int, int], fill: str, outline: str, radius: int = 28) -> None:
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=3)


def draw_multiline(
    draw,
    x: int,
    y: int,
    lines: Iterable[str],
    font,
    fill: str,
    line_gap: int = 10,
) -> int:
    cur_y = y
    for line in lines:
        draw.text((x, cur_y), line, font=font, fill=fill)
        bbox = draw.textbbox((x, cur_y), line, font=font)
        cur_y = bbox[3] + line_gap
    return cur_y


def arrow(draw, start: Tuple[int, int], end: Tuple[int, int], width: int = 8) -> None:
    draw.line([start, end], fill=ARROW, width=width)
    ex, ey = end
    size = 18
    draw.polygon([(ex, ey), (ex - size * 2, ey - size), (ex - size * 2, ey + size)], fill=ARROW)


def write_svg() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    boxes = {
        "data": (70, 170, 470, 930),
        "auto": (560, 240, 980, 760),
        "hybrid": (1020, 240, 1440, 760),
        "frame": (1480, 240, 1900, 760),
        "output": (700, 865, 1270, 1060),
        "callout": (1295, 840, 1890, 1080),
    }

    def rect(box, fill, stroke):
        x1, y1, x2, y2 = box
        return f'<rect x="{x1}" y="{y1}" width="{x2-x1}" height="{y2-y1}" rx="28" ry="28" fill="{fill}" stroke="{stroke}" stroke-width="3"/>'

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{BG}"/>',
        '<style>',
        ".title { font: 700 52px 'Microsoft YaHei','Times New Roman'; fill: #1D2B44; }",
        ".subtitle { font: 24px 'Times New Roman','Microsoft YaHei'; fill: #4F607A; }",
        ".boxTitle { font: 700 30px 'Times New Roman','Microsoft YaHei'; fill: #1D2B44; }",
        ".body { font: 23px 'Microsoft YaHei','Times New Roman'; fill: #1D2B44; }",
        ".small { font: 20px 'Times New Roman','Microsoft YaHei'; fill: #4F607A; }",
        ".call { font: 700 26px 'Microsoft YaHei','Times New Roman'; fill: #1D2B44; }",
        '</style>',
        '<text x="72" y="88" class="title">从 patent landscape 到 patent intelligence 的三条当前实现路径</text>',
        '<text x="74" y="126" class="subtitle">Shared data layer -> three reasoning routes -> evidence-grounded answer and benchmark</text>',
    ]

    svg.append(rect(boxes["data"], COLORS["data"][0], COLORS["data"][1]))
    svg.append(rect(boxes["auto"], COLORS["auto"][0], COLORS["auto"][1]))
    svg.append(rect(boxes["hybrid"], COLORS["hybrid"][0], COLORS["hybrid"][1]))
    svg.append(rect(boxes["frame"], COLORS["frame"][0], COLORS["frame"][1]))
    svg.append(rect(boxes["output"], COLORS["output"][0], COLORS["output"][1]))
    svg.append(rect(boxes["callout"], COLORS["callout"][0], COLORS["callout"][1]))

    svg.extend(
        [
            '<line x1="470" y1="500" x2="540" y2="500" stroke="#6E7F99" stroke-width="8"/>',
            '<polygon points="540,500 504,482 504,518" fill="#6E7F99"/>',
            '<line x1="470" y1="548" x2="1000" y2="548" stroke="#6E7F99" stroke-width="0"/>',
            '<line x1="980" y1="500" x2="1010" y2="500" stroke="#6E7F99" stroke-width="8"/>',
            '<polygon points="1010,500 974,482 974,518" fill="#6E7F99"/>',
            '<line x1="1440" y1="500" x2="1470" y2="500" stroke="#6E7F99" stroke-width="8"/>',
            '<polygon points="1470,500 1434,482 1434,518" fill="#6E7F99"/>',
            '<line x1="770" y1="760" x2="930" y2="850" stroke="#6E7F99" stroke-width="8"/>',
            '<polygon points="930,850 892,850 910,818" fill="#6E7F99"/>',
            '<line x1="1230" y1="760" x2="1080" y2="850" stroke="#6E7F99" stroke-width="8"/>',
            '<polygon points="1080,850 1098,818 1118,850" fill="#6E7F99"/>',
            '<line x1="1680" y1="760" x2="1585" y2="840" stroke="#6E7F99" stroke-width="8"/>',
            '<polygon points="1585,840 1605,806 1622,842" fill="#6E7F99"/>',
        ]
    )

    def text_block(x: int, y: int, title: str, body_lines: list[str], small_lines: list[str] | None = None) -> None:
        svg.append(f'<text x="{x}" y="{y}" class="boxTitle">{title}</text>')
        cy = y + 55
        for line in body_lines:
            svg.append(f'<text x="{x}" y="{cy}" class="body">{line}</text>')
            cy += 42
        if small_lines:
            cy += 6
            for line in small_lines:
                svg.append(f'<text x="{x}" y="{cy}" class="small">{line}</text>')
                cy += 34

    text_block(
        102,
        228,
        "共享数据底座",
        [
            "Curated patent dataset",
            "Neo4j KG + NodeCatalog",
            "Target / TargetPair / Assignee / Origin",
            "Pathway / Function / TechnologyClass1",
            "Intent JSON + benchmark datasets",
        ],
        [
            "source of truth:",
            "tests/scripts + _curated_20260401",
        ],
    )

    text_block(
        590,
        290,
        "Path A  AutoCypher",
        [
            "official Text2Cypher",
            "v1 candidate ranking",
            "v2 one-shot baseline",
            "v3 repair with execution feedback",
        ],
        [
            "role: baseline / fallback / ablation",
            "Dataset5: 143 match | 183 partial | 22 mismatch | 2 no_cypher",
        ],
    )

    text_block(
        1050,
        290,
        "Path B  Hybrid Intent",
        [
            "intent classification",
            "entity extraction + normalization",
            "template Cypher first",
            "AutoCypher fallback when needed",
        ],
        [
            "role: current deployable path",
            "Dataset5: 115 match | 198 partial | 32 mismatch | 5 no_cypher",
        ],
    )

    text_block(
        1510,
        290,
        "Path C  Query Frame few-shot",
        [
            "select FRAME_* question pattern",
            "slot filling",
            "execute fixed Cypher frame",
            "answer from graph results",
        ],
        [
            "role: current best benchmark path",
            "Dataset5: 216 match | 126 partial | 8 mismatch",
        ],
    )

    text_block(
        735,
        920,
        "统一输出与评测层",
        [
            "KG-grounded answer",
            "Cypher + graph evidence",
            "audit workbook",
            "match / partial / mismatch / no_cypher",
        ],
    )

    svg.append('<text x="1330" y="895" class="call">当前结论</text>')
    for i, line in enumerate(
        [
            "Query Frame 当前效果最高，适合写成方法创新线。",
            "Hybrid 最适合持续系统化与在线维护。",
            "AutoCypher 必须保留，既是开放基线，也是 fallback 来源。",
        ]
    ):
        svg.append(f'<text x="1330" y="{948 + i*50}" class="body">{line}</text>')

    svg.append("</svg>")
    SVG_PATH.write_text("\n".join(svg), encoding="utf-8")


def write_png() -> None:
    from PIL import Image, ImageDraw

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    font_title = _font(["msyhbd.ttc", "simhei.ttf", "timesbd.ttf"], 52)
    font_subtitle = _font(["times.ttf", "msyh.ttc"], 24)
    font_box_title = _font(["timesbd.ttf", "msyhbd.ttc"], 30)
    font_box_body = _font(["msyh.ttc", "simsun.ttc", "times.ttf"], 23)
    font_small = _font(["times.ttf", "msyh.ttc"], 20)
    font_call = _font(["msyhbd.ttc", "timesbd.ttf"], 26)
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    draw.text((72, 52), "从 patent landscape 到 patent intelligence 的三条当前实现路径", font=font_title, fill=NAVY)
    draw.text(
        (74, 112),
        "Shared data layer -> three reasoning routes -> evidence-grounded answer and benchmark",
        font=font_subtitle,
        fill=MUTED,
    )

    boxes = {
        "data": (70, 170, 470, 930),
        "auto": (560, 240, 980, 760),
        "hybrid": (1020, 240, 1440, 760),
        "frame": (1480, 240, 1900, 760),
        "output": (700, 865, 1270, 1060),
        "callout": (1295, 840, 1890, 1080),
    }

    for key, box in boxes.items():
        rounded_box(draw, box, COLORS[key][0], COLORS[key][1])

    arrow(draw, (470, 500), (540, 500))
    arrow(draw, (980, 500), (1010, 500))
    arrow(draw, (1440, 500), (1470, 500))
    arrow(draw, (770, 760), (930, 850))
    arrow(draw, (1230, 760), (1080, 850))
    arrow(draw, (1680, 760), (1585, 840))

    def block(x1: int, y1: int, title: str, body: list[str], small: list[str] | None = None) -> None:
        draw.text((x1, y1), title, font=font_box_title, fill=NAVY)
        cy = draw_multiline(draw, x1, y1 + 55, body, font_box_body, NAVY, line_gap=10)
        if small:
            draw_multiline(draw, x1, cy + 8, small, font_small, MUTED, line_gap=8)

    block(
        102,
        228,
        "共享数据底座",
        [
            "Curated patent dataset",
            "Neo4j KG + NodeCatalog",
            "Target / TargetPair / Assignee / Origin",
            "Pathway / Function / TechnologyClass1",
            "Intent JSON + benchmark datasets",
        ],
        [
            "source of truth:",
            "tests/scripts + _curated_20260401",
        ],
    )
    block(
        590,
        290,
        "Path A  AutoCypher",
        [
            "official Text2Cypher",
            "v1 candidate ranking",
            "v2 one-shot baseline",
            "v3 repair with execution feedback",
        ],
        [
            "role: baseline / fallback / ablation",
            "Dataset5: 143 match | 183 partial | 22 mismatch | 2 no_cypher",
        ],
    )
    block(
        1050,
        290,
        "Path B  Hybrid Intent",
        [
            "intent classification",
            "entity extraction + normalization",
            "template Cypher first",
            "AutoCypher fallback when needed",
        ],
        [
            "role: current deployable path",
            "Dataset5: 115 match | 198 partial | 32 mismatch | 5 no_cypher",
        ],
    )
    block(
        1510,
        290,
        "Path C  Query Frame few-shot",
        [
            "select FRAME_* question pattern",
            "slot filling",
            "execute fixed Cypher frame",
            "answer from graph results",
        ],
        [
            "role: current best benchmark path",
            "Dataset5: 216 match | 126 partial | 8 mismatch",
        ],
    )
    block(
        735,
        920,
        "统一输出与评测层",
        [
            "KG-grounded answer",
            "Cypher + graph evidence",
            "audit workbook",
            "match / partial / mismatch / no_cypher",
        ],
    )

    draw.text((1330, 890), "当前结论", font=font_call, fill=NAVY)
    draw_multiline(
        draw,
        1330,
        938,
        [
            "Query Frame 当前效果最高，适合写成方法创新线。",
            "Hybrid 最适合持续系统化与在线维护。",
            "AutoCypher 必须保留，既是开放基线，也是 fallback 来源。",
        ],
        font_box_body,
        NAVY,
        line_gap=12,
    )

    img.save(PNG_PATH, format="PNG", optimize=True)


def main() -> None:
    write_svg()
    try:
        write_png()
    except Exception as exc:  # noqa: BLE001
        print(f"Skipped PNG export: {exc}")
    print(f"Saved SVG: {SVG_PATH}")
    if PNG_PATH.exists():
        print(f"Saved PNG: {PNG_PATH}")


if __name__ == "__main__":
    main()
