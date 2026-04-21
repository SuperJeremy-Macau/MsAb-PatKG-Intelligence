from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path

from neo4j import GraphDatabase
from PIL import Image, ImageDraw, ImageFont


WORKDIR = Path(__file__).resolve().parents[1]
OUTDIR = WORKDIR / "docs" / "bsab_patent_landscape"
FIGDIR = OUTDIR / "figures"
DATADIR = OUTDIR / "data"
MANUSCRIPT = OUTDIR / "bsab_patent_landscape_draft_zh.md"

URI = "neo4j+s://cc3f2e35.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "WBFA55g2Sf0n0tVJT4MTtCcNTkVFbXY8eNI17pXPTlw"


def ensure_dirs() -> None:
    FIGDIR.mkdir(parents=True, exist_ok=True)
    DATADIR.mkdir(parents=True, exist_ok=True)


def font(size: int, bold: bool = False):
    candidates = [
        ("C:/Windows/Fonts/msyhbd.ttc", bold),
        ("C:/Windows/Fonts/msyh.ttc", not bold),
        ("C:/Windows/Fonts/arialbd.ttf", bold),
        ("C:/Windows/Fonts/arial.ttf", not bold),
    ]
    for path, cond in candidates:
        if Path(path).exists() and cond:
            return ImageFont.truetype(path, size=size)
    for path, _ in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def wrap(draw: ImageDraw.ImageDraw, text: str, box, fnt, fill="#334155", line_gap=10):
    x1, y1, x2, y2 = box
    width = x2 - x1
    lines = []
    for raw in text.split("\n"):
        cur = ""
        for ch in raw:
            trial = cur + ch
            if draw.textlength(trial, font=fnt) <= width:
                cur = trial
            else:
                if cur:
                    lines.append(cur)
                cur = ch
        lines.append(cur)
    y = y1
    for line in lines:
        draw.text((x1, y), line, font=fnt, fill=fill)
        bbox = draw.textbbox((x1, y), line or " ", font=fnt)
        y += (bbox[3] - bbox[1]) + line_gap
        if y > y2:
            break


def query_all():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    queries = {
        "year_patents": "MATCH (p:Patent)-[:PUBLISHED_IN]->(y:Year) RETURN y.year AS year, count(DISTINCT p) AS patents ORDER BY year",
        "family_first_year": "MATCH (f:Family)-[:HAS_PATENT]->(p:Patent)-[:PUBLISHED_IN]->(y:Year) WITH f, min(y.year) AS year RETURN year, count(f) AS families ORDER BY year",
        "origin_year": "MATCH (p:Patent)-[:PUBLISHED_IN]->(y:Year) MATCH (p)-[:HAS_ASSIGNEE]->(:Assignee)-[:ORIGIN_FROM]->(o:Origin) RETURN y.year AS year, o.name AS origin, count(DISTINCT p) AS patents ORDER BY year, patents DESC",
        "origin_total": "MATCH (p:Patent)-[:HAS_ASSIGNEE]->(:Assignee)-[:ORIGIN_FROM]->(o:Origin) RETURN o.name AS origin, count(DISTINCT p) AS patents ORDER BY patents DESC",
        "top_assignees": "MATCH (a:Assignee)<-[:HAS_ASSIGNEE]-(p:Patent) RETURN a.name AS assignee, count(DISTINCT p) AS patents ORDER BY patents DESC LIMIT 15",
        "top_targetpairs": "MATCH (p:Patent)-[:HAS_TARGET_PAIR]->(tp:TargetPair) RETURN tp.name AS target_pair, count(DISTINCT p) AS patents ORDER BY patents DESC LIMIT 15",
        "targetpair_first_year": "MATCH (tp:TargetPair)<-[:HAS_TARGET_PAIR]-(p:Patent)-[:PUBLISHED_IN]->(y:Year) WITH tp, min(y.year) AS year RETURN year, count(tp) AS new_targetpairs ORDER BY year",
        "top_function_pairs": "MATCH (tp:TargetPair)-[:HAS_TARGET]->(t:Target)-[:FUNCTIONED_AS]->(f:Functional_of_Target) WITH tp, collect(DISTINCT f.name) AS fs WHERE size(fs)=2 WITH CASE WHEN fs[0] < fs[1] THEN fs[0] + ' / ' + fs[1] ELSE fs[1] + ' / ' + fs[0] END AS function_pair RETURN function_pair, count(*) AS targetpair_count ORDER BY targetpair_count DESC LIMIT 12",
        "summary_counts": "MATCH (p:Patent) WITH count(p) AS patents MATCH (f:Family) WITH patents, count(f) AS families MATCH (tp:TargetPair) WITH patents,families,count(tp) AS targetpairs MATCH (a:Assignee) WITH patents,families,targetpairs,count(a) AS assignees MATCH (t:Target) RETURN patents, families, targetpairs, assignees, count(t) AS targets",
    }
    out = {}
    with driver.session(database="neo4j") as s:
        for name, q in queries.items():
            out[name] = [dict(r) for r in s.run(q)]
    driver.close()
    return out


def save_csv(name: str, rows: list[dict]) -> None:
    if not rows:
        return
    path = DATADIR / f"{name}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def chart_canvas(title: str, subtitle: str = ""):
    img = Image.new("RGB", (1800, 1050), "#FFFFFF")
    draw = ImageDraw.Draw(img)
    draw.text((70, 40), title, font=font(42, True), fill="#0F172A")
    if subtitle:
        draw.text((70, 95), subtitle, font=font(22), fill="#475569")
    return img, draw


def draw_axes(draw, left, top, right, bottom):
    draw.line((left, top, left, bottom), fill="#475569", width=3)
    draw.line((left, bottom, right, bottom), fill="#475569", width=3)


def plot_line_bar_patents(year_patents, family_first_year, outpath):
    img, draw = chart_canvas("Figure 1. Annual patent publications and newly appearing families", "Patent counts by publication year; family counts by first observed publication year")
    left, top, right, bottom = 110, 170, 1690, 880
    draw_axes(draw, left, top, right, bottom)
    year_to_family = {r["year"]: r["families"] for r in family_first_year}
    years = [r["year"] for r in year_patents]
    pvals = [r["patents"] for r in year_patents]
    fvals = [year_to_family.get(y, 0) for y in years]
    pmax = max(pvals)
    fmax = max(fvals) if max(fvals) > 0 else 1
    step = (right - left) / max(1, len(years) - 1)
    bar_w = max(10, int(step * 0.55))
    prev = None
    for i, y in enumerate(years):
        x = left + i * step
        py = bottom - (pvals[i] / pmax) * (bottom - top - 40)
        if prev:
            draw.line((prev[0], prev[1], x, py), fill="#2563EB", width=5)
        draw.ellipse((x - 5, py - 5, x + 5, py + 5), fill="#2563EB")
        prev = (x, py)
        fh = (fvals[i] / fmax) * 220
        draw.rectangle((x - bar_w / 2, bottom - fh, x + bar_w / 2, bottom), fill="#F59E0B")
        if i % 4 == 0:
            draw.text((x - 20, bottom + 15), str(y), font=font(18), fill="#334155")
    for frac, val in [(0.0, 0), (0.25, round(pmax * 0.25),), (0.5, round(pmax * 0.5)), (0.75, round(pmax * 0.75)), (1.0, pmax)]:
        y = bottom - frac * (bottom - top - 40)
        draw.line((left - 8, y, left, y), fill="#475569", width=2)
        draw.text((20, y - 10), str(val), font=font(18), fill="#334155")
    wrap(draw, "Blue line: patent publications\nOrange bars: newly appearing families", (1290, 210, 1690, 320), font(20), "#334155", 6)
    img.save(outpath)


def plot_stacked_origin(origin_year_rows, outpath):
    img, draw = chart_canvas("Figure 2. Annual patent publications by assignee origin", "Stacked bars show annual patent counts grouped by ultimate assignee origin")
    left, top, right, bottom = 110, 170, 1690, 900
    draw_axes(draw, left, top, right, bottom)
    all_years = sorted({r["year"] for r in origin_year_rows})
    origins = ["US", "EU", "China", "JP", "UK", "KR", "Other", "Personal"]
    colors = {
        "US": "#2563EB", "EU": "#0EA5E9", "China": "#F97316", "JP": "#8B5CF6",
        "UK": "#10B981", "KR": "#F43F5E", "Other": "#94A3B8", "Personal": "#D97706"
    }
    table = defaultdict(lambda: defaultdict(int))
    for r in origin_year_rows:
        table[r["year"]][r["origin"]] = r["patents"]
    ymax = max(sum(table[y].values()) for y in all_years)
    step = (right - left) / len(all_years)
    bar_w = max(12, int(step * 0.72))
    for i, year in enumerate(all_years):
        x1 = left + i * step + (step - bar_w) / 2
        x2 = x1 + bar_w
        cum = 0
        total_h = bottom - top - 30
        for origin in origins:
            val = table[year].get(origin, 0)
            if val == 0:
                continue
            h = val / ymax * total_h
            draw.rectangle((x1, bottom - cum - h, x2, bottom - cum), fill=colors[origin], outline="#FFFFFF")
            cum += h
        if i % 3 == 0:
            draw.text((x1 - 6, bottom + 10), str(year), font=font(16), fill="#334155")
    legend_x = 1280
    legend_y = 220
    for origin in origins:
        draw.rectangle((legend_x, legend_y, legend_x + 26, legend_y + 18), fill=colors[origin])
        draw.text((legend_x + 36, legend_y - 4), origin, font=font(18), fill="#334155")
        legend_y += 34
    img.save(outpath)


def plot_hbar(rows, key, value, title, subtitle, outpath, color="#2563EB"):
    img, draw = chart_canvas(title, subtitle)
    left, top, right, bottom = 420, 170, 1700, 930
    maxv = max(r[value] for r in rows)
    count = len(rows)
    gap = (bottom - top) / count
    for i, r in enumerate(rows):
        y = top + i * gap + 12
        bar_len = (r[value] / maxv) * (right - left - 20)
        draw.rectangle((left, y, left + bar_len, y + 32), fill=color)
        draw.text((60, y - 2), str(r[key])[:40], font=font(20), fill="#0F172A")
        draw.text((left + bar_len + 14, y - 2), str(r[value]), font=font(20, True), fill="#334155")
    img.save(outpath)


def plot_new_targetpairs(rows, outpath):
    img, draw = chart_canvas("Figure 5. Emergence of new target pairs over time", "Bars: newly appearing target pairs; red line: cumulative distinct target pairs")
    left, top, right, bottom = 110, 170, 1690, 900
    draw_axes(draw, left, top, right, bottom)
    years = [r["year"] for r in rows]
    vals = [r["new_targetpairs"] for r in rows]
    cumulative = []
    s = 0
    for v in vals:
        s += v
        cumulative.append(s)
    vmax = max(vals)
    cmax = max(cumulative)
    step = (right - left) / len(years)
    bar_w = max(12, int(step * 0.68))
    prev = None
    for i, year in enumerate(years):
        x = left + i * step + step / 2
        h = vals[i] / vmax * (bottom - top - 40)
        draw.rectangle((x - bar_w / 2, bottom - h, x + bar_w / 2, bottom), fill="#0EA5E9")
        cy = bottom - cumulative[i] / cmax * (bottom - top - 40)
        if prev:
            draw.line((prev[0], prev[1], x, cy), fill="#DC2626", width=4)
        draw.ellipse((x - 4, cy - 4, x + 4, cy + 4), fill="#DC2626")
        prev = (x, cy)
        if i % 4 == 0:
            draw.text((x - 20, bottom + 10), str(year), font=font(16), fill="#334155")
    wrap(draw, "Red line = cumulative distinct target pairs", (1220, 210, 1690, 260), font(20), "#334155", 6)
    img.save(outpath)


def manuscript_text(data, fig_paths):
    summary = data["summary_counts"][0]
    years = data["year_patents"]
    peak = max(years, key=lambda x: x["patents"])
    year_2024 = next((r for r in years if r["year"] == 2024), None)
    year_2025 = next((r for r in years if r["year"] == 2025), None)
    origins = data["origin_total"]
    top_origin = origins[0]
    top_assignees = data["top_assignees"][:10]
    top_pairs = data["top_targetpairs"][:10]
    function_pairs = data["top_function_pairs"][:8]
    new_pairs = data["targetpair_first_year"]
    latest_new_pair_peak = max(new_pairs, key=lambda x: x["new_targetpairs"])
    return f"""# A patent landscape of bispecific antibodies: macro trends, assignee geography and target-pair evolution

## Abstract
Bispecific antibodies (BsAbs) have become one of the most active modalities in antibody therapeutics, with rapidly expanding target-pair strategies and increasing competition across companies and regions. To provide a structured view of this field, we analyzed a curated BsAb patent corpus represented in Neo4j and focused exclusively on patent-landscape dimensions rather than downstream knowledge-graph querying. The current corpus contains {summary['patents']} patent publications, {summary['families']} patent families, {summary['targetpairs']} distinct target pairs, {summary['assignees']} assignees and {summary['targets']} targets. Patent publications increased from sporadic filings in the late 1980s and 1990s to a sustained high-volume phase after 2017, reaching a peak of {peak['patents']} publications in {peak['year']}. At the assignee-origin level, the United States ({top_origin['patents']} publications), Europe and China were the three dominant sources of activity, together accounting for most of the current corpus. At the company level, Roche, Amgen, Regeneron and Johnson & Johnson were the most prolific assignees by publication count. At the target-pair level, both hematologic and immuno-oncology combinations were highly represented, including BCMA/CD3, CD20/CD3, CTLA4/PD-1 and LAG3/PD-1, while non-oncology and coagulation-related combinations also contributed substantial volume. The emergence of new target pairs accelerated markedly after 2015, indicating continued expansion of the BsAb design space. Functional annotation further showed that combinations linking malignant-cell-surface targets to T-cell engagement or co-stimulatory axes were among the most frequent structural design logics in the corpus. Together, these results provide a publication-ready macro-level patent landscape of the BsAb field and establish a curated foundation for subsequent mechanistic, competitive and translational analyses.

## Introduction
Bispecific antibodies have become a central direction in therapeutic antibody development because they can simultaneously engage two targets and thereby reshape pharmacology beyond what is achievable with conventional monospecific antibodies. Their use cases now span T-cell redirection, dual checkpoint modulation, tumor microenvironment remodeling, angiogenesis control, co-stimulation, cytokine modulation and other increasingly complex design logics. As a result, the BsAb field has moved from a relatively small set of classical formats and target combinations to a rapidly expanding innovation space with high biological diversity and intense patent competition.

Patent analysis is particularly important for understanding this field. Compared with publications and review articles, patents often capture innovation earlier and more systematically and are more directly connected to asset value realization, freedom-to-operate considerations and competitive positioning. For BsAbs, where many strategic decisions depend on target-pair novelty, crowding and ownership patterns, a high-quality patent landscape can provide an essential macro-level view of technical evolution, assignee competition and geographic concentration.

Here, we present a dedicated patent landscape analysis of bispecific antibodies based on a curated patent corpus. This analysis deliberately focuses on landscape-level questions rather than knowledge-graph or query-system aspects. Specifically, we summarize the temporal expansion of the field, the dynamics of patent families, the geographic origin of assignee activity, the leading corporate players, the most represented target pairs and the evolution of target-pair novelty and functional design logic.

## Results

### 1. The BsAb patent corpus shows rapid expansion after 2015
The curated corpus contained {summary['patents']} patent publications linked to {summary['families']} patent families, with a long low-volume early phase followed by clear acceleration in the 2010s. Annual publication counts remained low for many years but rose sharply after 2015 and entered a sustained high-volume period after 2017. The maximum publication count was observed in {peak['year']} ({peak['patents']} publications), followed closely by 2024 ({year_2024['patents'] if year_2024 else 'NA'} publications). By contrast, 2025 showed only {year_2025['patents'] if year_2025 else 'NA'} publications in the current corpus and should be interpreted cautiously because the latest year is likely affected by publication lag or an incomplete data cutoff. The steep increase after 2015 is consistent with the broader maturation of BsAb engineering platforms and the subsequent expansion of target-pair exploration.

![Figure 1](figures/{fig_paths['fig1']})

At the family level, newly appearing families also accumulated steadily over time, indicating that the expansion in publication counts was not solely a republication effect. The continuing entry of new families supports the view that the field is still generating fresh invention activity rather than only extending older portfolios.

### 2. The United States, Europe and China dominate assignee-origin activity
Assignee-origin analysis showed a highly concentrated geographic structure. The United States ranked first with {origins[0]['patents']} patent publications, followed by Europe ({origins[1]['patents']}) and China ({origins[2]['patents']}). Japan, the United Kingdom and South Korea contributed smaller but still visible volumes, whereas the \"Other\" and \"Personal\" categories accounted for relatively minor shares. Over time, the stacked annual distribution suggests that the recent growth phase has been driven mainly by the United States, Europe and China.

![Figure 2](figures/{fig_paths['fig2']})

This result indicates that the global BsAb patent landscape is not evenly distributed but instead centered on a limited number of major innovation ecosystems. From a strategic perspective, these geographies likely represent the most important competitive and collaborative arenas for BsAb development.

### 3. Corporate activity is concentrated in a limited set of leading assignees
Patent-publication counts were strongly concentrated among a small number of companies. Roche was the most prolific assignee ({top_assignees[0]['patents']} publications), followed by Amgen ({top_assignees[1]['patents']}), Regeneron ({top_assignees[2]['patents']}) and Johnson & Johnson ({top_assignees[3]['patents']}). Other prominent players included Sanofi, MacroGenics, Merus, Genmab and Bristol-Myers Squibb.

![Figure 3](figures/{fig_paths['fig3']})

This concentration is consistent with the high technological and translational barriers of BsAb development. The leading companies are mainly organizations with either long-standing antibody-engineering capabilities or strong immuno-oncology pipelines, suggesting that both platform maturity and disease-area strategy contribute to patent output.

### 4. The target-pair landscape is broad, with both immune-cell-engaging and checkpoint/modulatory combinations highly represented
The target-pair layer of the corpus was diverse, with {summary['targetpairs']} distinct target pairs identified. Among the most frequent target pairs were {', '.join([f"{x['target_pair']} ({x['patents']})" for x in top_pairs[:5]])}. Several of these combinations reflect major design logics in the field, including T-cell engagement against lineage or tumor antigens, checkpoint co-blockade and angiogenesis-related dual targeting.

![Figure 4](figures/{fig_paths['fig4']})

Notably, the top-ranked target pairs were not limited to one therapeutic theme. The coexistence of immune-engaging, checkpoint-related, angiogenesis-related and coagulation-related combinations indicates that the BsAb patent landscape is not a narrow immuno-oncology niche but a broader multi-domain innovation space.

### 5. New target-pair emergence accelerated during the recent expansion phase
To estimate the expansion of the target-combination design space, we counted target pairs by their first observed publication year. The annual number of newly appearing target pairs remained low in the early years but increased markedly in the later phase of the field. The highest annual introduction of new target pairs occurred in {latest_new_pair_peak['year']} ({latest_new_pair_peak['new_targetpairs']} new pairs), and the cumulative trajectory continued to rise over time.

![Figure 5](figures/{fig_paths['fig5']})

This pattern suggests that recent field growth has not simply been driven by repeated filings on a stable set of canonical target pairs. Instead, the design space itself has been expanding, supporting the idea that target-pair innovation remains an active driver of BsAb patenting.

### 6. Functional annotation reveals recurring design logics in target-pair construction
Functional annotation of target pairs showed that the most common combination logic was the pairing of malignant-cell-surface targets with T-cell-engagement targets ({function_pairs[0]['targetpair_count']} target pairs). Other high-frequency strategies included combining malignant-cell-surface targets with co-stimulatory signaling axes or growth-factor-receptor signaling targets, as well as pairing adaptive immune checkpoint targets with co-stimulatory or cytokine-ligand targets.

![Figure 6](figures/{fig_paths['fig6']})

These functional patterns provide a compact view of the biological logic behind the target-pair landscape. Rather than being randomly distributed, the observed combinations suggest recurrent strategic themes, especially tumor-directed immune engagement and dual immunomodulatory design.

## Discussion
This analysis highlights several features of the current BsAb patent landscape. First, the field has clearly transitioned from a low-volume exploratory phase to a mature, high-activity patenting phase. Second, the global landscape is dominated by a limited number of geographic origins and corporate players, indicating both concentration and competitive intensity. Third, the target-pair design space continues to expand, with newly emerging combinations contributing materially to recent activity. Fourth, the functional annotation of target pairs reveals recognizable design logics, implying that target-pair innovation follows partly structured biological strategies rather than purely opportunistic combination.

At the same time, several limitations should be acknowledged. The present analysis is publication-centric and therefore subject to publication lag, especially in the most recent years. Patent publications also do not map perfectly onto invention significance, legal scope or clinical impact. In addition, the quality of target-pair and functional analyses depends on the upstream curation and annotation rules used to normalize entities and remove noise. These caveats should be considered when interpreting fine-grained rankings. Nonetheless, the current corpus provides a strong macro-level view of the BsAb field and can serve as a foundation for more focused analyses of mechanism classes, disease-specific segments, assignee strategy and target-pair competition.

## Methods
This draft was generated from the currently curated BsAb Neo4j corpus and focuses only on patent-landscape dimensions. Patent-level, family-level, assignee-origin, target-pair and function-pair statistics were retrieved directly from the graph. Annual family counts were estimated using the first observed publication year of each family. Annual target-pair emergence was estimated using the first observed publication year of each distinct target pair. All figures were drawn directly from these aggregated outputs.

## Figure legends
**Figure 1.** Annual patent publications and newly appearing families.  
**Figure 2.** Annual patent publications by assignee origin.  
**Figure 3.** Top assignees ranked by patent-publication count.  
**Figure 4.** Top target pairs ranked by patent-publication count.  
**Figure 5.** Annual emergence of new target pairs and cumulative distinct target pairs.  
**Figure 6.** Top functional target-pair classes ranked by number of distinct target pairs.  
"""


def main():
    ensure_dirs()
    data = query_all()
    for name, rows in data.items():
        save_csv(name, rows)

    fig1 = FIGDIR / "figure1_annual_patents_families.png"
    fig2 = FIGDIR / "figure2_origin_over_time.png"
    fig3 = FIGDIR / "figure3_top_assignees.png"
    fig4 = FIGDIR / "figure4_top_targetpairs.png"
    fig5 = FIGDIR / "figure5_new_targetpairs.png"
    fig6 = FIGDIR / "figure6_function_pairs.png"

    plot_line_bar_patents(data["year_patents"], data["family_first_year"], fig1)
    plot_stacked_origin(data["origin_year"], fig2)
    plot_hbar(data["top_assignees"], "assignee", "patents", "Figure 3. Top assignees by patent-publication count", "Ranked by distinct patent publications", fig3, "#2563EB")
    plot_hbar(data["top_targetpairs"], "target_pair", "patents", "Figure 4. Top target pairs by patent-publication count", "Ranked by distinct patent publications", fig4, "#F97316")
    plot_new_targetpairs(data["targetpair_first_year"], fig5)
    plot_hbar(data["top_function_pairs"], "function_pair", "targetpair_count", "Figure 6. Top functional target-pair classes", "Counted by distinct target pairs", fig6, "#10B981")

    fig_paths = {
        "fig1": fig1.name,
        "fig2": fig2.name,
        "fig3": fig3.name,
        "fig4": fig4.name,
        "fig5": fig5.name,
        "fig6": fig6.name,
    }
    MANUSCRIPT.write_text(manuscript_text(data, fig_paths), encoding="utf-8")
    print(OUTDIR)


if __name__ == "__main__":
    main()
