#!/usr/bin/env python3
"""
generate_dashboard_video.py — Build a professional 7-minute narrated video
presentation for the Observability Platform — Databricks ETL Pipeline project.

Covers:
  1. Title / Intro                           (~30s)
  2. Problem Statement — The Observability Gap (~35s)
  3. Architecture — Medallion Pipeline        (~40s)
  4. Data Source — LogHub Benchmark            (~30s)
  5. Log Complexity — 5 Dimensions            (~35s)
  6. Figure 1 — Semantic Ambiguity            (~30s)
  7. Figure 2 — Error Concentration           (~30s)
  8. Figure 3 — Trace Reconstructability      (~35s)
  9. Figure 4 — Anomaly Detection             (~35s)
  10. Figure 5 — Query Performance            (~30s)
  11. AI-Augmented Visualization              (~40s)
  12. Conclusion & Key Takeaways              (~30s)
                                     Total ≈ 6:40

Output:
  job_results/dashboard_video/dashboard_explainer.mp4
  job_results/dashboard_video/slides/*.png
  job_results/dashboard_video/audio/*.mp3
"""

import os
import sys
import textwrap
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIGURES_DIR  = os.path.join(PROJECT_ROOT, "docs", "figures")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "job_results", "dashboard_video")
SLIDES_DIR   = os.path.join(OUTPUT_DIR, "slides")
AUDIO_DIR    = os.path.join(OUTPUT_DIR, "audio")

os.makedirs(SLIDES_DIR, exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)

# ── Design constants ───────────────────────────────────────────────────────
SLIDE_W, SLIDE_H = 16, 9          # 16:9 aspect ratio
DPI = 180
BG_DARK   = "#0F172A"             # Slate 900
BG_CARD   = "#1E293B"             # Slate 800
ACCENT    = "#3B82F6"             # Blue 500
ACCENT2   = "#10B981"             # Emerald 500
WARN      = "#F59E0B"             # Amber 500
DANGER    = "#EF4444"             # Red 500
TEXT_W    = "#F8FAFC"             # Slate 50
TEXT_M    = "#94A3B8"             # Slate 400
PURPLE    = "#8B5CF6"


def slide_base(title="", subtitle="", slide_num=None, total=12):
    """Create a base figure with dark background, title bar, and footer."""
    fig = plt.figure(figsize=(SLIDE_W, SLIDE_H), facecolor=BG_DARK)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 9)
    ax.set_facecolor(BG_DARK)
    ax.axis("off")

    # ── Top accent bar ──
    ax.fill_between([0, 16], [9, 9], [8.85, 8.85], color=ACCENT)

    # ── Title ──
    if title:
        ax.text(0.6, 8.4, title, fontsize=28, fontweight="bold",
                color=TEXT_W, va="center",
                path_effects=[pe.withStroke(linewidth=3, foreground=BG_DARK)])
    if subtitle:
        ax.text(0.6, 7.85, subtitle, fontsize=14, color=TEXT_M, va="center")

    # ── Footer ──
    ax.fill_between([0, 16], [0.45, 0.45], [0, 0], color=BG_CARD)
    ax.text(0.4, 0.22, "DS-610 Big Data Analytics  |  Andrew Espira  |  Saint Peter's University  |  February 2026",
            fontsize=9, color=TEXT_M, va="center")
    if slide_num is not None:
        ax.text(15.6, 0.22, f"{slide_num}/{total}", fontsize=9,
                color=TEXT_M, va="center", ha="right")

    return fig, ax


def draw_card(ax, x, y, w, h, text, color=BG_CARD, text_color=TEXT_W,
              fontsize=11, border_color=None, alpha=0.95):
    """Draw a rounded card on the axis."""
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15",
                         facecolor=color, edgecolor=border_color or color,
                         alpha=alpha, linewidth=1.5 if border_color else 0)
    ax.add_patch(box)
    ax.text(x + w/2, y + h/2, text, ha="center", va="center",
            fontsize=fontsize, color=text_color, fontweight="bold",
            linespacing=1.5,
            path_effects=[pe.withStroke(linewidth=2, foreground=color)])


def draw_arrow(ax, x1, y1, x2, y2, color=TEXT_M):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=2))


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 1: TITLE
# ═══════════════════════════════════════════════════════════════════════════
def slide_01_title():
    fig = plt.figure(figsize=(SLIDE_W, SLIDE_H), facecolor=BG_DARK)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 16); ax.set_ylim(0, 9)
    ax.set_facecolor(BG_DARK); ax.axis("off")

    # Accent bar top
    ax.fill_between([0, 16], [9, 9], [8.7, 8.7], color=ACCENT)

    # Main title
    ax.text(8, 6.2, "Observability Platform", fontsize=42, fontweight="bold",
            color=TEXT_W, ha="center", va="center")
    ax.text(8, 5.2, "Databricks ETL Pipeline for Log Analytics at Scale",
            fontsize=20, color=ACCENT, ha="center", va="center")

    # Divider
    ax.plot([3, 13], [4.4, 4.4], color=ACCENT, linewidth=2, alpha=0.5)

    # Subtitle cards
    cards = [
        ("[Spark]  Apache Spark", 1.5), ("[Delta]  Delta Lake", 5.5),
        ("[ML]  MLlib Anomaly Detection", 9.5), ("[AI]  AI Agents", 13.2),
    ]
    for label, x in cards:
        draw_card(ax, x, 3.2, 2.8, 0.8, label, color=BG_CARD,
                  fontsize=11, border_color=ACCENT)

    # Author / course
    ax.text(8, 2.0, "Andrew Espira", fontsize=18, color=TEXT_W,
            ha="center", va="center", fontweight="bold")
    ax.text(8, 1.4, "DS-610 Big Data Analytics  —  Saint Peter's University",
            fontsize=13, color=TEXT_M, ha="center", va="center")
    ax.text(8, 0.9, "February 2026", fontsize=12, color=TEXT_M,
            ha="center", va="center")

    path = os.path.join(SLIDES_DIR, "slide_01.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 2: PROBLEM STATEMENT
# ═══════════════════════════════════════════════════════════════════════════
def slide_02_problem():
    fig, ax = slide_base("The Observability Context Gap",
                         "Why traditional log pipelines fail at scale", 2)

    # Three problem cards
    problems = [
        ("[X]  Aggregate Too Early", "Lose trace context —\ncan't reconstruct\nincident paths", DANGER),
        ("[X]  Sample Aggressively", "Miss rare but critical\nerrors — 23% of errors\nhidden at INFO level", WARN),
        ("[X]  Store Everything Raw", "Unsustainable costs —\n50–500 GB/day per cluster\nSlow queries (seconds)", TEXT_M),
    ]
    for i, (title, desc, color) in enumerate(problems):
        x = 0.8 + i * 5.1
        draw_card(ax, x, 4.0, 4.5, 3.0, "", color=BG_CARD, border_color=color)
        ax.text(x + 2.25, 6.4, title, ha="center", va="center",
                fontsize=14, fontweight="bold", color=color)
        ax.text(x + 2.25, 5.0, desc, ha="center", va="center",
                fontsize=12, color=TEXT_W, linespacing=1.6)

    # Bottom insight
    draw_card(ax, 2, 1.0, 12, 2.2, "", color=BG_CARD, border_color=ACCENT2)
    ax.text(8, 2.4, ">>  Our Solution: Context-Preserving Pipeline",
            ha="center", va="center", fontsize=16, fontweight="bold", color=ACCENT2)
    ax.text(8, 1.6, "200× data reduction  •  95.5% trace reconstructability  •  75× faster queries",
            ha="center", va="center", fontsize=13, color=TEXT_W)

    path = os.path.join(SLIDES_DIR, "slide_02.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 3: ARCHITECTURE
# ═══════════════════════════════════════════════════════════════════════════
def slide_03_architecture():
    fig, ax = slide_base("Medallion Architecture",
                         "Bronze → Silver → Gold  |  Context preserved at every layer", 3)

    layers = [
        ("BRONZE\n(Raw Ingestion)", "Schema validation\nDeduplication\nTimestamp normalization\nPartition by date",
         "#B45309", 0.8),
        ("SILVER\n(Enrichment)", "Flatten nested JSON\nTrace reconstruction\nService metadata join\nWindow functions",
         "#6B7280", 5.5),
        ("GOLD\n(Analytics)", "Hourly aggregation\nError rate, latency P50/P95/P99\nSample trace IDs\n200× reduction",
         "#CA8A04", 10.2),
    ]

    for label, desc, color, x in layers:
        draw_card(ax, x, 3.2, 4.2, 4.2, "", color=BG_CARD, border_color=color)
        ax.text(x + 2.1, 6.8, label, ha="center", va="center",
                fontsize=15, fontweight="bold", color=color, linespacing=1.4)
        ax.text(x + 2.1, 4.6, desc, ha="center", va="center",
                fontsize=11, color=TEXT_W, linespacing=1.5)

    # Arrows between layers
    draw_arrow(ax, 5.0, 5.3, 5.5, 5.3, color=TEXT_M)
    draw_arrow(ax, 9.7, 5.3, 10.2, 5.3, color=TEXT_M)

    # Volume indicators
    ax.text(3.0, 2.5, "100,000 events", ha="center", fontsize=10, color="#B45309", fontweight="bold")
    ax.text(7.6, 2.5, "100,000 enriched", ha="center", fontsize=10, color="#6B7280", fontweight="bold")
    ax.text(12.3, 2.5, "~500 edges", ha="center", fontsize=10, color="#CA8A04", fontweight="bold")

    # Bottom tech stack
    ax.text(8, 1.3, "Apache Spark 3.4  •  Delta Lake 2.4  •  Databricks Runtime 13.3 LTS  •  MLlib  •  Structured Streaming",
            ha="center", fontsize=10, color=TEXT_M, va="center")

    path = os.path.join(SLIDES_DIR, "slide_03.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 4: DATA SOURCE — LOGHUB
# ═══════════════════════════════════════════════════════════════════════════
def slide_04_loghub():
    fig, ax = slide_base("Data Source: LogHub Benchmark",
                         "Real production logs from academic research  (He et al., 2020)", 4)

    datasets = [
        ("HDFS",   "100K lines", "Hadoop Distributed\nFile System"),
        ("Spark",  "33K lines",  "Apache Spark\nData Processing"),
        ("BGL",    "4.7M lines", "Blue Gene/L\nSupercomputer"),
        ("Linux",  "25K lines",  "Linux Syslog\nSystem Logs"),
    ]

    for i, (name, size, desc) in enumerate(datasets):
        x = 0.6 + i * 3.9
        draw_card(ax, x, 4.2, 3.4, 3.2, "", color=BG_CARD, border_color=ACCENT)
        ax.text(x + 1.7, 6.8, name, ha="center", va="center",
                fontsize=18, fontweight="bold", color=ACCENT)
        ax.text(x + 1.7, 6.0, size, ha="center", va="center",
                fontsize=13, color=ACCENT2, fontweight="bold")
        ax.text(x + 1.7, 5.0, desc, ha="center", va="center",
                fontsize=11, color=TEXT_W, linespacing=1.4)

    # Why LogHub
    reasons = [
        "+  Real production data (not synthetic)",
        "+  100+ research citations",
        "+  Multiple log formats per dataset",
        "+  Publicly available — fully reproducible",
    ]
    for i, r in enumerate(reasons):
        ax.text(1.0 + (i % 2) * 7.5, 3.0 - (i // 2) * 0.6, r,
                fontsize=12, color=TEXT_W, va="center")

    path = os.path.join(SLIDES_DIR, "slide_04.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 5: FIVE DIMENSIONS OF COMPLEXITY
# ═══════════════════════════════════════════════════════════════════════════
def slide_05_complexity():
    fig, ax = slide_base("Five Dimensions of Log Data Complexity",
                         "Why logs are the hardest observability signal to process", 5)

    dims = [
        ("1. Structural\nHeterogeneity", "6 timestamp formats\n4 service identity patterns\n3 levels of JSON nesting", ACCENT),
        ("2. Semantic\nAmbiguity", "23% of errors at INFO level\n8% of ERROR level is benign\nKeyword filtering unreliable", DANGER),
        ("3. Volume &\nVelocity", "50–500 GB/day per cluster\n100× spike during incidents\nHeavy-tailed distribution", WARN),
        ("4. Temporal\nDependencies", "Trace DAGs span 5–15 services\nCausal reconstruction needed\nExpensive shuffle operations", PURPLE),
        ("5. Contextual\nDependencies", "Meaning depends on neighbors\nAggregation destroys context\n40% of traces lost naively", ACCENT2),
    ]

    for i, (title, desc, color) in enumerate(dims):
        x = 0.3 + i * 3.1
        draw_card(ax, x, 3.0, 2.8, 4.4, "", color=BG_CARD, border_color=color)
        ax.text(x + 1.4, 6.7, title, ha="center", va="center",
                fontsize=12, fontweight="bold", color=color, linespacing=1.3)
        ax.text(x + 1.4, 4.5, desc, ha="center", va="center",
                fontsize=9.5, color=TEXT_W, linespacing=1.6)

    # Bottom
    ax.text(8, 1.5, "Each dimension requires a different engineering strategy — no single technique addresses all five.",
            ha="center", fontsize=12, color=TEXT_M, va="center", fontstyle="italic")

    path = os.path.join(SLIDES_DIR, "slide_05.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# SLIDES 6–10: EMBED THE PAPER 1 FIGURES
# ═══════════════════════════════════════════════════════════════════════════
def slide_with_figure(slide_num, title, subtitle, figure_file, caption):
    """Create a slide that embeds a pre-generated figure."""
    fig, ax = slide_base(title, subtitle, slide_num)

    fig_path = os.path.join(FIGURES_DIR, figure_file)
    if os.path.exists(fig_path):
        img = plt.imread(fig_path)
        # Place figure in center — use inset axes
        img_ax = fig.add_axes([0.08, 0.12, 0.84, 0.62])
        img_ax.imshow(img)
        img_ax.axis("off")
    else:
        ax.text(8, 4.5, f"[Figure: {figure_file}]", ha="center", va="center",
                fontsize=16, color=TEXT_M)

    # Caption below the image
    ax.text(8, 0.7, caption, ha="center", va="center",
            fontsize=10, color=TEXT_M, fontstyle="italic")

    path = os.path.join(SLIDES_DIR, f"slide_{slide_num:02d}.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


def slide_06_confusion():
    return slide_with_figure(
        6, "Semantic Ambiguity: Log Level ≠ Error Severity",
        "23% of real errors are hidden at INFO level  |  500 HDFS entries sampled",
        "figure_1_confusion_matrix.png",
        "Figure 1: Confusion matrix — Log level vs. actual error semantics (HDFS dataset)"
    )

def slide_07_concentration():
    return slide_with_figure(
        7, "Error Concentration: Heavy-Tailed Distribution",
        "Top 1% of time windows contain 18% of all error events  |  BGL dataset",
        "figure_2_error_concentration.png",
        "Figure 2: Error events per 1-minute window — the long tail of incident data"
    )

def slide_08_reconstructability():
    return slide_with_figure(
        8, "Trace Reconstructability: Context Matters",
        "Naive aggregation loses 40% of incident paths  |  200 sampled error traces",
        "figure_3_trace_reconstructability.png",
        "Figure 3: 95.5% reconstructable with context-preserving aggregation vs. 60.5% naive"
    )

def slide_09_anomaly():
    return slide_with_figure(
        9, "Anomaly Detection with MLlib KMeans",
        "5% of service edges flagged anomalous  |  Silhouette score = 0.53",
        "figure_4_anomaly_scatter.png",
        "Figure 4: Error rate vs. latency — anomalies classified by root cause"
    )

def slide_10_performance():
    return slide_with_figure(
        10, "Query Performance: Gold vs. Silver Layer",
        "75× faster queries through intelligent aggregation  |  134–201× data reduction",
        "figure_5_query_latency.png",
        "Figure 5: Gold layer enables sub-second analytical queries"
    )


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 11: AI-AUGMENTED VISUALIZATION
# ═══════════════════════════════════════════════════════════════════════════
def slide_11_ai_agents():
    fig, ax = slide_base("AI-Augmented Visualization & Agents",
                         "From static charts to intelligent interpretation", 11)

    # Three agent cards
    agents = [
        ("[1]  Dependency\nReviewer", "Analyzes service coupling\nidentifies single points\nof failure, fan-out risks", ACCENT),
        ("[2]  Error Rate\nReviewer", "Root cause hypotheses\nseverity ratings\nimmediate action items", DANGER),
        ("[3]  Anomaly\nReviewer", "Validates ML detections\nfilters noise from incidents\nsuggests threshold tuning", PURPLE),
    ]

    for i, (title, desc, color) in enumerate(agents):
        x = 0.6 + i * 5.2
        draw_card(ax, x, 4.5, 4.6, 2.8, "", color=BG_CARD, border_color=color)
        ax.text(x + 2.3, 6.7, title, ha="center", va="center",
                fontsize=13, fontweight="bold", color=color, linespacing=1.3)
        ax.text(x + 2.3, 5.2, desc, ha="center", va="center",
                fontsize=11, color=TEXT_W, linespacing=1.5)

    # Data Copilot card
    draw_card(ax, 1.5, 1.0, 6.0, 2.8, "", color=BG_CARD, border_color=ACCENT2)
    ax.text(4.5, 3.3, "Data Copilot", ha="center", va="center",
            fontsize=14, fontweight="bold", color=ACCENT2)
    ax.text(4.5, 2.2, 'Ask: "Which service has the\nhighest error rate?"\n> Generates Pandas query\n> Returns answer + code',
            ha="center", va="center", fontsize=10, color=TEXT_W, linespacing=1.4)

    # Cost card
    draw_card(ax, 8.5, 1.0, 6.0, 2.8, "", color=BG_CARD, border_color=WARN)
    ax.text(11.5, 3.3, "Cost-Aware Design", ha="center", va="center",
            fontsize=14, fontweight="bold", color=WARN)
    ax.text(11.5, 2.2, "MLlib triages 100K events → 25 anomalies\nOnly anomalies sent to GPT-4o-mini\nEstimated 20× cost reduction\n$0.02 per full dashboard review",
            ha="center", va="center", fontsize=10, color=TEXT_W, linespacing=1.4)

    path = os.path.join(SLIDES_DIR, "slide_11.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 12: CONCLUSION
# ═══════════════════════════════════════════════════════════════════════════
def slide_12_conclusion():
    fig = plt.figure(figsize=(SLIDE_W, SLIDE_H), facecolor=BG_DARK)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 16); ax.set_ylim(0, 9)
    ax.set_facecolor(BG_DARK); ax.axis("off")

    ax.fill_between([0, 16], [9, 9], [8.85, 8.85], color=ACCENT2)

    ax.text(8, 8.3, "Key Takeaways", fontsize=30, fontweight="bold",
            color=TEXT_W, ha="center", va="center")

    takeaways = [
        ("200×", "data reduction\nBronze → Gold", ACCENT),
        ("95.5%", "trace reconstructability\nwith context preservation", ACCENT2),
        ("75×", "faster queries\non Gold vs. Silver", WARN),
        ("5%", "anomaly detection rate\nMLlib KMeans (k=3)", DANGER),
        ("20×", "cost reduction\nML-triage → LLM", PURPLE),
    ]

    for i, (num, desc, color) in enumerate(takeaways):
        x = 0.5 + i * 3.1
        draw_card(ax, x, 4.2, 2.7, 3.5, "", color=BG_CARD, border_color=color)
        ax.text(x + 1.35, 6.8, num, ha="center", va="center",
                fontsize=28, fontweight="bold", color=color)
        ax.text(x + 1.35, 5.2, desc, ha="center", va="center",
                fontsize=10, color=TEXT_W, linespacing=1.5)

    # Bottom
    ax.text(8, 2.6, "The future of observability: not just showing data — understanding it on behalf of humans.",
            ha="center", fontsize=14, color=TEXT_W, va="center", fontstyle="italic")

    ax.fill_between([0, 16], [1.5, 1.5], [0, 0], color=BG_CARD)
    ax.text(8, 1.1, "Andrew Espira  •  DS-610 Big Data Analytics  •  Saint Peter's University",
            ha="center", fontsize=12, color=TEXT_M, va="center")
    ax.text(8, 0.5, "GitHub:  Observability-Platform---Databricks-ETL-Pipeline  |  Apache Spark  •  Delta Lake  •  MLlib  •  GPT-4o",
            ha="center", fontsize=10, color=TEXT_M, va="center")

    path = os.path.join(SLIDES_DIR, "slide_12.png")
    fig.savefig(path, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# NARRATION SCRIPTS  (~35 seconds each = 85-100 words)
# ═══════════════════════════════════════════════════════════════════════════
NARRATIONS = [
    # 1. Title (~20s)
    (
        "Welcome to the Observability Platform presentation. "
        "This project builds a production-grade ETL pipeline on Apache Spark and Delta Lake "
        "for processing log data from distributed systems. "
        "Let's walk through how it works."
    ),
    # 2. Problem (~25s)
    (
        "Modern systems generate 50 to 500 gigabytes of logs per day per cluster. "
        "Traditional pipelines face a dilemma. "
        "Aggregate too early and you lose trace context. "
        "Sample too aggressively and you miss critical errors. "
        "Store everything raw and costs become unsustainable. "
        "Our solution achieves 200-times data reduction while keeping 95.5 percent of traces reconstructable."
    ),
    # 3. Architecture (~30s)
    (
        "We implement the medallion architecture with three layers. "
        "Bronze ingests raw logs, validates schemas, deduplicates, and partitions by date. "
        "Silver enriches events by flattening JSON, reconstructing traces, and joining metadata. "
        "Gold aggregates into hourly service flow edges with error rate, latency percentiles, "
        "and sampled trace IDs, achieving a 200-times reduction."
    ),
    # 4. LogHub (~25s)
    (
        "Our data comes from LogHub, a benchmark of real production logs "
        "cited in over 100 research papers. "
        "We process HDFS with 100 thousand lines, "
        "Spark with 33 thousand, "
        "B-G-L with 4.7 million supercomputer logs, "
        "and Linux with 25 thousand syslog entries. "
        "Real-world data gives our results academic credibility."
    ),
    # 5. Complexity (~30s)
    (
        "Log data has five complexity dimensions. "
        "Structural heterogeneity: no universal format. "
        "Semantic ambiguity: grep for ERROR misses 23 percent of actual errors. "
        "Volume spikes 100 times during incidents. "
        "Temporal dependencies require trace reconstruction across services. "
        "Contextual dependencies mean a message's meaning depends on its neighbors. "
        "Each dimension needs a different strategy."
    ),
    # 6. Confusion Matrix (~25s)
    (
        "This confusion matrix quantifies semantic ambiguity. "
        "From 500 manually labeled HDFS entries, "
        "23 percent of error events were logged at INFO level. "
        "8 percent of ERROR entries were benign. "
        "Simple keyword filtering is unreliable. "
        "Template-based parsing and machine learning are essential."
    ),
    # 7. Error Concentration (~25s)
    (
        "This shows the heavy-tailed error distribution in the B-G-L dataset. "
        "The top 1 percent of one-minute windows contain 18 percent of all errors. "
        "When logs matter most, they're hardest to process. "
        "Our pipeline uses date partitioning and Z-ordering to handle this skew."
    ),
    # 8. Reconstructability (~30s)
    (
        "This is our key result. "
        "Trace reconstructability measures whether we can identify root cause, "
        "call chain, and error message from Gold data alone. "
        "Naive aggregation: only 60.5 percent reconstructable. "
        "Our context-preserving approach using collect-set and retained error messages: "
        "95.5 percent reconstructable at the same 200-times compression. "
        "Context preservation is the critical difference."
    ),
    # 9. Anomaly Detection (~25s)
    (
        "Gold layer data feeds into M-L-lib K-Means clustering with k equals 3. "
        "Features include error rate, latency variance, and P95 to P50 ratio. "
        "Silhouette score is 0.53. "
        "5 percent of service edges are flagged as anomalous, "
        "classified by root cause: high error rate, high latency, or combined degradation."
    ),
    # 10. Performance (~25s)
    (
        "Gold layer queries are 75 times faster than Silver. "
        "A 7.5 second Silver query completes in 100 milliseconds on Gold. "
        "Data reduction ranges from 134 to 201 times across our four datasets. "
        "Intelligent aggregation enables sub-second analytics without losing value."
    ),
    # 11. AI Agents (~30s)
    (
        "We extend the pipeline with three specialized LLM agents: "
        "a Dependency Reviewer, an Error Rate Reviewer, and an Anomaly Reviewer. "
        "A Data Copilot accepts natural language questions and generates Pandas queries. "
        "Every AI feature has a rule-based fallback. "
        "Only the 25 flagged anomalies go to GPT-4o-mini, not all 100 thousand events, "
        "delivering 20-times cost reduction."
    ),
    # 12. Conclusion (~20s)
    (
        "In summary: 200 times data reduction. "
        "95.5 percent trace reconstructability. "
        "75 times faster queries. "
        "5 percent anomaly detection with validated clustering. "
        "20 times cost reduction through M-L-then-LLM design. "
        "The future of observability is understanding data on behalf of humans. "
        "Thank you for watching."
    ),
]


# ═══════════════════════════════════════════════════════════════════════════
# TTS GENERATION
# ═══════════════════════════════════════════════════════════════════════════
def generate_audio(narrations):
    """Generate MP3 narration for each slide using gTTS."""
    try:
        from gtts import gTTS
    except ImportError:
        print("⚠️  gTTS not installed. Run: pip install gtts")
        return [None] * len(narrations)

    audio_paths = []
    total_est = 0
    for i, text in enumerate(narrations, 1):
        out = os.path.join(AUDIO_DIR, f"slide_{i:02d}.mp3")
        tts = gTTS(text=text, lang="en", slow=False)
        tts.save(out)
        # Rough estimate: ~2.5 words per second for gTTS
        word_count = len(text.split())
        est_sec = word_count / 2.5
        total_est += est_sec
        audio_paths.append(out)
        print(f"  🔊 slide_{i:02d}.mp3 — {word_count} words ≈ {est_sec:.0f}s")

    print(f"\n  📏 Estimated total narration: {total_est:.0f}s ({total_est/60:.1f} min)")
    return audio_paths


# ═══════════════════════════════════════════════════════════════════════════
# VIDEO ASSEMBLY
# ═══════════════════════════════════════════════════════════════════════════
def assemble_video(slide_paths, audio_paths):
    """Combine slides + audio into final MP4."""
    try:
        from moviepy import ImageClip, AudioFileClip, concatenate_videoclips
    except ImportError:
        print("⚠️  moviepy not installed. Run: pip install moviepy imageio-ffmpeg")
        return None

    clips = []
    total_duration = 0

    for i, (slide, audio) in enumerate(zip(slide_paths, audio_paths)):
        if audio and os.path.exists(audio):
            audio_clip = AudioFileClip(audio)
            duration = audio_clip.duration + 0.5  # 0.5s padding after narration
            img_clip = ImageClip(slide, duration=duration)
            img_clip = img_clip.with_audio(audio_clip)
        else:
            duration = 10  # fallback: 10s per slide
            img_clip = ImageClip(slide, duration=duration)

        # Resize to 1920x1080 for consistent output
        img_clip = img_clip.resized((1920, 1080))
        clips.append(img_clip)
        total_duration += duration

    print(f"\n  🎬 Total video duration: {total_duration:.0f}s ({total_duration/60:.1f} min)")

    if total_duration > 420:
        print(f"  ⚠️  Video exceeds 7 minutes! ({total_duration/60:.1f} min)")
    else:
        print(f"  ✅ Within 7-minute limit")

    final = concatenate_videoclips(clips, method="compose")
    out_path = os.path.join(OUTPUT_DIR, "dashboard_explainer.mp4")
    final.write_videofile(out_path, fps=24, audio_codec="aac",
                          logger="bar")

    file_size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"\n  ✅ Video saved: {out_path} ({file_size_mb:.1f} MB)")
    return out_path


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main():
    print("\n" + "=" * 60)
    print("🎬  GENERATING DASHBOARD PRESENTATION VIDEO")
    print("=" * 60)

    # Step 1: Generate slides
    print("\n📊 Step 1: Generating slides...")
    slide_paths = [
        slide_01_title(),
        slide_02_problem(),
        slide_03_architecture(),
        slide_04_loghub(),
        slide_05_complexity(),
        slide_06_confusion(),
        slide_07_concentration(),
        slide_08_reconstructability(),
        slide_09_anomaly(),
        slide_10_performance(),
        slide_11_ai_agents(),
        slide_12_conclusion(),
    ]
    print(f"  ✅ {len(slide_paths)} slides generated")

    # Step 2: Generate audio
    print("\n🔊 Step 2: Generating narration audio...")
    audio_paths = generate_audio(NARRATIONS)

    # Step 3: Assemble video
    print("\n🎥 Step 3: Assembling video...")
    video_path = assemble_video(slide_paths, audio_paths)

    print("\n" + "=" * 60)
    print("✅  DONE!")
    print(f"   Slides: {SLIDES_DIR}/")
    print(f"   Audio:  {AUDIO_DIR}/")
    if video_path:
        print(f"   Video:  {video_path}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
