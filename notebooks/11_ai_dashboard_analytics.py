# Databricks notebook source
# MAGIC %md
# MAGIC # 11: AI-Powered Dashboard Analytics & Data Copilot
# MAGIC
# MAGIC **Purpose**: Comprehensive data visualization, AI-driven insight generation,
# MAGIC narrated presentation, and interactive data copilot for the Observability Pipeline.
# MAGIC
# MAGIC **Capabilities**:
# MAGIC 1. 📊 **Rich Data Visualizations** — Plotly interactive dashboards from pipeline data
# MAGIC 2. 🤖 **AI Dashboard Reviewer** — LLM agents that review and critique charts
# MAGIC 3. 🎙️ **Presentation Narrator** — Text-to-speech analyst-style narration
# MAGIC 4. 🧠 **LLM Chart Interpreter** — GPT-powered chart & insight interpretation
# MAGIC 5. 📝 **Automated Report Generator** — Detailed HTML report with charts + commentary
# MAGIC 6. 💬 **Data Copilot** — Natural language interface to query and explore the data
# MAGIC
# MAGIC **Input**: Gold, Silver, and Anomaly Delta tables from the pipeline
# MAGIC
# MAGIC **Output**:
# MAGIC - Interactive Plotly dashboards
# MAGIC - AI-generated insight reports (HTML)
# MAGIC - Narrated slide deck (MP3 audio per slide)
# MAGIC - Data Copilot chat interface

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 0: Install Dependencies

# COMMAND ----------

# MAGIC %pip install plotly kaleido gtts openai langchain langchain-openai tabulate Jinja2 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Imports & Configuration

# COMMAND ----------

import os
import json
import time
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Text-to-speech
try:
    from gtts import gTTS
    HAS_GTTS = True
    print("✅ gTTS available")
except ImportError:
    HAS_GTTS = False
    print("⚠️  gTTS not available — narration will be skipped")

# OpenAI / LLM
try:
    from openai import OpenAI
    HAS_OPENAI = True
    print("✅ OpenAI SDK available")
except ImportError:
    HAS_OPENAI = False
    print("⚠️  OpenAI SDK not available — AI features will use rule-based fallback")

# Jinja2 for HTML reports
try:
    from jinja2 import Template
    HAS_JINJA = True
    print("✅ Jinja2 available")
except ImportError:
    HAS_JINJA = False
    print("⚠️  Jinja2 not available — HTML report will use basic formatting")

# ── Paths ──────────────────────────────────────────────────────────────
GOLD_PATH       = "/observability-data/gold/service_flow_edges"
SILVER_PATH     = "/observability-data/silver/events"
ANOMALY_PATH    = "/observability-data/analytics/anomalies"
OUTPUT_DIR      = "/dbfs/observability-data/ai_dashboard"
SLIDES_DIR      = os.path.join(OUTPUT_DIR, "slides")
AUDIO_DIR       = os.path.join(OUTPUT_DIR, "audio")
REPORT_DIR      = os.path.join(OUTPUT_DIR, "reports")

for d in [OUTPUT_DIR, SLIDES_DIR, AUDIO_DIR, REPORT_DIR]:
    os.makedirs(d, exist_ok=True)

# ── LLM Configuration ─────────────────────────────────────────────────
# Set your OpenAI API key via Databricks secrets or widget
try:
    OPENAI_API_KEY = dbutils.secrets.get(scope="openai", key="api_key")
except Exception:
    try:
        dbutils.widgets.text("openai_api_key", "", "OpenAI API Key (optional)")
        OPENAI_API_KEY = dbutils.widgets.get("openai_api_key").strip()
    except Exception:
        OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

if OPENAI_API_KEY and HAS_OPENAI:
    llm_client = OpenAI(api_key=OPENAI_API_KEY)
    LLM_AVAILABLE = True
    print("✅ LLM connected (OpenAI)")
else:
    llm_client = None
    LLM_AVAILABLE = False
    print("ℹ️  LLM not configured — using rule-based analytics (set openai_api_key widget to enable)")

print(f"\n📂 Output directory: {OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Load Pipeline Data

# COMMAND ----------

# ── Helper: load Delta or fall back gracefully ─────────────────────────
def load_delta_safe(path, name):
    """Load a Delta table; return None with warning if missing."""
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"✅ {name}: {count:,} records from {path}")
        return df
    except Exception as e:
        print(f"⚠️  {name}: Could not load ({e})")
        return None

gold_df    = load_delta_safe(GOLD_PATH,    "Gold edges")
silver_df  = load_delta_safe(SILVER_PATH,  "Silver events")
anomaly_df = load_delta_safe(ANOMALY_PATH, "Anomalies")

# Convert to Pandas for visualization (sample if large)
MAX_ROWS = 500_000

def to_pandas_safe(spark_df, name, max_rows=MAX_ROWS):
    if spark_df is None:
        print(f"⚠️  {name} not available — generating sample data")
        return None
    count = spark_df.count()
    if count > max_rows:
        print(f"   ↳ Sampling {max_rows:,} of {count:,} rows for {name}")
        return spark_df.sample(fraction=max_rows / count, seed=42).toPandas()
    return spark_df.toPandas()

gold_pd    = to_pandas_safe(gold_df,    "Gold")
silver_pd  = to_pandas_safe(silver_df,  "Silver")
anomaly_pd = to_pandas_safe(anomaly_df, "Anomaly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b: Generate Sample Data if Pipeline Tables Missing
# MAGIC
# MAGIC If the Delta tables don't exist yet (e.g., first run), we generate
# MAGIC representative sample data so the rest of the notebook still works.

# COMMAND ----------

import random, uuid

def _generate_sample_gold(n=800):
    """Generate representative Gold service-flow-edge data."""
    services = ["api-gateway", "payment-service", "user-service",
                "order-service", "inventory-service", "notification-service", "risk-service"]
    endpoints = ["/api/v1/checkout", "/api/v1/orders", "/internal/payments/authorize",
                 "/internal/users/profile", "/internal/orders/create",
                 "/internal/inventory/check", "/internal/risk/score"]
    rows = []
    base = datetime(2026, 2, 16, 0, 0, 0)
    for i in range(n):
        src = random.choice(services)
        tgt = random.choice([s for s in services if s != src])
        hour = base + timedelta(hours=random.randint(0, 23))
        req = random.randint(10, 5000)
        err = int(req * random.uniform(0, 0.15))
        avg_lat = random.uniform(5, 500)
        rows.append({
            "hour": hour,
            "source_service": src,
            "target_service": tgt,
            "endpoint": random.choice(endpoints),
            "request_count": req,
            "error_count": err,
            "success_count": req - err,
            "error_rate": round(err / req, 4),
            "success_rate": round(1 - err / req, 4),
            "avg_latency": round(avg_lat, 2),
            "min_latency": round(avg_lat * 0.2, 2),
            "max_latency": round(avg_lat * 3.5, 2),
            "p50_latency": round(avg_lat * 0.8, 2),
            "p95_latency": round(avg_lat * 2.2, 2),
            "p99_latency": round(avg_lat * 3.0, 2),
            "first_seen": hour,
            "last_seen": hour + timedelta(minutes=59),
            "partition_date": hour.date(),
        })
    return pd.DataFrame(rows)


def _generate_sample_anomaly(gold_pdf, frac=0.05):
    """Mark ~5 % of gold rows as anomalies."""
    adf = gold_pdf.copy()
    n_anom = max(1, int(len(adf) * frac))
    adf["anomaly_score"] = np.random.exponential(1.0, len(adf)).round(4)
    threshold = np.percentile(adf["anomaly_score"], 95)
    adf["is_anomaly"] = adf["anomaly_score"] > threshold
    adf["anomaly_severity"] = adf["anomaly_score"].apply(
        lambda s: "high" if s > threshold * 1.5 else ("medium" if s > threshold else "normal"))
    adf["cluster"] = np.random.randint(0, 3, len(adf))
    return adf


if gold_pd is None:
    print("🔄 Generating sample Gold data …")
    gold_pd = _generate_sample_gold()
    print(f"   → {len(gold_pd):,} sample Gold edges")

if anomaly_pd is None:
    print("🔄 Generating sample Anomaly data …")
    anomaly_pd = _generate_sample_anomaly(gold_pd)
    print(f"   → {len(anomaly_pd):,} sample Anomaly records")

if silver_pd is None:
    print("ℹ️  Silver data not loaded — some charts will use Gold-level data instead")

print(f"\n📊 Data ready for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 PART 1 — Rich Interactive Visualizations
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 1: Service Dependency Heatmap

# COMMAND ----------

pip install -U kaleido

# COMMAND ----------

# DBTITLE 1,Cell 12
# ── Service-to-Service Request Volume Heatmap ──────────────────────────
edge_matrix = gold_pd.groupby(["source_service", "target_service"])["request_count"].sum().reset_index()
pivot = edge_matrix.pivot(index="source_service", columns="target_service", values="request_count").fillna(0)

fig1 = px.imshow(
    pivot,
    labels=dict(x="Target Service", y="Source Service", color="Requests"),
    title="Service Dependency Heatmap — Total Request Volume",
    color_continuous_scale="YlOrRd",
    aspect="auto",
)
fig1.update_layout(width=800, height=500)
fig1.show()

# Save static image for report
try:
    fig1.write_image(os.path.join(SLIDES_DIR, "chart_heatmap.png"), scale=2)
except Exception as e:
    print(f"⚠️  Could not save image (kaleido may not be installed): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 2: Error Rate by Service Edge (Top 15)

# COMMAND ----------

# DBTITLE 1,Cell 15
edge_errors = (gold_pd.groupby(["source_service", "target_service"])
    .agg({"error_rate": "mean", "request_count": "sum"})
    .reset_index()
    .sort_values("error_rate", ascending=False)
    .head(15))
edge_errors["edge"] = edge_errors["source_service"] + " → " + edge_errors["target_service"]

fig2 = px.bar(
    edge_errors, x="edge", y="error_rate",
    color="error_rate",
    color_continuous_scale="Reds",
    title="Top 15 Service Edges by Error Rate",
    labels={"error_rate": "Error Rate", "edge": "Service Edge"},
)
fig2.update_layout(xaxis_tickangle=-35, width=900, height=450)
fig2.show()
try:
    fig2.write_image(os.path.join(SLIDES_DIR, "chart_error_rate.png"), scale=2)
except Exception as e:
    print(f"⚠️  Could not save image (kaleido may not be installed): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 3: Latency Distribution (p50 / p95 / p99)

# COMMAND ----------

# DBTITLE 1,Cell 17
latency_data = gold_pd.groupby("target_service")[["p50_latency_ms", "p95_latency_ms", "p99_latency_ms"]].mean().reset_index()

fig3 = go.Figure()
for pct, color in [("p50_latency_ms", "#2ca02c"), ("p95_latency_ms", "#ff7f0e"), ("p99_latency_ms", "#d62728")]:
    fig3.add_trace(go.Bar(name=pct.replace("_", " ").title(), x=latency_data["target_service"], y=latency_data[pct], marker_color=color))

fig3.update_layout(
    barmode="group",
    title="Average Latency Percentiles by Target Service",
    yaxis_title="Latency (ms)",
    xaxis_title="Target Service",
    width=900, height=450,
)
fig3.show()
try:
    fig3.write_image(os.path.join(SLIDES_DIR, "chart_latency.png"), scale=2)
except Exception as e:
    print(f"⚠️  Could not save image (kaleido may not be installed): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 4: Hourly Traffic Volume & Error Rate (Dual Axis)

# COMMAND ----------

hourly = gold_pd.groupby("hour").agg({"request_count": "sum", "error_count": "sum"}).reset_index()
hourly["error_rate"] = (hourly["error_count"] / hourly["request_count"]).fillna(0)

fig4 = make_subplots(specs=[[{"secondary_y": True}]])
fig4.add_trace(go.Scatter(x=hourly["hour"], y=hourly["request_count"],
               mode="lines+markers", name="Requests", line=dict(color="#1f77b4")), secondary_y=False)
fig4.add_trace(go.Scatter(x=hourly["hour"], y=hourly["error_rate"],
               mode="lines+markers", name="Error Rate", line=dict(color="#d62728", dash="dot")), secondary_y=True)
fig4.update_layout(title="Hourly Traffic Volume & Error Rate", width=950, height=420)
fig4.update_yaxes(title_text="Request Count", secondary_y=False)
fig4.update_yaxes(title_text="Error Rate", secondary_y=True)
fig4.show()
try:
    fig4.write_image(os.path.join(SLIDES_DIR, "chart_hourly_traffic.png"), scale=2)
except Exception as e:
    print(f"⚠️  Could not save image (kaleido may not be installed): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 5: Anomaly Scatter — Error Rate vs Latency

# COMMAND ----------

fig5 = px.scatter(
    anomaly_pd, x="error_rate", y="avg_latency_ms",
    color="is_anomaly" if "is_anomaly" in anomaly_pd.columns else None,
    size="request_count",
    hover_data=["source_service", "target_service", "endpoint"],
    title="Anomaly Landscape: Error Rate vs Latency",
    color_discrete_map={True: "#d62728", False: "#1f77b4"},
    labels={"error_rate": "Error Rate", "avg_latency_ms": "Avg Latency (ms)"},
)
fig5.update_layout(width=900, height=500)
fig5.show()
try:
    fig5.write_image(os.path.join(SLIDES_DIR, "chart_anomaly_scatter.png"), scale=2)
except Exception as e:
    print(f"⚠️  Could not save image (kaleido may not be installed): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 6: Service Flow Sankey Diagram

# COMMAND ----------

# Build Sankey from top 20 edges by volume
top_edges = gold_pd.groupby(["source_service", "target_service"])["request_count"].sum() \
    .reset_index().sort_values("request_count", ascending=False).head(20)

all_nodes = list(set(top_edges["source_service"].tolist() + top_edges["target_service"].tolist()))
node_idx = {n: i for i, n in enumerate(all_nodes)}

fig6 = go.Figure(go.Sankey(
    node=dict(pad=15, thickness=20, label=all_nodes,
              color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
                     "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"][:len(all_nodes)]),
    link=dict(
        source=[node_idx[s] for s in top_edges["source_service"]],
        target=[node_idx[t] for t in top_edges["target_service"]],
        value=top_edges["request_count"].tolist(),
    )
))
fig6.update_layout(title="Service Flow Sankey — Top 20 Edges", width=900, height=500)
fig6.show()
try:
    fig6.write_image(os.path.join(SLIDES_DIR, "chart_sankey.png"), scale=2)
except Exception as e:
    print(f"⚠️  Could not save image (kaleido may not be installed): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🤖 PART 2 — AI Dashboard Review Agent
# MAGIC ---
# MAGIC
# MAGIC An LLM agent reviews each chart's underlying data and provides
# MAGIC **critique, insights, and recommendations** like a senior SRE analyst.

# COMMAND ----------

# ── LLM helper ─────────────────────────────────────────────────────────
def ask_llm(prompt, system_msg="You are a senior SRE analyst reviewing observability dashboards.", max_tokens=600):
    """Send a prompt to the LLM. Falls back to rule-based summary if LLM unavailable."""
    if LLM_AVAILABLE:
        try:
            response = llm_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.4,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"[LLM Error: {e}] — Falling back to rule-based analysis."
    else:
        return None  # caller should handle fallback

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent Review: Heatmap

# COMMAND ----------

# ── Prepare data summary for the LLM ──────────────────────────────────
heatmap_summary = pivot.to_string()

prompt_heatmap = f"""Review this service dependency heatmap data and provide:
1. Key observations (which services are tightly coupled)
2. Risk assessment (single points of failure)
3. Recommendations for the SRE team

Data (source → target request counts):
{heatmap_summary}
"""

review_heatmap = ask_llm(prompt_heatmap)
if review_heatmap is None:
    # Rule-based fallback
    busiest_src = gold_pd.groupby("source_service")["request_count"].sum().idxmax()
    busiest_tgt = gold_pd.groupby("target_service")["request_count"].sum().idxmax()
    review_heatmap = (
        f"📌 Rule-based analysis:\n"
        f"• Busiest source service: {busiest_src}\n"
        f"• Busiest target service: {busiest_tgt}\n"
        f"• {len(pivot.columns)} unique target services detected\n"
        f"• Recommendation: Monitor the top callers for circuit-breaker readiness."
    )

print("🤖 AI Dashboard Review — Heatmap\n")
print(review_heatmap)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent Review: Error Rate Chart

# COMMAND ----------

error_summary = edge_errors[["edge", "error_rate", "request_count"]].to_string(index=False)

prompt_errors = f"""Analyze these service edge error rates and provide:
1. Which edges are most concerning and why
2. Possible root causes
3. Immediate action items for the on-call engineer

Top 15 edges by error rate:
{error_summary}
"""

review_errors = ask_llm(prompt_errors)
if review_errors is None:
    high_err = edge_errors[edge_errors["error_rate"] > 0.05]
    review_errors = (
        f"📌 Rule-based analysis:\n"
        f"• {len(high_err)} edges have error rate > 5%\n"
        f"• Highest error edge: {edge_errors.iloc[0]['edge']} ({edge_errors.iloc[0]['error_rate']:.2%})\n"
        f"• Action: Investigate top-3 error edges for upstream dependency issues."
    )

print("🤖 AI Dashboard Review — Error Rates\n")
print(review_errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent Review: Anomaly Analysis

# COMMAND ----------

anomaly_stats = anomaly_pd[anomaly_pd.get("is_anomaly", False) == True] if "is_anomaly" in anomaly_pd.columns else anomaly_pd.head(0)
avg_score = anomaly_stats['anomaly_score'].mean() if len(anomaly_stats) > 0 else 0

prompt_anomalies = f"""You are reviewing anomaly detection results from a K-Means model on service flow data.
Total edges: {len(anomaly_pd)}
Anomalies detected: {len(anomaly_stats)} ({len(anomaly_stats)/max(len(anomaly_pd),1)*100:.1f}%)
Average anomaly score: {avg_score:.3f}

Top anomalies (service, error_rate, latency, score):
{anomaly_stats.nlargest(10, 'anomaly_score')[['source_service','target_service','error_rate','avg_latency_ms','anomaly_score']].to_string(index=False) if len(anomaly_stats) > 0 else 'None'}

Provide:
1. Assessment of model quality
2. Which anomalies look like real incidents vs noise
3. Tuning suggestions
"""

review_anomalies = ask_llm(prompt_anomalies)
if review_anomalies is None:
    review_anomalies = (
        f"📌 Rule-based analysis:\n"
        f"• {len(anomaly_stats)} anomalies detected out of {len(anomaly_pd)} edges\n"
        f"• Detection rate: {len(anomaly_stats)/max(len(anomaly_pd),1)*100:.1f}%\n"
        f"• Recommendation: Review high-score anomalies manually; consider adjusting K or threshold."
    )

print("🤖 AI Dashboard Review — Anomalies\n")
print(review_anomalies)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎙️ PART 3 — Presentation Narrator (Text-to-Speech)
# MAGIC ---
# MAGIC
# MAGIC Generates analyst-style spoken narration for each chart —
# MAGIC like having an SRE present the dashboard in a meeting.

# COMMAND ----------

def generate_narration(chart_name, data_summary_text):
    """Generate narration text using LLM or rule-based."""
    prompt = f"""You are a data analyst presenting an observability dashboard in a team meeting.
Write a short (3-4 sentence) spoken narration for this chart. Be professional and insightful.
Chart: {chart_name}
Data summary: {data_summary_text}
"""
    narration = ask_llm(prompt, system_msg="You are a data analyst giving a live dashboard presentation.", max_tokens=200)
    if narration is None:
        narration = f"This chart shows the {chart_name}. {data_summary_text}"
    return narration


def text_to_audio(text, filename):
    """Convert text to MP3 audio file."""
    if not HAS_GTTS:
        print(f"   ⚠️  gTTS unavailable — skipping audio for {filename}")
        return None
    path = os.path.join(AUDIO_DIR, filename)
    tts = gTTS(text=text, lang="en", slow=False)
    tts.save(path)
    return path


# ── Generate narrations for all charts ─────────────────────────────────────────
narrations = {}

max_anomaly_score = anomaly_stats['anomaly_score'].max() if len(anomaly_stats) > 0 else 0

chart_data = {
    "Service Dependency Heatmap": f"Shows request volume between {len(pivot.columns)} services. Busiest edge has {int(pivot.values.max())} requests.",
    "Error Rate by Service Edge": f"Top error edge: {edge_errors.iloc[0]['edge']} at {edge_errors.iloc[0]['error_rate']:.2%}. {len(edge_errors[edge_errors['error_rate'] > 0.05])} edges above 5%.",
    "Latency Percentiles": f"P99 latency ranges from {latency_data['p99_latency_ms'].min():.0f}ms to {latency_data['p99_latency_ms'].max():.0f}ms across services.",
    "Hourly Traffic & Error Rate": f"Total requests: {hourly['request_count'].sum():,.0f}. Peak hour has {hourly['request_count'].max():,.0f} requests.",
    "Anomaly Landscape": f"{len(anomaly_stats)} anomalies detected. Highest anomaly score: {max_anomaly_score:.2f}.",
    "Service Flow Sankey": f"Top flow: {top_edges.iloc[0]['source_service']} → {top_edges.iloc[0]['target_service']} with {top_edges.iloc[0]['request_count']:,} requests."
}

print("🎙️ Generating narrations …\n")
for chart_name, data_summary in chart_data.items():
    narration_text = generate_narration(chart_name, data_summary)
    narrations[chart_name] = narration_text
    print(f"📝 {chart_name}:")
    print(f"   {narration_text}\n")
    audio_file = text_to_audio(narration_text, f"narration_{chart_name.lower().replace(' ', '_')}.mp3")
    if audio_file:
        print(f"   🔊 Audio: {audio_file}")

print("\n✅ Narrations complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🧠 PART 4 — LLM Chart Interpretation
# MAGIC ---
# MAGIC
# MAGIC Feeds the full dataset statistics to the LLM for deep interpretation
# MAGIC — patterns, correlations, and actionable insights.

# COMMAND ----------

# ── Compile comprehensive data profile ─────────────────────────────────────────
total_requests   = gold_pd["request_count"].sum()
total_errors     = gold_pd["error_count"].sum()
overall_err_rate = total_errors / total_requests if total_requests > 0 else 0
num_services     = gold_pd["source_service"].nunique() + gold_pd["target_service"].nunique()
num_edges        = len(gold_pd.groupby(["source_service", "target_service"]))
avg_latency      = gold_pd["avg_latency_ms"].mean()
p95_lat          = gold_pd["p95_latency_ms"].mean()
p99_lat          = gold_pd["p99_latency_ms"].mean()
num_anomalies    = len(anomaly_stats)
anomaly_rate     = num_anomalies / max(len(anomaly_pd), 1) * 100

data_profile = f"""
Observability Pipeline — Data Profile
═══════════════════════════════════════
Total requests:          {total_requests:,.0f}
Total errors:            {total_errors:,.0f}
Overall error rate:      {overall_err_rate:.2%}
Unique services:         {num_services}
Service edges:           {num_edges}
Avg latency:             {avg_latency:.1f} ms
P95 latency (avg):       {p95_lat:.1f} ms
P99 latency (avg):       {p99_lat:.1f} ms
Anomalies detected:      {num_anomalies} ({anomaly_rate:.1f}%)
Gold records:            {len(gold_pd):,}
Anomaly records:         {len(anomaly_pd):,}

Top 5 edges by request volume:
{gold_pd.groupby(['source_service','target_service'])['request_count'].sum().nlargest(5).to_string()}

Top 5 edges by error rate:
{gold_pd.groupby(['source_service','target_service'])['error_rate'].mean().nlargest(5).to_string()}

Latency by service:
{gold_pd.groupby('target_service')[['avg_latency_ms','p95_latency_ms','p99_latency_ms']].mean().round(1).to_string()}
"""

print(data_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deep Interpretation

# COMMAND ----------

prompt_interpret = f"""You are a principal SRE engineer analyzing an observability platform's pipeline output.
Given the following data profile, provide a comprehensive interpretation covering:

1. **System Health Summary** (1 paragraph)
2. **Critical Findings** (top 3 issues with severity ratings)
3. **Correlation Analysis** (e.g., do high-error services also have high latency?)
4. **Capacity Planning** (any services nearing saturation?)
5. **Recommendations** (prioritized action items for the engineering team)

{data_profile}
"""

interpretation = ask_llm(prompt_interpret, max_tokens=1000)
if interpretation is None:
    # Rule-based deep interpretation
    high_err_edges = gold_pd[gold_pd["error_rate"] > 0.05]
    high_lat_services = gold_pd[gold_pd["p99_latency_ms"] > p99_lat * 1.5]["target_service"].unique()
    interpretation = f"""📌 Rule-Based Deep Interpretation

1. **System Health Summary**
   The pipeline processed {total_requests:,.0f} requests across {num_edges} service edges.
   Overall error rate is {overall_err_rate:.2%}. Average latency is {avg_latency:.1f}ms.

2. **Critical Findings**
   • {len(high_err_edges)} edges have error rate > 5% — investigate immediately
   • P99 latency averages {p99_lat:.0f}ms — some services may have tail latency issues
   • {num_anomalies} anomalies flagged by ML model ({anomaly_rate:.1f}% detection rate)

3. **Correlation Analysis**
   • Services with high latency: {', '.join(high_lat_services[:3]) if len(high_lat_services) > 0 else 'None identified'}
   • Error rate and latency correlation should be monitored

4. **Capacity Planning**
   • Monitor top traffic sources for scaling needs

5. **Recommendations**
   • Prioritize investigating high-error-rate edges
   • Set up latency SLOs based on P95/P99 data
   • Review anomaly detector threshold tuning
"""

print("🧠 LLM Chart & Data Interpretation\n")
print(interpretation)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📝 PART 5 — Automated Detailed Report
# MAGIC ---
# MAGIC
# MAGIC Generates a comprehensive HTML report with embedded charts,
# MAGIC AI commentary, and key metrics.

# COMMAND ----------

# ── Collect all AI reviews ─────────────────────────────────────────────
all_reviews = {
    "heatmap": review_heatmap,
    "error_rates": review_errors,
    "anomalies": review_anomalies,
    "interpretation": interpretation,
}

report_html_template = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Observability Pipeline — AI Analytics Report</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; max-width: 1100px; margin: 40px auto; padding: 0 20px; color: #1a1a2e; background: #f8f9fa; }
  h1 { color: #0f3460; border-bottom: 3px solid #e94560; padding-bottom: 10px; }
  h2 { color: #16213e; margin-top: 40px; }
  h3 { color: #0f3460; }
  .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 20px 0; }
  .metric-card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }
  .metric-card .value { font-size: 2em; font-weight: bold; color: #e94560; }
  .metric-card .label { font-size: 0.9em; color: #666; margin-top: 5px; }
  .chart-section { background: white; border-radius: 10px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .chart-section img { max-width: 100%%; border-radius: 6px; }
  .ai-review { background: #e8f4f8; border-left: 4px solid #0f3460; padding: 15px 20px; margin: 15px 0; border-radius: 0 8px 8px 0; white-space: pre-wrap; }
  .ai-review::before { content: "🤖 AI Analysis"; display: block; font-weight: bold; margin-bottom: 8px; color: #0f3460; }
  .narration { background: #fff3e0; border-left: 4px solid #ff9800; padding: 12px 18px; margin: 10px 0; border-radius: 0 8px 8px 0; font-style: italic; }
  .narration::before { content: "🎙️ Narration"; display: block; font-weight: bold; font-style: normal; margin-bottom: 5px; color: #e65100; }
  table { border-collapse: collapse; width: 100%%; margin: 15px 0; }
  th, td { border: 1px solid #ddd; padding: 10px 14px; text-align: left; }
  th { background: #0f3460; color: white; }
  tr:nth-child(even) { background: #f2f2f2; }
  .footer { text-align: center; color: #999; margin-top: 50px; padding: 20px; border-top: 1px solid #ddd; }
</style>
</head>
<body>
<h1>📊 Observability Pipeline — AI Analytics Report</h1>
<p><strong>Generated:</strong> %(timestamp)s &nbsp;|&nbsp; <strong>Pipeline:</strong> Bronze → Silver → Gold → Analytics</p>

<div class="metric-grid">
  <div class="metric-card"><div class="value">%(total_requests)s</div><div class="label">Total Requests</div></div>
  <div class="metric-card"><div class="value">%(overall_err_rate)s</div><div class="label">Error Rate</div></div>
  <div class="metric-card"><div class="value">%(avg_latency)s ms</div><div class="label">Avg Latency</div></div>
  <div class="metric-card"><div class="value">%(p99_lat)s ms</div><div class="label">P99 Latency</div></div>
  <div class="metric-card"><div class="value">%(num_edges)s</div><div class="label">Service Edges</div></div>
  <div class="metric-card"><div class="value">%(num_anomalies)s</div><div class="label">Anomalies</div></div>
</div>

<h2>1. Service Dependency Heatmap</h2>
<div class="chart-section"><img src="slides/chart_heatmap.png" alt="Heatmap"></div>
<div class="ai-review">%(review_heatmap)s</div>
<div class="narration">%(narration_heatmap)s</div>

<h2>2. Error Rate Analysis</h2>
<div class="chart-section"><img src="slides/chart_error_rate.png" alt="Error Rate"></div>
<div class="ai-review">%(review_errors)s</div>
<div class="narration">%(narration_errors)s</div>

<h2>3. Latency Percentiles</h2>
<div class="chart-section"><img src="slides/chart_latency.png" alt="Latency"></div>
<div class="narration">%(narration_latency)s</div>

<h2>4. Hourly Traffic & Error Rate</h2>
<div class="chart-section"><img src="slides/chart_hourly_traffic.png" alt="Hourly Traffic"></div>
<div class="narration">%(narration_traffic)s</div>

<h2>5. Anomaly Detection Results</h2>
<div class="chart-section"><img src="slides/chart_anomaly_scatter.png" alt="Anomaly Scatter"></div>
<div class="ai-review">%(review_anomalies)s</div>
<div class="narration">%(narration_anomaly)s</div>

<h2>6. Service Flow Diagram</h2>
<div class="chart-section"><img src="slides/chart_sankey.png" alt="Sankey"></div>
<div class="narration">%(narration_sankey)s</div>

<h2>7. Deep Interpretation</h2>
<div class="ai-review">%(interpretation)s</div>

<h2>8. Data Summary Table</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Total Requests</td><td>%(total_requests)s</td></tr>
<tr><td>Total Errors</td><td>%(total_errors)s</td></tr>
<tr><td>Overall Error Rate</td><td>%(overall_err_rate)s</td></tr>
<tr><td>Service Edges</td><td>%(num_edges)s</td></tr>
<tr><td>Avg Latency</td><td>%(avg_latency)s ms</td></tr>
<tr><td>P95 Latency</td><td>%(p95_lat)s ms</td></tr>
<tr><td>P99 Latency</td><td>%(p99_lat)s ms</td></tr>
<tr><td>Anomalies</td><td>%(num_anomalies)s (%(anomaly_rate)s)</td></tr>
<tr><td>Gold Records</td><td>%(gold_count)s</td></tr>
</table>

<div class="footer">
  <p>Generated by <strong>DS-610 Observability Pipeline</strong> — AI-Powered Analytics Module</p>
  <p>Data Source: LogHub + Synthetic | Platform: Databricks + Apache Spark + Delta Lake</p>
</div>
</body>
</html>
"""

# ── Fill in the template ───────────────────────────────────────────────
narr_keys = list(narrations.values())

report_html = report_html_template % {
    "timestamp":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_requests":  f"{total_requests:,.0f}",
    "overall_err_rate": f"{overall_err_rate:.2%}",
    "avg_latency":     f"{avg_latency:.1f}",
    "p99_lat":         f"{p99_lat:.1f}",
    "p95_lat":         f"{p95_lat:.1f}",
    "num_edges":       f"{num_edges}",
    "num_anomalies":   f"{num_anomalies}",
    "anomaly_rate":    f"{anomaly_rate:.1f}%",
    "total_errors":    f"{total_errors:,.0f}",
    "gold_count":      f"{len(gold_pd):,}",
    "review_heatmap":  review_heatmap.replace("\n", "\n"),
    "review_errors":   review_errors.replace("\n", "\n"),
    "review_anomalies": review_anomalies.replace("\n", "\n"),
    "interpretation":  interpretation.replace("\n", "\n"),
    "narration_heatmap": narr_keys[0] if len(narr_keys) > 0 else "",
    "narration_errors":  narr_keys[1] if len(narr_keys) > 1 else "",
    "narration_latency": narr_keys[2] if len(narr_keys) > 2 else "",
    "narration_traffic": narr_keys[3] if len(narr_keys) > 3 else "",
    "narration_anomaly": narr_keys[4] if len(narr_keys) > 4 else "",
    "narration_sankey":  narr_keys[5] if len(narr_keys) > 5 else "",
}

report_path = os.path.join(OUTPUT_DIR, "ai_analytics_report.html")
with open(report_path, "w") as f:
    f.write(report_html)

print(f"✅ Report saved: {report_path}")
print(f"   Open in browser to view full interactive report")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 💬 PART 6 — Data Copilot (Natural Language Chat)
# MAGIC ---
# MAGIC
# MAGIC A conversational interface that lets users ask questions about the data
# MAGIC in plain English. The copilot translates questions into Spark SQL / Pandas
# MAGIC queries and returns answers with context.

# COMMAND ----------

# ── Data Copilot Class ─────────────────────────────────────────────────
class DataCopilot:
    """
    Natural-language interface to the observability pipeline data.
    Uses LLM when available; falls back to keyword matching otherwise.
    """

    def __init__(self, gold_pdf, anomaly_pdf, silver_pdf=None):
        self.gold    = gold_pdf
        self.anomaly = anomaly_pdf
        self.silver  = silver_pdf
        self.history = []

        # Pre-compute common stats
        self.stats = {
            "total_requests":   int(gold_pdf["request_count"].sum()),
            "total_errors":     int(gold_pdf["error_count"].sum()),
            "overall_err_rate": round(gold_pdf["error_count"].sum() / max(gold_pdf["request_count"].sum(), 1), 4),
            "services":         sorted(set(gold_pdf["source_service"].unique().tolist() + gold_pdf["target_service"].unique().tolist())),
            "num_edges":        len(gold_pdf.groupby(["source_service", "target_service"])),
            "avg_latency":      round(gold_pdf["avg_latency_ms"].mean(), 1),
            "p95_latency":      round(gold_pdf["p95_latency_ms"].mean(), 1),
            "p99_latency":      round(gold_pdf["p99_latency_ms"].mean(), 1),
            "num_anomalies":    int(anomaly_pdf["is_anomaly"].sum()) if "is_anomaly" in anomaly_pdf.columns else 0,
        }

    # ── schema description for the LLM ────────────────────────────────
    def _schema_context(self):
        return f"""Available data:
1. gold — columns: {list(self.gold.columns)} — {len(self.gold)} rows (hourly service edge aggregations)
2. anomaly — columns: {list(self.anomaly.columns)} — {len(self.anomaly)} rows (anomaly detection results)
{"3. silver — columns: " + str(list(self.silver.columns)) + " — " + str(len(self.silver)) + " rows (enriched events)" if self.silver is not None else ""}

Pre-computed stats: {json.dumps(self.stats, indent=2)}
"""

    # ── ask the copilot ────────────────────────────────────────────────
    def ask(self, question):
        """Answer a natural language question about the data."""
        self.history.append({"role": "user", "question": question})

        # Try LLM path
        if LLM_AVAILABLE:
            answer = self._ask_llm(question)
        else:
            answer = self._ask_rules(question)

        self.history.append({"role": "copilot", "answer": answer})
        return answer

    def _ask_llm(self, question):
        schema = self._schema_context()
        prompt = f"""{schema}

The user asks: "{question}"

Write a Python expression using the DataFrames `self.gold`, `self.anomaly`, or `self.silver` (pandas DataFrames) that answers the question.
Return ONLY a JSON with two keys:
  "code": <python expression as string>
  "explanation": <one-sentence explanation>

Use only pandas operations. Do NOT import anything. The result should be printable.
"""
        try:
            resp = llm_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a data analyst copilot. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=400,
                temperature=0.1,
            )
            raw = resp.choices[0].message.content.strip()
            # Extract JSON from possible markdown code block
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            parsed = json.loads(raw)
            code = parsed.get("code", "")
            explanation = parsed.get("explanation", "")

            # Execute safely
            try:
                result = eval(code, {"self": self, "pd": pd, "np": np})
                return f"📊 **Answer**: {result}\n\n💡 {explanation}\n\n```python\n{code}\n```"
            except Exception as exec_err:
                return f"⚠️ Generated code failed: {exec_err}\n\nCode: {code}\n\nExplanation: {explanation}"
        except Exception as e:
            return f"⚠️ LLM error: {e}\n\nFalling back to rule-based …\n\n{self._ask_rules(question)}"

    def _ask_rules(self, question):
        """Keyword-based fallback when LLM is unavailable."""
        q = question.lower()

        if any(w in q for w in ["error rate", "error", "errors", "failure"]):
            top_err = self.gold.groupby(["source_service", "target_service"])["error_rate"].mean() \
                .nlargest(5).reset_index()
            top_err["edge"] = top_err["source_service"] + " → " + top_err["target_service"]
            return f"📊 Overall error rate: {self.stats['overall_err_rate']:.2%}\n\nTop 5 error edges:\n{top_err[['edge','error_rate']].to_string(index=False)}"

        elif any(w in q for w in ["latency", "slow", "p95", "p99", "response time"]):
            lat = self.gold.groupby("target_service")[["avg_latency_ms", "p95_latency_ms", "p99_latency_ms"]].mean().round(1)
            return f"📊 Latency by service:\n{lat.to_string()}"

        elif any(w in q for w in ["anomaly", "anomalies", "unusual", "outlier"]):
            return f"📊 Anomalies: {self.stats['num_anomalies']} detected ({self.stats['num_anomalies']/max(len(self.anomaly),1)*100:.1f}%)\n\nTop anomalies:\n{self.anomaly.nlargest(5, 'anomaly_score')[['source_service','target_service','anomaly_score','error_rate']].to_string(index=False) if 'anomaly_score' in self.anomaly.columns else 'N/A'}"

        elif any(w in q for w in ["traffic", "volume", "requests", "busiest", "top"]):
            top_vol = self.gold.groupby(["source_service", "target_service"])["request_count"].sum() \
                .nlargest(5).reset_index()
            top_vol["edge"] = top_vol["source_service"] + " → " + top_vol["target_service"]
            return f"📊 Total requests: {self.stats['total_requests']:,}\n\nTop 5 busiest edges:\n{top_vol[['edge','request_count']].to_string(index=False)}"

        elif any(w in q for w in ["service", "services", "list"]):
            return f"📊 Services ({len(self.stats['services'])}): {', '.join(self.stats['services'])}"

        elif any(w in q for w in ["summary", "overview", "health", "status"]):
            return (
                f"📊 Pipeline Health Summary:\n"
                f"• Requests: {self.stats['total_requests']:,}\n"
                f"• Errors: {self.stats['total_errors']:,} ({self.stats['overall_err_rate']:.2%})\n"
                f"• Avg Latency: {self.stats['avg_latency']}ms\n"
                f"• P99 Latency: {self.stats['p99_latency']}ms\n"
                f"• Service Edges: {self.stats['num_edges']}\n"
                f"• Anomalies: {self.stats['num_anomalies']}"
            )
        else:
            return (
                f"🤔 I'm not sure how to answer that. Try asking about:\n"
                f"• Error rates (e.g., 'What are the top error edges?')\n"
                f"• Latency (e.g., 'Which service has the highest P99 latency?')\n"
                f"• Traffic (e.g., 'What are the busiest service edges?')\n"
                f"• Anomalies (e.g., 'How many anomalies were detected?')\n"
                f"• Health (e.g., 'Give me a system health summary')"
            )

# ── Instantiate the copilot ─────────────────────────────────────────────────────────────────────────
copilot = DataCopilot(gold_pd, anomaly_pd, silver_pd)
print("✅ Data Copilot ready!")
print("   Use copilot.ask('your question') to chat with the data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💬 Try the Data Copilot
# MAGIC
# MAGIC Run the cells below to interact. Change the question to anything!

# COMMAND ----------

print(copilot.ask("Give me a system health summary"))

# COMMAND ----------

print(copilot.ask("What are the top error edges?"))

# COMMAND ----------

print(copilot.ask("Which service has the highest P99 latency?"))

# COMMAND ----------

print(copilot.ask("How many anomalies were detected and what are the worst ones?"))

# COMMAND ----------

print(copilot.ask("What are the busiest service-to-service flows?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💬 Ask Your Own Question
# MAGIC
# MAGIC Type your question below and run the cell:

# COMMAND ----------

# ── Change this to ask anything about the data! ───────────────────────
my_question = "What is the overall error rate and which service should we investigate first?"
print(copilot.ask(my_question))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ Summary
# MAGIC ---

# COMMAND ----------

print("=" * 70)
print("   AI-POWERED DASHBOARD ANALYTICS — COMPLETE")
print("=" * 70)
print()
print(f"📊 Charts Generated:         6 interactive Plotly visualizations")
print(f"🤖 AI Reviews:               3 dashboard sections reviewed")
print(f"🎙️ Narrations Generated:     {len(narrations)} chart narrations")
if HAS_GTTS:
    print(f"🔊 Audio Files:              {len(narrations)} MP3 files in {AUDIO_DIR}")
print(f"🧠 Deep Interpretation:      Complete data profile analysis")
print(f"📝 HTML Report:              {report_path}")
print(f"💬 Data Copilot:             Ready (copilot.ask('...'))")
print()
print(f"📂 All outputs saved to: {OUTPUT_DIR}")
print()
print("Next steps:")
print("  1. Open ai_analytics_report.html in a browser for the full report")
print("  2. Use copilot.ask() to explore data interactively")
print("  3. Share the narration audio files for team presentations")
print("=" * 70)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: AI Dashboard Analytics complete")