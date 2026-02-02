# Databricks notebook source
# MAGIC %md
# MAGIC # 10: Dashboard + Narrated Video (LogHub Data)
# MAGIC
# MAGIC **Goal**: Build a simple dashboard from LogHub output data (JSONL) and
# MAGIC generate a short narrated video that explains what each chart shows.
# MAGIC
# MAGIC **Output**:
# MAGIC - `outputs/dashboard_video/dashboard.png`
# MAGIC - `outputs/dashboard_video/slides/slide_*.png`
# MAGIC - `outputs/dashboard_video/dashboard_explainer.mp4`

# COMMAND ----------

import os
import glob
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Optional dependencies (used for narration/video)
try:
    import pyttsx3  # offline text-to-speech
except Exception:
    pyttsx3 = None

try:
    from moviepy.editor import ImageClip, AudioFileClip, concatenate_videoclips
except Exception:
    ImageClip = AudioFileClip = concatenate_videoclips = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Load LogHub output (JSONL)

# COMMAND ----------

try:
    dbutils.widgets.text(
        "loghub_path",
        "/dbfs/observability-data/raw-logs/aws_cloudwatch_*.jsonl",
        "LogHub JSONL Path (AWS CloudWatch)",
    )
    loghub_path = dbutils.widgets.get("loghub_path")
except Exception:
    loghub_path = "/dbfs/observability-data/raw-logs/aws_cloudwatch_*.jsonl"

jsonl_files = sorted(glob.glob(loghub_path))

if not jsonl_files:
    raise FileNotFoundError(
        f"No LogHub JSONL files found at: {loghub_path}. "
        "Run 00_ingest_from_log_hub.py or update the widget path."
    )

raw_df = pd.concat(
    [pd.read_json(f, lines=True) for f in jsonl_files],
    ignore_index=True,
)


def pick_first(row, candidates, default=None):
    for key in candidates:
        if key in row and pd.notnull(row[key]):
            return row[key]
    return default


def normalize_row(row):
    # CloudWatch ingestion writes parsed JSON (if any) + timestamp + log_stream.
    # If only a raw message is present, use that for service/endpoint if possible.
    message = row.get("message")
    if isinstance(message, str) and message.startswith("{") and message.endswith("}"):
        try:
            message_json = json.loads(message)
            for key, value in message_json.items():
                if key not in row or pd.isnull(row[key]):
                    row[key] = value
        except Exception:
            pass

    timestamp = pick_first(
        row,
        ["timestamp", "@timestamp", "TimeGenerated", "time", "event_time"],
        None,
    )
    service = pick_first(
        row,
        ["service", "serviceName", "app", "Application", "ContainerID", "log_stream"],
        "unknown",
    )
    endpoint = pick_first(
        row,
        ["endpoint", "path", "route", "request_path", "url", "uri"],
        "unknown",
    )
    status_code = pick_first(
        row,
        ["status_code", "status", "httpStatus", "code"],
        None,
    )
    latency_ms = pick_first(
        row,
        ["latency_ms", "latency", "duration_ms", "duration"],
        None,
    )

    return {
        "timestamp": timestamp,
        "service": service,
        "endpoint": endpoint,
        "status_code": status_code,
        "latency_ms": latency_ms,
    }


normalized = raw_df.apply(normalize_row, axis=1, result_type="expand")
normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce")
normalized["status_code"] = pd.to_numeric(
    normalized["status_code"], errors="coerce"
)
normalized["latency_ms"] = pd.to_numeric(
    normalized["latency_ms"], errors="coerce"
)

normalized["is_error"] = normalized["status_code"] >= 500
normalized = normalized.dropna(subset=["timestamp"])
normalized["hour"] = normalized["timestamp"].dt.floor("H")

df = normalized
print(f"Loaded {len(df):,} log events from LogHub JSONL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Build dashboard charts

# COMMAND ----------

output_dir = "outputs/dashboard_video"
slides_dir = os.path.join(output_dir, "slides")
audio_dir = os.path.join(output_dir, "audio")

os.makedirs(slides_dir, exist_ok=True)
os.makedirs(audio_dir, exist_ok=True)

# Chart 1: Requests over time
req_over_time = df.groupby("hour").size().reset_index(name="requests")

# Chart 2: Error rate by service
error_rate = (
    df.groupby("service")["is_error"].mean().reset_index(name="error_rate")
)

# Chart 3: Avg latency by service
avg_latency = (
    df.groupby("service")["latency_ms"].mean().reset_index(name="avg_latency_ms")
)

# Chart 4: Top endpoints by volume
top_endpoints = (
    df.groupby("endpoint").size().reset_index(name="requests")
    .sort_values("requests", ascending=False)
    .head(6)
)

# Create a 2x2 dashboard
fig, axes = plt.subplots(2, 2, figsize=(12, 8))
fig.suptitle("Observability Dashboard (LogHub Data)", fontsize=16)

axes[0, 0].plot(req_over_time["hour"], req_over_time["requests"], color="#1f77b4")
axes[0, 0].set_title("Requests Over Time")
axes[0, 0].set_xlabel("Hour")
axes[0, 0].set_ylabel("Requests")
axes[0, 0].tick_params(axis="x", rotation=30)

axes[0, 1].bar(error_rate["service"], error_rate["error_rate"], color="#d62728")
axes[0, 1].set_title("Error Rate by Service")
axes[0, 1].set_xlabel("Service")
axes[0, 1].set_ylabel("Error Rate")

axes[1, 0].bar(avg_latency["service"], avg_latency["avg_latency_ms"], color="#2ca02c")
axes[1, 0].set_title("Average Latency by Service")
axes[1, 0].set_xlabel("Service")
axes[1, 0].set_ylabel("Latency (ms)")

axes[1, 1].bar(top_endpoints["endpoint"], top_endpoints["requests"], color="#9467bd")
axes[1, 1].set_title("Top Endpoints by Volume")
axes[1, 1].set_xlabel("Endpoint")
axes[1, 1].set_ylabel("Requests")
axes[1, 1].tick_params(axis="x", rotation=20)

plt.tight_layout(rect=[0, 0, 1, 0.96])

dashboard_path = os.path.join(output_dir, "dashboard.png")
plt.savefig(dashboard_path, dpi=160)
plt.close(fig)

print(f"Saved dashboard: {dashboard_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Create narrated slides

# COMMAND ----------

def save_slide(title, subtitle, chart_func, output_path):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_title(title, fontsize=16, pad=15)
    chart_func(ax)
    fig.text(0.5, 0.02, subtitle, ha="center", fontsize=11)
    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    plt.savefig(output_path, dpi=160)
    plt.close(fig)


def chart_requests(ax):
    ax.plot(req_over_time["hour"], req_over_time["requests"], color="#1f77b4")
    ax.set_xlabel("Hour")
    ax.set_ylabel("Requests")
    ax.tick_params(axis="x", rotation=30)


def chart_errors(ax):
    ax.bar(error_rate["service"], error_rate["error_rate"], color="#d62728")
    ax.set_xlabel("Service")
    ax.set_ylabel("Error Rate")


def chart_latency(ax):
    ax.bar(avg_latency["service"], avg_latency["avg_latency_ms"], color="#2ca02c")
    ax.set_xlabel("Service")
    ax.set_ylabel("Latency (ms)")


def chart_endpoints(ax):
    ax.bar(top_endpoints["endpoint"], top_endpoints["requests"], color="#9467bd")
    ax.set_xlabel("Endpoint")
    ax.set_ylabel("Requests")
    ax.tick_params(axis="x", rotation=20)


top_error_service = error_rate.sort_values("error_rate", ascending=False).iloc[0]
top_endpoint = top_endpoints.iloc[0]
if len(req_over_time) > 0:
    peak_hour = req_over_time.loc[req_over_time["requests"].idxmax(), "hour"]
    peak_hour_str = peak_hour.strftime("%H:00")
else:
    peak_hour_str = "unknown time"

slides = [
    {
        "path": os.path.join(slides_dir, "slide_1.png"),
        "title": "Requests Over Time",
        "subtitle": "Shows traffic volume across the last 24 hours.",
        "chart": chart_requests,
        "narration": (
            "This chart shows total requests over time. "
            f"Traffic peaks around {peak_hour_str}."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_2.png"),
        "title": "Error Rate by Service",
        "subtitle": "Highlights which services produce the most errors.",
        "chart": chart_errors,
        "narration": (
            "Here we compare error rates by service. "
            f"{top_error_service['service']} has the highest error rate at "
            f"{top_error_service['error_rate']:.1%}."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_3.png"),
        "title": "Average Latency by Service",
        "subtitle": "Compares response time across services.",
        "chart": chart_latency,
        "narration": (
            "This chart compares average latency. "
            f"{avg_latency.sort_values('avg_latency_ms', ascending=False).iloc[0]['service']} "
            "is currently the slowest service."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_4.png"),
        "title": "Top Endpoints by Volume",
        "subtitle": "Shows the busiest user-facing endpoints.",
        "chart": chart_endpoints,
        "narration": (
            "These are the busiest endpoints. "
            f"{top_endpoint['endpoint']} handled {int(top_endpoint['requests'])} requests."
        ),
    },
]

for slide in slides:
    save_slide(slide["title"], slide["subtitle"], slide["chart"], slide["path"])

print(f"Saved {len(slides)} slides to {slides_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Generate narration audio

# COMMAND ----------

def speak_to_file(text, output_path):
    if pyttsx3 is None:
        return None
    engine = pyttsx3.init()
    engine.setProperty("rate", 165)
    engine.save_to_file(text, output_path)
    engine.runAndWait()
    return output_path


audio_files = []
for idx, slide in enumerate(slides, start=1):
    audio_path = os.path.join(audio_dir, f"slide_{idx}.mp3")
    audio_files.append(speak_to_file(slide["narration"], audio_path))

if pyttsx3 is None:
    print("⚠️  pyttsx3 not available. Narration audio will be skipped.")
else:
    print(f"Generated narration audio in {audio_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Build the video

# COMMAND ----------

if ImageClip is None:
    print("⚠️  moviepy not available. Video will be skipped.")
else:
    clips = []
    for idx, slide in enumerate(slides):
        img_clip = ImageClip(slide["path"])
        audio_path = audio_files[idx]
        if audio_path and os.path.exists(audio_path):
            audio_clip = AudioFileClip(audio_path)
            img_clip = img_clip.set_duration(audio_clip.duration + 0.5)
            img_clip = img_clip.set_audio(audio_clip)
        else:
            img_clip = img_clip.set_duration(6)
        clips.append(img_clip)

    final_video = concatenate_videoclips(clips, method="compose")
    video_path = os.path.join(output_dir, "dashboard_explainer.mp4")
    final_video.write_videofile(video_path, fps=24, audio_codec="aac")

    print(f"Saved video: {video_path}")
