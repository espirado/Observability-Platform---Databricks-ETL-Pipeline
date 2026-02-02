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
import re
import tarfile
import urllib.request
from datetime import datetime

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
# MAGIC ## 1) Load LogHub Linux dataset (syslog)

# COMMAND ----------

try:
    dbutils.widgets.text(
        "loghub_path",
        "/dbfs/loghub/Linux/*.log",
        "LogHub Linux Log Path",
    )
    loghub_path = dbutils.widgets.get("loghub_path")
except Exception:
    loghub_path = "/dbfs/loghub/Linux/*.log"

log_files = sorted(glob.glob(loghub_path))

if not log_files:
    # Download + extract the LogHub Linux dataset into DBFS
    linux_tar_path = "/dbfs/loghub/Linux.tar.gz"
    linux_extract_path = "/dbfs/loghub/Linux"
    os.makedirs(linux_extract_path, exist_ok=True)

    if not os.path.exists(linux_tar_path):
        linux_url = "https://zenodo.org/records/8196385/files/Linux.tar.gz?download=1"
        print("Downloading LogHub Linux dataset...")
        urllib.request.urlretrieve(linux_url, linux_tar_path)
        print(f"Saved: {linux_tar_path}")

    print("Extracting LogHub Linux dataset...")
    with tarfile.open(linux_tar_path, "r:gz") as tar:
        tar.extractall(linux_extract_path)

    log_files = sorted(glob.glob(loghub_path))
    if not log_files:
        raise FileNotFoundError(
            f"No LogHub Linux log files found at: {loghub_path}. "
            "Check extraction path or update the widget path."
        )

lines = []
for path in log_files:
    with open(path, "r") as f:
        lines.extend([line.strip() for line in f.readlines() if line.strip()])

log_pattern = re.compile(
    r"^(?P<month>\\w{3})\\s+(?P<day>\\d{1,2})\\s"
    r"(?P<time>\\d{2}:\\d{2}:\\d{2})\\s"
    r"(?P<host>\\S+)\\s"
    r"(?P<process>[^\\[:]+)(?:\\[(?P<pid>\\d+)\\])?:\\s"
    r"(?P<message>.*)$"
)

records = []
current_year = datetime.now().year
for line in lines:
    match = log_pattern.match(line)
    if not match:
        continue
    parts = match.groupdict()
    ts_str = f"{current_year} {parts['month']} {parts['day']} {parts['time']}"
    try:
        timestamp = datetime.strptime(ts_str, "%Y %b %d %H:%M:%S")
    except Exception:
        timestamp = None
    records.append(
        {
            "timestamp": timestamp,
            "host": parts["host"],
            "process": parts["process"],
            "message": parts["message"],
        }
    )

df = pd.DataFrame(records).dropna(subset=["timestamp"])

def classify_severity(message):
    msg = str(message).lower()
    if any(k in msg for k in ["error", "failed", "panic", "critical"]):
        return "error"
    if any(k in msg for k in ["warn", "deprecated"]):
        return "warn"
    return "info"


df["severity"] = df["message"].apply(classify_severity)
df["hour"] = df["timestamp"].dt.floor("H")

print(f"Loaded {len(df):,} Linux syslog events from LogHub.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Build dashboard charts

# COMMAND ----------

output_dir = "outputs/dashboard_video"
slides_dir = os.path.join(output_dir, "slides")
audio_dir = os.path.join(output_dir, "audio")

os.makedirs(slides_dir, exist_ok=True)
os.makedirs(audio_dir, exist_ok=True)

# Chart 1: Logs over time
logs_over_time = df.groupby("hour").size().reset_index(name="log_count")

# Chart 2: Severity distribution
severity_counts = (
    df.groupby("severity").size().reset_index(name="log_count")
    .sort_values("log_count", ascending=False)
)

# Chart 3: Top processes by volume
top_processes = (
    df.groupby("process").size().reset_index(name="log_count")
    .sort_values("log_count", ascending=False)
    .head(8)
)

# Chart 4: Top hosts by volume
top_hosts = (
    df.groupby("host").size().reset_index(name="log_count")
    .sort_values("log_count", ascending=False)
    .head(8)
)

# Create a 2x2 dashboard
fig, axes = plt.subplots(2, 2, figsize=(12, 8))
fig.suptitle("Observability Dashboard (LogHub Data)", fontsize=16)

axes[0, 0].plot(logs_over_time["hour"], logs_over_time["log_count"], color="#1f77b4")
axes[0, 0].set_title("Logs Over Time")
axes[0, 0].set_xlabel("Hour")
axes[0, 0].set_ylabel("Log Count")
axes[0, 0].tick_params(axis="x", rotation=30)

axes[0, 1].bar(severity_counts["severity"], severity_counts["log_count"], color="#d62728")
axes[0, 1].set_title("Severity Distribution")
axes[0, 1].set_xlabel("Severity")
axes[0, 1].set_ylabel("Log Count")

axes[1, 0].bar(top_processes["process"], top_processes["log_count"], color="#2ca02c")
axes[1, 0].set_title("Top Processes by Volume")
axes[1, 0].set_xlabel("Process")
axes[1, 0].set_ylabel("Log Count")
axes[1, 0].tick_params(axis="x", rotation=20)

axes[1, 1].bar(top_hosts["host"], top_hosts["log_count"], color="#9467bd")
axes[1, 1].set_title("Top Hosts by Volume")
axes[1, 1].set_xlabel("Host")
axes[1, 1].set_ylabel("Log Count")
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
    ax.plot(logs_over_time["hour"], logs_over_time["log_count"], color="#1f77b4")
    ax.set_xlabel("Hour")
    ax.set_ylabel("Log Count")
    ax.tick_params(axis="x", rotation=30)


def chart_errors(ax):
    ax.bar(severity_counts["severity"], severity_counts["log_count"], color="#d62728")
    ax.set_xlabel("Severity")
    ax.set_ylabel("Log Count")


def chart_latency(ax):
    ax.bar(top_processes["process"], top_processes["log_count"], color="#2ca02c")
    ax.set_xlabel("Process")
    ax.set_ylabel("Log Count")
    ax.tick_params(axis="x", rotation=20)


def chart_endpoints(ax):
    ax.bar(top_hosts["host"], top_hosts["log_count"], color="#9467bd")
    ax.set_xlabel("Host")
    ax.set_ylabel("Log Count")
    ax.tick_params(axis="x", rotation=20)


top_severity = severity_counts.iloc[0]["severity"] if len(severity_counts) else "unknown"
top_process = top_processes.iloc[0]["process"] if len(top_processes) else "unknown"
top_host = top_hosts.iloc[0]["host"] if len(top_hosts) else "unknown"
if len(logs_over_time) > 0:
    peak_hour = logs_over_time.loc[logs_over_time["log_count"].idxmax(), "hour"]
    peak_hour_str = peak_hour.strftime("%H:00")
else:
    peak_hour_str = "unknown time"

slides = [
    {
        "path": os.path.join(slides_dir, "slide_1.png"),
        "title": "Logs Over Time",
        "subtitle": "Shows log volume across the time window.",
        "chart": chart_requests,
        "narration": (
            "This chart shows total log volume over time. "
            f"Traffic peaks around {peak_hour_str}."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_2.png"),
        "title": "Severity Distribution",
        "subtitle": "Shows how many logs are info, warn, or error.",
        "chart": chart_errors,
        "narration": (
            "Here we compare log counts by severity. "
            f"The most common severity is {top_severity}."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_3.png"),
        "title": "Top Processes by Volume",
        "subtitle": "Highlights which processes are most active.",
        "chart": chart_latency,
        "narration": (
            "This chart shows which processes generate the most logs. "
            f"The busiest process is {top_process}."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_4.png"),
        "title": "Top Hosts by Volume",
        "subtitle": "Shows which machines produce the most logs.",
        "chart": chart_endpoints,
        "narration": (
            "These are the noisiest hosts in the dataset. "
            f"The top host is {top_host}."
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
