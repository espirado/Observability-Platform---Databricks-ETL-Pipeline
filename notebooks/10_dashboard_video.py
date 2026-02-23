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

# DBTITLE 1,Cell 2
# MAGIC %pip install --upgrade gtts
# MAGIC %pip install --upgrade moviepy
# MAGIC %pip install --upgrade imageio imageio-ffmpeg
# MAGIC
# MAGIC # Restart Python to load newly installed packages
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Cell 2b - Imports
import os
import glob
import re
import tarfile
import urllib.request
from datetime import datetime
import shutil
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# Text-to-speech
try:
    from gtts import gTTS
    print("✓ gTTS available")
except Exception:
    gTTS = None
    print("✗ gTTS not available")

# Video editing - moviepy 2.x uses different import paths
try:
    from moviepy import ImageClip, AudioFileClip, concatenate_videoclips
    print("✓ moviepy available (version 2.x)")
except Exception as e:
    print(f"✗ moviepy import failed: {e}")
    ImageClip = AudioFileClip = concatenate_videoclips = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Load LogHub Linux dataset (syslog)

# COMMAND ----------

# DBTITLE 1,Cell 4
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
    with open(path, "r", encoding="latin-1") as f:
        lines.extend([line.strip() for line in f.readlines() if line.strip()])

log_pattern = re.compile(
    r"^(?P<month>\w{3})\s+(?P<day>\d{1,2})\s"
    r"(?P<time>\d{2}:\d{2}:\d{2})\s"
    r"(?P<host>\S+)\s"
    r"(?P<process>[^\[:]+)(?:\[(?P<pid>\d+)\])?:\s"
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
# MAGIC ## 2) Quick analytics (explore the dataset)

# COMMAND ----------

print("Date range:")
print(f"  Start: {df['timestamp'].min()}")
print(f"  End:   {df['timestamp'].max()}")

print("\nTop 10 processes:")
print(df["process"].value_counts().head(10))

print("\nTop 10 hosts:")
print(df["host"].value_counts().head(10))

print("\nSeverity distribution:")
print(df["severity"].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Build dashboard charts

# COMMAND ----------

output_dir = "/dbfs/observability-data/dashboard_video"
try:
    dbutils.widgets.text(
        "repo_output_path",
        "",
        "Optional Repo Output Path (e.g., /Workspace/Repos/<user>/repo/outputs/dashboard_video)",
    )
    repo_output_path = dbutils.widgets.get("repo_output_path").strip()
except Exception:
    repo_output_path = ""
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
if repo_output_path:
    repo_dash = Path(repo_output_path) / "dashboard.png"
    repo_dash.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(dashboard_path, repo_dash)
    print(f"Copied dashboard to repo: {repo_dash}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Create narrated slides

# COMMAND ----------

# DBTITLE 1,Cell 11
def save_slide(title, subtitle, chart_func, output_path):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_title(title, fontsize=16, pad=15)
    chart_func(ax)
    fig.text(0.5, 0.02, subtitle, ha="center", fontsize=11)
    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    plt.savefig(output_path, dpi=160)
    plt.close(fig)


def chart_intro(ax):
    ax.text(0.5, 0.5, "Observability Dashboard\nLogHub Linux System Logs", 
            ha='center', va='center', fontsize=24, weight='bold')
    ax.axis('off')


def chart_requests(ax):
    ax.plot(logs_over_time["hour"], logs_over_time["log_count"], 
            color="#1f77b4", linewidth=2, marker='o')
    ax.set_xlabel("Hour", fontsize=12)
    ax.set_ylabel("Log Count", fontsize=12)
    ax.tick_params(axis="x", rotation=30)
    ax.grid(True, alpha=0.3)


def chart_errors(ax):
    bars = ax.bar(severity_counts["severity"], severity_counts["log_count"], color="#d62728")
    ax.set_xlabel("Severity", fontsize=12)
    ax.set_ylabel("Log Count", fontsize=12)
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}', ha='center', va='bottom')


def chart_latency(ax):
    bars = ax.bar(top_processes["process"], top_processes["log_count"], color="#2ca02c")
    ax.set_xlabel("Process", fontsize=12)
    ax.set_ylabel("Log Count", fontsize=12)
    ax.tick_params(axis="x", rotation=20)
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}', ha='center', va='bottom', fontsize=8)


def chart_endpoints(ax):
    bars = ax.bar(top_hosts["host"], top_hosts["log_count"], color="#9467bd")
    ax.set_xlabel("Host", fontsize=12)
    ax.set_ylabel("Log Count", fontsize=12)
    ax.tick_params(axis="x", rotation=20)
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}', ha='center', va='bottom', fontsize=8)


def chart_summary(ax):
    summary_text = f"""Total Logs Analyzed: {len(df):,}
    
Top Severity: {severity_counts.iloc[0]['severity'] if len(severity_counts) else 'N/A'}
Top Process: {top_processes.iloc[0]['process'] if len(top_processes) else 'N/A'}
Top Host: {top_hosts.iloc[0]['host'] if len(top_hosts) else 'N/A'}
    
Data Source: LogHub Linux Dataset
Analysis Platform: Databricks"""
    ax.text(0.5, 0.5, summary_text, ha='center', va='center', 
            fontsize=14, family='monospace',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    ax.axis('off')


top_severity = severity_counts.iloc[0]["severity"] if len(severity_counts) else "unknown"
top_severity_count = int(severity_counts.iloc[0]["log_count"]) if len(severity_counts) else 0
top_process = top_processes.iloc[0]["process"] if len(top_processes) else "unknown"
top_process_count = int(top_processes.iloc[0]["log_count"]) if len(top_processes) else 0
top_host = top_hosts.iloc[0]["host"] if len(top_hosts) else "unknown"
top_host_count = int(top_hosts.iloc[0]["log_count"]) if len(top_hosts) else 0
total_logs = len(df)

if len(logs_over_time) > 0:
    peak_hour = logs_over_time.loc[logs_over_time["log_count"].idxmax(), "hour"]
    peak_hour_str = peak_hour.strftime("%H:00")
    peak_count = int(logs_over_time["log_count"].max())
else:
    peak_hour_str = "unknown time"
    peak_count = 0

slides = [
    {
        "path": os.path.join(slides_dir, "slide_0.png"),
        "title": "Introduction",
        "subtitle": "Observability Platform Dashboard",
        "chart": chart_intro,
        "narration": (
            "Welcome to the Observability Platform Dashboard presentation. "
            f"In this analysis, we examine {total_logs:,} log entries from the LogHub Linux dataset. "
            "We'll explore log patterns, identify key processes, and understand system behavior. "
            "Let's dive into the data."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_1.png"),
        "title": "Logs Over Time",
        "subtitle": "Temporal distribution of log events across the observation window",
        "chart": chart_requests,
        "narration": (
            "This chart shows the temporal distribution of log volume. "
            f"We can see that traffic peaks around {peak_hour_str}, with {peak_count:,} log entries at that hour. "
            "The pattern reveals the system's activity cycles. "
            "Understanding these patterns helps us identify normal behavior and detect anomalies. "
            "Notice how the log volume fluctuates throughout the day, reflecting varying system workloads."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_2.png"),
        "title": "Severity Distribution",
        "subtitle": "Breakdown of log entries by severity level",
        "chart": chart_errors,
        "narration": (
            "Here we analyze log severity distribution. "
            f"The most common severity level is {top_severity}, with {top_severity_count:,} occurrences. "
            "This breakdown is crucial for understanding system health. "
            "A high proportion of error or warning messages could indicate underlying issues. "
            "In this dataset, we can assess whether the system is operating normally or experiencing problems."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_3.png"),
        "title": "Top Processes by Volume",
        "subtitle": "Most active processes generating log entries",
        "chart": chart_latency,
        "narration": (
            "This visualization highlights which processes are most active in the system. "
            f"The busiest process is {top_process}, generating {top_process_count:,} log entries. "
            "High log volume from a process could indicate heavy usage, or potentially, issues requiring attention. "
            "By monitoring process activity, we can identify resource-intensive operations. "
            "This information is valuable for capacity planning and performance optimization."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_4.png"),
        "title": "Top Hosts by Volume",
        "subtitle": "Machines producing the highest number of log entries",
        "chart": chart_endpoints,
        "narration": (
            "Now let's examine which hosts are generating the most logs. "
            f"The top host is {top_host}, with {top_host_count:,} log entries. "
            "Identifying noisy hosts helps us understand system load distribution. "
            "A host with unusually high log volume might be experiencing issues, or simply handling more traffic. "
            "This metric is essential for infrastructure monitoring and troubleshooting."
        ),
    },
    {
        "path": os.path.join(slides_dir, "slide_5.png"),
        "title": "Summary",
        "subtitle": "Key findings from the observability analysis",
        "chart": chart_summary,
        "narration": (
            "Let's summarize our key findings. "
            f"We analyzed {total_logs:,} log entries from the LogHub Linux dataset. "
            f"The dominant severity level was {top_severity}. "
            f"The most active process was {top_process}. "
            f"And the busiest host was {top_host}. "
            "This observability platform, built on Databricks, enables real-time monitoring and analysis. "
            "Thank you for watching this dashboard presentation."
        ),
    },
]

for slide in slides:
    save_slide(slide["title"], slide["subtitle"], slide["chart"], slide["path"])

print(f"Saved {len(slides)} slides to {slides_dir}")
if repo_output_path:
    repo_slides_dir = Path(repo_output_path) / "slides"
    repo_slides_dir.mkdir(parents=True, exist_ok=True)
    for slide in slides:
        dest = repo_slides_dir / Path(slide["path"]).name
        shutil.copy2(slide["path"], dest)
    print(f"Copied slides to repo: {repo_slides_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Generate narration audio

# COMMAND ----------

# DBTITLE 1,Cell 10
def speak_to_file(text, output_path):
    if gTTS is None:
        return None
    tts = gTTS(text=text, lang='en', slow=False)
    tts.save(output_path)
    return output_path


audio_files = []
for idx, slide in enumerate(slides, start=1):
    audio_path = os.path.join(audio_dir, f"slide_{idx}.mp3")
    audio_files.append(speak_to_file(slide["narration"], audio_path))

if gTTS is None:
    print("⚠️  gTTS not available. Narration audio will be skipped.")
else:
    print(f"Generated narration audio in {audio_dir}")
    if repo_output_path:
        repo_audio_dir = Path(repo_output_path) / "audio"
        repo_audio_dir.mkdir(parents=True, exist_ok=True)
        for audio_file in audio_files:
            if audio_file and os.path.exists(audio_file):
                shutil.copy2(audio_file, repo_audio_dir / Path(audio_file).name)
        print(f"Copied audio to repo: {repo_audio_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Build the video

# COMMAND ----------

# DBTITLE 1,Cell 13
if ImageClip is None:
    print("⚠️  moviepy not available. Video will be skipped.")
else:
    clips = []
    for idx, slide in enumerate(slides):
        # Safely get audio path if it exists
        audio_path = audio_files[idx] if idx < len(audio_files) else None
        if audio_path and os.path.exists(audio_path):
            audio_clip = AudioFileClip(audio_path)
            duration = audio_clip.duration + 0.5
            img_clip = ImageClip(slide["path"], duration=duration)
            img_clip = img_clip.with_audio(audio_clip)
        else:
            # Use longer duration for slides without audio (more narration text)
            img_clip = ImageClip(slide["path"], duration=8)
        clips.append(img_clip)

    final_video = concatenate_videoclips(clips, method="compose")
    
    # Write to local temp path first
    local_video_path = "/tmp/dashboard_explainer.mp4"
    final_video.write_videofile(local_video_path, fps=24, audio_codec="aac")
    
    # Copy to DBFS
    dbfs_video_path = "dbfs:/observability-data/dashboard_video/dashboard_explainer.mp4"
    dbutils.fs.cp(f"file:{local_video_path}", dbfs_video_path, recurse=False)
    
    # Verify
    final_path = "/dbfs/observability-data/dashboard_video/dashboard_explainer.mp4"
    if os.path.exists(final_path):
        file_size_mb = os.path.getsize(final_path) / (1024 * 1024)
        print(f"✓ Saved video: {final_path} ({file_size_mb:.2f} MB)")
        print(f"✓ Video duration: ~{len(slides) * 8} seconds ({len(slides)} slides)")
        if repo_output_path:
            repo_video = Path(repo_output_path) / "dashboard_explainer.mp4"
            repo_video.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(final_path, repo_video)
            print(f"Copied video to repo: {repo_video}")
    else:
        print(f"⚠️  Video generated but not found at: {final_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Copy all outputs into a repo folder (for GitHub)

# COMMAND ----------

if repo_output_path:
    try:
        dbutils.fs.cp(
            f"dbfs:{output_dir}",
            f"file:{repo_output_path}",
            True,
        )
        print(f"✓ Copied all outputs into repo folder: {repo_output_path}")
    except Exception as e:
        print(f"✗ Failed to copy outputs to repo: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Copy outputs to Workspace for download

# COMMAND ----------

workspace_user = "aespira@saintpetersuniversity1.onmicrosoft.com"
workspace_dir = f"/Workspace/Users/{workspace_user}"

video_path = "/dbfs/observability-data/dashboard_video/dashboard_explainer.mp4"
dashboard_path = "/dbfs/observability-data/dashboard_video/dashboard.png"

def copy_to_workspace(dbfs_path, filename):
    if not os.path.exists(dbfs_path):
        print(f"✗ Not found: {dbfs_path}")
        return
    file_size_mb = os.path.getsize(dbfs_path) / (1024 * 1024)
    # Remove /dbfs prefix to get the correct dbfs: path
    dbfs_source = dbfs_path.replace("/dbfs", "dbfs:", 1)
    dbutils.fs.cp(dbfs_source, f"file:{workspace_dir}/{filename}")
    print(f"✓ Copied {filename} ({file_size_mb:.2f} MB) to {workspace_dir}")


copy_to_workspace(video_path, "dashboard_explainer.mp4")
copy_to_workspace(dashboard_path, "dashboard.png")

print("\n📥 To download:")
print("  1. Go to Workspace in the left sidebar")
print(f"  2. Navigate to: Users → {workspace_user}")
print("  3. Right-click the file and select 'Download'")