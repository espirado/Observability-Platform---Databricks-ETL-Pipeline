#!/usr/bin/env python3
"""
run_databricks.py — Run notebooks on a remote Databricks workspace via CLI.

Prerequisites:
  1. pip install databricks-cli
  2. databricks configure --token
     (enter your workspace URL + personal access token)

Usage:
  # Upload notebooks and run the full ETL pipeline
  python local/run_databricks.py --upload --run full

  # Upload notebooks only
  python local/run_databricks.py --upload

  # Run a single notebook on Databricks
  python local/run_databricks.py --run-notebook 00_ingest_from_loghub --params dataset=HDFS

  # Check workspace connection
  python local/run_databricks.py --check

  # List remote workspace contents
  python local/run_databricks.py --ls /Workspace
"""

import argparse
import json
import subprocess
import sys
import time
import textwrap
from pathlib import Path
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
REMOTE_PATH = "/Workspace/observability-etl"

# Pipeline definitions (same as local runner)
PIPELINES = {
    "full": [
        "00_ingest_from_loghub",
        "01_ingest_raw_logs",
        "02_enrich_events",
        "03_build_flow_dataset",
        "06_anomaly_detection_mllib",
    ],
    "ingest": [
        "00_ingest_from_loghub",
        "01_ingest_raw_logs",
    ],
    "etl": [
        "01_ingest_raw_logs",
        "02_enrich_events",
        "03_build_flow_dataset",
    ],
}


def run_cmd(cmd: list[str], capture: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command, print it, return result."""
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=capture, text=True)
    if result.returncode != 0:
        print(f"  ❌ Command failed (exit {result.returncode})")
        if result.stderr:
            print(f"  stderr: {result.stderr.strip()}")
    return result


def check_cli():
    """Verify Databricks CLI is installed and configured."""
    print("\n🔍 Checking Databricks CLI...")

    # Check installation
    r = run_cmd(["databricks", "--version"])
    if r.returncode != 0:
        print("❌ Databricks CLI not found. Install with: pip install databricks-cli")
        return False
    print(f"  ✅ {r.stdout.strip()}")

    # Check configuration
    r = run_cmd(["databricks", "workspace", "ls", "/"])
    if r.returncode != 0:
        print("❌ CLI not configured. Run: databricks configure --token")
        print("   You need your workspace URL (e.g., https://adb-xxxx.azuredatabricks.net)")
        print("   and a personal access token (Settings → Developer → Access Tokens)")
        return False
    print("  ✅ Connected to workspace")

    # Show what's there
    print(f"\n  Workspace root contents:")
    for line in r.stdout.strip().split("\n")[:10]:
        print(f"    {line}")

    return True


def upload_notebooks():
    """Upload all notebooks to the Databricks workspace."""
    print(f"\n📤 Uploading notebooks to {REMOTE_PATH}...")

    r = run_cmd([
        "databricks", "workspace", "import_dir",
        str(NOTEBOOKS_DIR), REMOTE_PATH,
        "--overwrite",
    ])

    if r.returncode == 0:
        print(f"  ✅ Notebooks uploaded to {REMOTE_PATH}")
        # List what was uploaded
        r2 = run_cmd(["databricks", "workspace", "ls", REMOTE_PATH])
        if r2.returncode == 0:
            print(f"\n  Remote notebooks:")
            for line in r2.stdout.strip().split("\n"):
                print(f"    📓 {line}")
    else:
        print("  ❌ Upload failed")
        return False
    return True


def run_remote_notebook(name: str, params: dict[str, str] | None = None, cluster_id: str | None = None):
    """Run a notebook on a Databricks cluster."""
    notebook_path = f"{REMOTE_PATH}/{name}"
    print(f"\n▶ Running remote notebook: {notebook_path}")

    # Build the runs/submit payload
    payload = {
        "run_name": f"local-run-{name}-{datetime.now().strftime('%H%M%S')}",
        "tasks": [{
            "task_key": name.replace(" ", "_"),
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": params or {},
            },
        }],
    }

    # If cluster_id provided, use existing cluster; otherwise use new cluster
    if cluster_id:
        payload["tasks"][0]["existing_cluster_id"] = cluster_id
    else:
        payload["tasks"][0]["new_cluster"] = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
            "spark_conf": {
                "spark.databricks.delta.optimizeWrite.enabled": "true",
            },
        }

    payload_json = json.dumps(payload)

    # Submit the run
    r = run_cmd(["databricks", "jobs", "submit", "--json", payload_json])
    if r.returncode != 0:
        print(f"  ❌ Failed to submit {name}")
        return None

    try:
        result = json.loads(r.stdout)
        run_id = result.get("run_id")
        print(f"  🚀 Submitted! Run ID: {run_id}")
        return run_id
    except json.JSONDecodeError:
        print(f"  ⚠️  Could not parse response: {r.stdout}")
        return None


def wait_for_run(run_id: int, timeout: int = 3600):
    """Poll a run until it completes."""
    print(f"\n⏳ Waiting for run {run_id}...")
    start = time.time()

    while time.time() - start < timeout:
        r = run_cmd(["databricks", "runs", "get", "--run-id", str(run_id)])
        if r.returncode != 0:
            time.sleep(10)
            continue

        try:
            data = json.loads(r.stdout)
            state = data.get("state", {})
            life_cycle = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state", "")

            print(f"  State: {life_cycle} {result_state}")

            if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if result_state == "SUCCESS":
                    print(f"  ✅ Run {run_id} succeeded!")
                    return True
                else:
                    msg = state.get("state_message", "")
                    print(f"  ❌ Run {run_id} failed: {result_state} — {msg}")
                    return False
        except json.JSONDecodeError:
            pass

        time.sleep(15)

    print(f"  ⏰ Timeout after {timeout}s")
    return False


def run_pipeline_remote(pipeline_name: str, params: dict[str, str] | None = None, cluster_id: str | None = None):
    """Run a pipeline on Databricks (sequential notebooks)."""
    if pipeline_name not in PIPELINES:
        print(f"❌ Unknown pipeline: {pipeline_name}")
        sys.exit(1)

    steps = PIPELINES[pipeline_name]
    print(f"\n🚀 Running pipeline: {pipeline_name} on Databricks")
    print(f"   Steps: {' → '.join(steps)}\n")

    for i, step in enumerate(steps, 1):
        print(f"\n📌 Step {i}/{len(steps)}: {step}")
        run_id = run_remote_notebook(step, params, cluster_id)
        if run_id is None:
            print(f"  ⚠️  Pipeline halted — could not submit {step}")
            break
        ok = wait_for_run(run_id)
        if not ok:
            print(f"  ⚠️  Pipeline halted at step {i}")
            break
    else:
        print(f"\n✅ Pipeline '{pipeline_name}' completed successfully!")


def ls_workspace(path: str):
    """List workspace contents."""
    r = run_cmd(["databricks", "workspace", "ls", "-l", path])
    if r.returncode == 0:
        print(r.stdout)


def parse_params(param_strings: list[str] | None) -> dict[str, str]:
    if not param_strings:
        return {}
    params = {}
    for p in param_strings:
        if "=" in p:
            k, v = p.split("=", 1)
            params[k.strip()] = v.strip()
    return params


def main():
    parser = argparse.ArgumentParser(
        description="Run notebooks on a remote Databricks workspace",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Setup:
          1. pip install databricks-cli
          2. databricks configure --token
          3. python local/run_databricks.py --check
          4. python local/run_databricks.py --upload --run full
        """),
    )

    parser.add_argument("--check", action="store_true", help="Check CLI connection")
    parser.add_argument("--upload", action="store_true", help="Upload notebooks to workspace")
    parser.add_argument("--run", choices=list(PIPELINES.keys()), help="Run a pipeline remotely")
    parser.add_argument("--run-notebook", help="Run a single notebook remotely")
    parser.add_argument("--ls", help="List workspace path contents")
    parser.add_argument("--cluster-id", help="Use an existing cluster (skip new cluster creation)")
    parser.add_argument("--params", nargs="*", help="Notebook params: key=value ...")

    args = parser.parse_args()

    if not any([args.check, args.upload, args.run, args.run_notebook, args.ls]):
        parser.print_help()
        return

    if args.check:
        check_cli()

    if args.upload:
        upload_notebooks()

    params = parse_params(args.params)

    if args.run:
        run_pipeline_remote(args.run, params or None, args.cluster_id)

    if args.run_notebook:
        run_id = run_remote_notebook(args.run_notebook, params or None, args.cluster_id)
        if run_id:
            wait_for_run(run_id)

    if args.ls:
        ls_workspace(args.ls)


if __name__ == "__main__":
    main()
