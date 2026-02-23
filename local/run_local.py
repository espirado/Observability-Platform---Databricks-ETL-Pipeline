#!/usr/bin/env python3
"""
run_local.py — Run the Observability ETL pipeline locally with PySpark + Delta Lake.

Usage:
  # Run full pipeline (data gen → bronze → silver → gold)
  python local/run_local.py --pipeline full

  # Run a single notebook
  python local/run_local.py --notebook 00_ingest_from_loghub

  # Run a single notebook with parameter overrides
  python local/run_local.py --notebook 00_ingest_from_loghub --params dataset=Spark sample_size=5000

  # Convert all notebooks for local use (no execution)
  python local/run_local.py --convert-only

  # List available notebooks
  python local/run_local.py --list
"""

import argparse
import os
import sys
import time
import textwrap
from pathlib import Path
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
LOCAL_DIR = PROJECT_ROOT / "local"
LOCAL_NOTEBOOKS_DIR = PROJECT_ROOT / "local_notebooks"
DATA_ROOT = PROJECT_ROOT / "data"

# Ensure local module is importable
sys.path.insert(0, str(PROJECT_ROOT))


# ── Pipeline definitions ─────────────────────────────────────────────────
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
    "analytics": [
        "04_hive_sql_analysis",
        "06_anomaly_detection_mllib",
    ],
    "advanced": [
        "07_log_parsing_with_rdds",
        "08_streaming_log_analysis",
        "09_nosql_log_storage",
    ],
}


def list_notebooks():
    """List all available notebooks."""
    print("\n📓 Available Notebooks:")
    print("=" * 60)
    for nb in sorted(NOTEBOOKS_DIR.glob("*.py")):
        print(f"  {nb.stem}")
    print()
    print("🔗 Available Pipelines:")
    print("=" * 60)
    for name, steps in PIPELINES.items():
        print(f"  {name}:")
        for s in steps:
            print(f"    → {s}")
    print()


def convert_notebook(name: str) -> Path:
    """Convert a Databricks notebook to locally-runnable Python."""
    from local.convert_notebook import convert_file

    src = NOTEBOOKS_DIR / f"{name}.py"
    if not src.exists():
        print(f"❌ Notebook not found: {src}")
        sys.exit(1)

    dst = LOCAL_NOTEBOOKS_DIR / f"{name}.py"
    convert_file(str(src), str(dst), data_root=str(DATA_ROOT))
    return dst


def convert_all():
    """Convert all notebooks."""
    count = 0
    for nb in sorted(NOTEBOOKS_DIR.glob("*.py")):
        convert_notebook(nb.stem)
        count += 1
    print(f"\n✅ Converted {count} notebooks → {LOCAL_NOTEBOOKS_DIR}/")


def run_notebook(name: str, params: dict[str, str] | None = None):
    """Convert and run a single notebook locally."""
    print(f"\n{'='*60}")
    print(f"▶ Running: {name}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if params:
        print(f"  Params: {params}")
    print(f"{'='*60}\n")

    # Convert
    local_path = convert_notebook(name)

    # Ensure data directories exist
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    (DATA_ROOT / "observability-data" / "loghub").mkdir(parents=True, exist_ok=True)

    # Build execution globals
    # We exec the converted file — it has its own spark + dbutils init
    source = local_path.read_text()

    # If params provided, inject them after the dbutils install line
    if params:
        params_str = repr(params)
        source = source.replace(
            'dbutils = _install_dbutils(data_root=',
            f'dbutils = _install_dbutils(data_root=',
        )
        # Seed params after install
        seed_line = f"\ndbutils.widgets._seed({params_str})\n"
        # Insert after the dbutils line
        source = source.replace(
            'spark.sparkContext.setLogLevel("WARN")',
            f'spark.sparkContext.setLogLevel("WARN")\n{seed_line}',
        )

    start = time.time()
    try:
        exec(compile(source, str(local_path), "exec"), {"__file__": str(local_path), "__name__": "__main__"})
    except SystemExit as e:
        if e.code == 0:
            pass  # Normal notebook.exit()
        else:
            print(f"❌ Notebook exited with code: {e.code}")
            return False
    except Exception as e:
        print(f"\n❌ Error in {name}: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        elapsed = time.time() - start
        print(f"\n⏱  {name} completed in {elapsed:.1f}s")

    return True


def run_pipeline(pipeline_name: str, params: dict[str, str] | None = None):
    """Run a named pipeline (sequence of notebooks)."""
    if pipeline_name not in PIPELINES:
        print(f"❌ Unknown pipeline: {pipeline_name}")
        print(f"   Available: {', '.join(PIPELINES.keys())}")
        sys.exit(1)

    steps = PIPELINES[pipeline_name]
    print(f"\n🚀 Running pipeline: {pipeline_name}")
    print(f"   Steps: {' → '.join(steps)}\n")

    total_start = time.time()
    results = {}

    for i, step in enumerate(steps, 1):
        print(f"\n📌 Step {i}/{len(steps)}: {step}")
        ok = run_notebook(step, params)
        results[step] = "✅" if ok else "❌"
        if not ok:
            print(f"\n⚠️  Pipeline halted at step {i} ({step})")
            break

    total_elapsed = time.time() - total_start

    print(f"\n{'='*60}")
    print(f"📊 Pipeline Results: {pipeline_name}")
    print(f"{'='*60}")
    for step, status in results.items():
        print(f"  {status} {step}")
    print(f"\n⏱  Total time: {total_elapsed:.1f}s")
    print(f"{'='*60}\n")


def parse_params(param_strings: list[str] | None) -> dict[str, str]:
    """Parse key=value parameter strings."""
    if not param_strings:
        return {}
    params = {}
    for p in param_strings:
        if "=" not in p:
            print(f"⚠️  Invalid param (use key=value): {p}")
            continue
        k, v = p.split("=", 1)
        params[k.strip()] = v.strip()
    return params


def main():
    parser = argparse.ArgumentParser(
        description="Run Observability ETL pipeline locally",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Examples:
          python local/run_local.py --pipeline full
          python local/run_local.py --notebook 00_ingest_from_loghub --params dataset=Spark
          python local/run_local.py --list
          python local/run_local.py --convert-only
        """),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pipeline", choices=list(PIPELINES.keys()), help="Run a named pipeline")
    group.add_argument("--notebook", help="Run a single notebook (by stem name)")
    group.add_argument("--list", action="store_true", help="List available notebooks and pipelines")
    group.add_argument("--convert-only", action="store_true", help="Convert all notebooks without running")

    parser.add_argument("--params", nargs="*", help="Widget param overrides: key=value ...")
    parser.add_argument("--data-root", default=str(DATA_ROOT), help="Local data directory")

    args = parser.parse_args()

    if args.list:
        list_notebooks()
    elif args.convert_only:
        convert_all()
    elif args.notebook:
        params = parse_params(args.params)
        run_notebook(args.notebook, params or None)
    elif args.pipeline:
        params = parse_params(args.params)
        run_pipeline(args.pipeline, params or None)


if __name__ == "__main__":
    main()
