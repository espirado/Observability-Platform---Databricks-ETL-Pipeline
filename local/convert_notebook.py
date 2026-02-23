"""
convert_notebook.py — Convert Databricks notebook .py files into locally-runnable Python.

Handles:
  - Strips `# Databricks notebook source` header
  - Strips `# MAGIC %md`, `# MAGIC %sql`, `# MAGIC %pip` cells
  - Strips `# COMMAND ----------` separators
  - Strips display() calls (wraps them in print-based fallback)
  - Replaces /dbfs/ paths with local data root
  - Injects dbutils shim + SparkSession builder at the top
"""

import re
import textwrap
from pathlib import Path


def convert(notebook_path: str, data_root: str = "./data") -> str:
    """Convert a Databricks .py notebook to locally-runnable Python source."""

    source = Path(notebook_path).read_text()
    lines = source.splitlines()

    # ── Pass 1: strip Databricks markers and magic commands ──
    cleaned = []
    skip_magic_block = False

    for line in lines:
        stripped = line.strip()

        # Skip the notebook header
        if stripped == "# Databricks notebook source":
            continue

        # Skip command separators
        if stripped == "# COMMAND ----------":
            skip_magic_block = False
            cleaned.append("")  # blank line for readability
            continue

        # Skip MAGIC lines (markdown, sql, pip, etc.)
        if stripped.startswith("# MAGIC"):
            continue

        # Skip DBTITLE lines
        if stripped.startswith("# DBTITLE"):
            continue

        cleaned.append(line)

    converted = "\n".join(cleaned)

    # ── Pass 2: path rewrites ──
    # /dbfs/observability-data/... → {data_root}/observability-data/...
    converted = converted.replace('"/dbfs/', f'"{data_root}/')
    converted = converted.replace("'/dbfs/", f"'{data_root}/")

    # Spark reads use paths like "/observability-data/..." without /dbfs
    # These are DBFS root paths — rewrite them for local Delta reads
    # Only in .load() / .save() / path assignment contexts
    converted = re.sub(
        r'(["\'])/observability-data/',
        rf'\1{data_root}/observability-data/',
        converted,
    )

    # ── Pass 3: inject local preamble ──
    preamble = textwrap.dedent(f'''\
        # ═══════════════════════════════════════════════════════════════
        # AUTO-GENERATED — converted from Databricks notebook for local execution
        # Data root: {data_root}
        # ═══════════════════════════════════════════════════════════════
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from local.dbutils_shim import install as _install_dbutils

        # Initialize local dbutils shim
        dbutils = _install_dbutils(data_root="{data_root}", params={{}})

        # Initialize local SparkSession with Delta Lake support
        from pyspark.sql import SparkSession
        from delta.pip_utils import configure_spark_with_delta_pip

        _builder = (
            SparkSession.builder
            .master("local[*]")
            .appName("observability-local")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "{data_root}/spark-warehouse")
            .config("spark.driver.memory", "4g")
            .config("spark.ui.showConsoleProgress", "false")
        )
        spark = configure_spark_with_delta_pip(_builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # display() fallback for local mode
        def display(df, *args, **kwargs):
            if hasattr(df, "show"):
                df.show(20, truncate=False)
            else:
                print(df)

    ''')

    return preamble + converted


def convert_file(notebook_path: str, output_path: str, data_root: str = "./data"):
    """Convert a notebook file and write to output path."""
    converted = convert(notebook_path, data_root)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(converted)
    print(f"✅ Converted: {notebook_path} → {output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convert Databricks notebooks for local execution")
    parser.add_argument("notebook", help="Path to .py Databricks notebook")
    parser.add_argument("-o", "--output", help="Output path (default: local_notebooks/<name>)")
    parser.add_argument("--data-root", default="./data", help="Local data directory")
    args = parser.parse_args()

    nb = Path(args.notebook)
    out = args.output or f"local_notebooks/{nb.name}"
    convert_file(str(nb), out, args.data_root)
