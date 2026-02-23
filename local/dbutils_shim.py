"""
dbutils shim — drop-in replacement for Databricks dbutils when running locally.

Supports:
  dbutils.widgets.text / dropdown / get
  dbutils.notebook.exit
  dbutils.fs.ls / mkdirs / cp
  dbutils.secrets.get  (reads from env vars: SECRET_<SCOPE>_<KEY>)
  dbutils.library.restartPython  (no-op locally)

Usage:
  - The local runner injects this as a builtin before exec'ing notebooks.
  - Widget defaults are used automatically; override with --params key=value.
"""

import os
import sys
import shutil
from pathlib import Path


class _Widgets:
    """Simulates dbutils.widgets — stores defaults, returns values."""

    def __init__(self):
        self._values: dict[str, str] = {}

    def text(self, name: str, default: str, label: str = ""):
        self._values.setdefault(name, default)

    def dropdown(self, name: str, default: str, choices: list, label: str = ""):
        self._values.setdefault(name, default)

    def get(self, name: str) -> str:
        if name in self._values:
            return self._values[name]
        raise ValueError(f"Widget '{name}' not defined. Pass --params {name}=VALUE")

    def set(self, name: str, value: str):
        self._values[name] = value

    # allow pre-seeding from CLI
    def _seed(self, overrides: dict[str, str]):
        self._values.update(overrides)


class _Notebook:
    """Simulates dbutils.notebook.exit — prints the message and raises SystemExit."""

    @staticmethod
    def exit(message: str = ""):
        print(f"\n✅ notebook.exit → {message}")
        raise SystemExit(0)


class _FsEntry:
    """Simulates FileInfo returned by dbutils.fs.ls."""

    def __init__(self, path: str, name: str, size: int, mod_time: int = 0):
        self.path = path
        self.name = name
        self.size = size
        self.modificationTime = mod_time

    def __repr__(self):
        return f"FileInfo(path='{self.path}', name='{self.name}', size={self.size})"


class _Fs:
    """Simulates dbutils.fs — maps /observability-data/... to LOCAL_DATA_ROOT."""

    def __init__(self, data_root: str):
        self._root = Path(data_root)

    def _resolve(self, dbfs_path: str) -> Path:
        """Convert a DBFS-style path to a local path."""
        cleaned = dbfs_path
        # strip leading prefixes
        for prefix in ["dbfs:", "file:", "/dbfs/"]:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
        # strip leading slash
        cleaned = cleaned.lstrip("/")
        return self._root / cleaned

    def ls(self, path: str) -> list:
        local = self._resolve(path)
        if not local.exists():
            raise FileNotFoundError(f"Path does not exist: {path} (local: {local})")
        entries = []
        for item in sorted(local.iterdir()):
            name = item.name + ("/" if item.is_dir() else "")
            entries.append(_FsEntry(
                path=str(item),
                name=name,
                size=item.stat().st_size if item.is_file() else 0,
            ))
        return entries

    def mkdirs(self, path: str):
        local = self._resolve(path)
        local.mkdir(parents=True, exist_ok=True)
        print(f"📁 fs.mkdirs → {local}")

    def cp(self, src: str, dst: str, recurse: bool = False):
        src_path = self._resolve(src)
        dst_path = self._resolve(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if recurse and src_path.is_dir():
            shutil.copytree(str(src_path), str(dst_path), dirs_exist_ok=True)
        else:
            shutil.copy2(str(src_path), str(dst_path))
        print(f"📋 fs.cp → {src_path} → {dst_path}")

    def rm(self, path: str, recurse: bool = False):
        local = self._resolve(path)
        if local.is_dir() and recurse:
            shutil.rmtree(local, ignore_errors=True)
        elif local.exists():
            local.unlink()
        print(f"🗑  fs.rm → {local}")


class _Secrets:
    """Simulates dbutils.secrets.get — reads env vars: SECRET_<SCOPE>_<KEY>."""

    @staticmethod
    def get(scope: str, key: str) -> str:
        env_key = f"SECRET_{scope.upper()}_{key.upper().replace('-', '_')}"
        val = os.environ.get(env_key, "")
        if not val:
            print(f"⚠️  secrets.get('{scope}', '{key}') → env var {env_key} not set, returning empty string")
        return val


class _Library:
    """Simulates dbutils.library — restartPython is a no-op locally."""

    @staticmethod
    def restartPython():
        print("ℹ️  library.restartPython() → no-op in local mode")


class DBUtilsShim:
    """Top-level dbutils replacement."""

    def __init__(self, data_root: str = "./data"):
        self.widgets = _Widgets()
        self.notebook = _Notebook()
        self.fs = _Fs(data_root)
        self.secrets = _Secrets()
        self.library = _Library()


def install(data_root: str = "./data", params: dict[str, str] | None = None):
    """
    Install the shim as a global `dbutils` available to exec'd notebooks.

    Call this BEFORE exec'ing any Databricks notebook source.
    Returns the shim instance.
    """
    shim = DBUtilsShim(data_root)
    if params:
        shim.widgets._seed(params)
    return shim
