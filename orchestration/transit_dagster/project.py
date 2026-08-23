"""Locate the dbt project in both layouts: repo checkout
(<repo>/dbt/transit next to orchestration/) and the image (/opt/dagster/app/dbt/transit)."""

from __future__ import annotations

import os
from pathlib import Path


def dbt_project_dir() -> Path:
    override = os.environ.get("DBT_PROJECT_DIR")
    if override:
        return Path(override)
    here = Path(__file__).resolve()
    for root in (here.parents[1], here.parents[2]):
        candidate = root / "dbt" / "transit"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("dbt/transit not found; set DBT_PROJECT_DIR")


def dbt_manifest_path() -> Path:
    manifest = dbt_project_dir() / "target" / "manifest.json"
    if not manifest.exists():
        raise FileNotFoundError(
            f"{manifest} missing — run `dbt parse` there (the image bakes it at build)"
        )
    return manifest
