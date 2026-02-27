"""Generic cache helpers for process-level file caching."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence, cast


def bbox_token(bbox: Sequence[float], precision: int = 4) -> str:
    """Create a stable cache token from bbox coordinates."""
    fmt = "{:." + str(precision) + "f}"
    return "_".join(fmt.format(coord).replace("-", "m").replace(".", "p") for coord in bbox)


def monthly_periods(start: str, end: str) -> list[str]:
    """Return inclusive YYYY-MM periods between start and end (YYYY-MM)."""
    start_dt = datetime.strptime(start, "%Y-%m")
    end_dt = datetime.strptime(end, "%Y-%m")
    periods: list[str] = []
    year, month = start_dt.year, start_dt.month
    while (year, month) <= (end_dt.year, end_dt.month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def read_manifest(path: Path) -> dict[str, Any] | None:
    """Read JSON manifest from disk; return None on parse/read errors."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return cast(dict[str, Any], data)
        return None
    except (json.JSONDecodeError, OSError):
        return None


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON manifest to disk."""
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
