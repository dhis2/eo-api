"""Provider availability policies used by sync planning.

These functions keep source-specific release cadence rules out of the generic
sync engine. They are intentionally small and metadata-driven so dataset YAML can
choose the right policy per upstream provider.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from typing import Any

from open_climate_service.shared.time import datetime_to_period_string, utc_now, utc_today


def chirps3_daily_latest_available(*, dataset: dict[str, Any], requested_end: str) -> str:
    """Return latest complete CHIRPS3 daily period available for safe sync.

    The dhis2eo CHIRPS3 downloader groups daily files by source month. For
    final/rnl data, use only fully released months by default: after the 20th,
    the previous month is considered available; otherwise the month before that
    is the latest safe complete month.
    """
    availability = _availability_metadata(dataset)
    threshold_day = availability.get("complete_month_after_day", 20)
    if not isinstance(threshold_day, int):
        threshold_day = 20

    today = utc_today()
    months_back = 1 if today.day > threshold_day else 2
    available_month = _add_months(today.replace(day=1), -months_back)
    latest_day = monthrange(available_month.year, available_month.month)[1]
    return date(available_month.year, available_month.month, latest_day).isoformat()


def lagged_latest_available(*, dataset: dict[str, Any], requested_end: str) -> str:
    """Return latest available period by applying YAML-declared lag metadata."""
    availability = _availability_metadata(dataset)
    period_type = str(dataset.get("period_type", "daily"))

    if period_type == "hourly":
        lag_hours = availability.get("lag_hours")
        if isinstance(lag_hours, int) and lag_hours > 0:
            latest = utc_now() - timedelta(hours=lag_hours)
            return datetime_to_period_string(latest, period_type)
        return requested_end

    lag_days = availability.get("lag_days")
    if period_type in {"daily", "monthly"} and isinstance(lag_days, int) and lag_days > 0:
        latest_date = utc_today() - timedelta(days=lag_days)
        if period_type == "monthly":
            return f"{latest_date.year:04d}-{latest_date.month:02d}"
        return latest_date.isoformat()

    if period_type == "yearly":
        latest_year_offset = availability.get("latest_year_offset")
        if isinstance(latest_year_offset, int) and latest_year_offset >= 0:
            return str(utc_today().year - latest_year_offset)

    return requested_end


def worldpop_release_latest_available(*, dataset: dict[str, Any], requested_end: str) -> str:
    """Return WorldPop release availability, including configured projections."""
    availability = _availability_metadata(dataset)
    if availability.get("allow_future") is True:
        return requested_end

    latest_year = availability.get("latest_year")
    if isinstance(latest_year, int):
        return str(latest_year)

    return lagged_latest_available(dataset=dataset, requested_end=requested_end)


def _availability_metadata(dataset: dict[str, Any]) -> dict[str, Any]:
    """Return sync availability metadata from a dataset template."""
    availability = dataset.get("sync", {}).get("availability")
    return availability if isinstance(availability, dict) else {}


def _add_months(value: date, offset: int) -> date:
    """Add a month offset to the first day of a month."""
    month_index = value.year * 12 + value.month - 1 + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)
