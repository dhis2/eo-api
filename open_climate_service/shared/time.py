"""Time helpers shared across Open Climate Service modules."""

import logging
import re
from datetime import UTC, date, datetime
from typing import Any, cast

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_ISO_DURATION_RE = re.compile(r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def resolve_iso_period_step(dataset: dict[str, Any]) -> str | None:
    """Return the ISO 8601 duration step from ``extents.temporal.resolution``.

    Returns None if the field is absent or not a valid ISO 8601 duration, logging
    a warning in the latter case.
    """
    extents = dataset.get("extents")
    if not isinstance(extents, dict):
        return None
    temporal = extents.get("temporal")
    if not isinstance(temporal, dict):
        return None
    resolution = temporal.get("resolution")
    if not resolution:
        return None
    resolution_str = str(resolution)
    try:
        _iso_step_to_approx_hours(resolution_str)
    except ValueError:
        logger.warning("Invalid ISO 8601 duration in extents.temporal.resolution: %r", resolution_str)
        return None
    return resolution_str


def _iso_step_to_approx_hours(step: str) -> float:
    """Return the approximate duration in hours for an ISO 8601 duration string.

    Months and years use calendar averages (30.4375 days/month, 365.25 days/year).
    Raises ValueError for unrecognised formats.
    """
    m = _ISO_DURATION_RE.fullmatch(step)
    if not m:
        raise ValueError(f"Cannot parse ISO 8601 duration: '{step}'")
    years, months, weeks, days, hours, minutes, seconds = (int(g or 0) for g in m.groups())
    result = (
        years * 365.25 * 24 + months * 30.4375 * 24 + weeks * 7 * 24 + days * 24 + hours + minutes / 60 + seconds / 3600
    )
    if result <= 0:
        raise ValueError(f"ISO 8601 duration '{step}' resolves to zero — cannot derive chunk size")
    return result


def time_chunk_for_iso_step(step: str) -> int:
    """Return a suitable zarr time chunk size for a given ISO 8601 duration step.

    Targets roughly one week of data for sub-daily steps, one month for daily/sub-weekly
    steps, and one year for weekly and coarser steps.  This keeps individual chunk files
    at a manageable size while covering a natural analysis window in one read.
    """
    hours = _iso_step_to_approx_hours(step)
    if hours < 24:
        return max(1, round(24 * 7 / hours))  # ~1 week
    if hours < 24 * 7:
        return max(1, round(24 * 30 / hours))  # ~1 month
    return max(1, round(24 * 365.25 / hours))  # ~1 year


_WEEKLY_PERIOD_PATTERN = re.compile(r"^(?P<year>\d{4})-W(?P<week>\d{2})$")


def _normalize_datetime_for_period(value: datetime) -> datetime:
    """Convert aware datetimes to UTC before deriving dataset-native periods."""
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value


def _coerce_numpy_datetime(value: object) -> datetime:
    """Convert a numpy or Python datetime-like scalar to a datetime."""
    if isinstance(value, datetime):
        return value
    np_value = np.datetime64(cast(Any, value))
    return datetime.fromisoformat(np.datetime_as_string(np_value, unit="s"))


def datetime_to_period_string(value: datetime, period_type: str) -> str:
    """Convert a datetime to the dataset-native period string format."""
    value = _normalize_datetime_for_period(value)
    if period_type == "hourly":
        return value.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")
    if period_type == "daily":
        return value.date().isoformat()
    if period_type == "weekly":
        iso_year, iso_week, _ = value.isocalendar()
        return f"{iso_year:04d}-W{iso_week:02d}"
    if period_type == "monthly":
        return f"{value.year:04d}-{value.month:02d}"
    if period_type == "yearly":
        return str(value.year)
    raise ValueError(f"Unsupported period_type '{period_type}'")


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(UTC)


def utc_today() -> date:
    """Return the current UTC calendar date."""
    return utc_now().date()


def parse_hourly_period_string(value: str) -> datetime:
    """Parse a dataset-native hourly period string or full ISO datetime."""
    if len(value) == 13:
        return datetime.strptime(value, "%Y-%m-%dT%H")
    return datetime.fromisoformat(value)


def parse_weekly_period_string(value: str) -> datetime:
    """Parse a dataset-native weekly period string or full ISO datetime."""
    match = _WEEKLY_PERIOD_PATTERN.fullmatch(value)
    if match is not None:
        iso_year = int(match.group("year"))
        iso_week = int(match.group("week"))
        return datetime.combine(date.fromisocalendar(iso_year, iso_week, 1), datetime.min.time())
    return datetime.fromisoformat(value)


def normalize_period_string(value: str, period_type: str) -> str:
    """Normalize an input period string to the dataset-native period format."""
    if period_type == "hourly":
        try:
            return datetime_to_period_string(parse_hourly_period_string(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid hourly period '{value}'; expected YYYY-MM-DDTHH or ISO datetime") from exc
    if period_type == "daily":
        try:
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid daily period '{value}'; expected YYYY-MM-DD or ISO datetime") from exc
    if period_type == "weekly":
        try:
            return datetime_to_period_string(parse_weekly_period_string(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid weekly period '{value}'; expected YYYY-Www or ISO datetime") from exc
    if period_type == "monthly":
        try:
            if len(value) == 7:
                datetime.fromisoformat(f"{value}-01")
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid monthly period '{value}'; expected YYYY-MM or ISO datetime") from exc
    if period_type == "yearly":
        try:
            if len(value) == 4:
                int(value)
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid yearly period '{value}'; expected YYYY or ISO datetime") from exc
    raise ValueError(f"Unsupported period_type '{period_type}'")


def parse_period_string_to_datetime(value: str) -> datetime:
    """Parse a dataset-native period string to a UTC datetime."""
    normalized = value.strip()
    if _WEEKLY_PERIOD_PATTERN.fullmatch(normalized) is not None:
        return parse_weekly_period_string(normalized).replace(tzinfo=UTC)
    if "T" not in normalized:
        if len(normalized) == 4:
            normalized = f"{normalized}-01-01T00:00:00"
        elif len(normalized) == 7:
            normalized = f"{normalized}-01T00:00:00"
        else:
            normalized = f"{normalized}T00:00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def numpy_datetime_to_period_string(datetimes: np.ndarray[Any, Any], period_type: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    if period_type != "weekly":
        lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
        return np.datetime_as_string(datetimes, unit="s").astype(f"U{lengths[period_type]}")

    dt_index = pd.DatetimeIndex(np.atleast_1d(np.asarray(datetimes, dtype="datetime64[ns]")))
    iso = dt_index.isocalendar()
    strings = iso["year"].astype(str).str.zfill(4) + "-W" + iso["week"].astype(str).str.zfill(2)
    return cast(np.ndarray[Any, Any], strings.to_numpy().astype("U8"))
