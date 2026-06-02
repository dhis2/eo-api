import pytest

from open_climate_service.shared.time import (
    datetime_to_period_string,
    normalize_period_string,
    parse_period_string_to_datetime,
)


def test_normalize_period_string_raises_targeted_monthly_error() -> None:
    with pytest.raises(ValueError, match="Invalid monthly period '2024-13'; expected YYYY-MM or ISO datetime"):
        normalize_period_string("2024-13", "monthly")


def test_normalize_period_string_accepts_dataset_native_hourly_period() -> None:
    assert normalize_period_string("2026-04-21T13", "hourly") == "2026-04-21T13"


def test_normalize_period_string_converts_aware_hourly_datetime_to_utc_period() -> None:
    assert normalize_period_string("2026-04-21T13:30:00+02:00", "hourly") == "2026-04-21T11"


def test_normalize_period_string_converts_aware_daily_datetime_to_utc_period() -> None:
    assert normalize_period_string("2026-04-21T00:30:00+02:00", "daily") == "2026-04-20"


def test_normalize_period_string_accepts_dataset_native_weekly_period() -> None:
    assert normalize_period_string("2026-W17", "weekly") == "2026-W17"


def test_normalize_period_string_converts_datetime_to_weekly_period() -> None:
    assert normalize_period_string("2026-04-21T13:30:00+00:00", "weekly") == "2026-W17"


def test_datetime_to_period_string_converts_aware_monthly_datetime_to_utc_period() -> None:
    from datetime import datetime

    value = datetime.fromisoformat("2026-05-01T00:30:00+02:00")

    assert datetime_to_period_string(value, "monthly") == "2026-04"


def test_normalize_period_string_rejects_invalid_weekly_period() -> None:
    with pytest.raises(ValueError, match="Invalid weekly period '2026-W54'; expected YYYY-Www or ISO datetime"):
        normalize_period_string("2026-W54", "weekly")


def test_parse_period_string_to_datetime_accepts_dataset_native_hourly_period() -> None:
    parsed = parse_period_string_to_datetime("2026-04-21T13")

    assert parsed.isoformat() == "2026-04-21T13:00:00+00:00"


def test_parse_period_string_to_datetime_accepts_dataset_native_weekly_period() -> None:
    parsed = parse_period_string_to_datetime("2026-W17")

    assert parsed.isoformat() == "2026-04-20T00:00:00+00:00"
