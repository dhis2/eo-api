from datetime import UTC, date, datetime

import pytest

from open_climate_service.providers import availability


def test_chirps3_daily_latest_available_uses_previous_complete_month_after_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 21)

    monkeypatch.setattr(availability, "utc_today", lambda: FixedDate(2026, 4, 21))

    result = availability.chirps3_daily_latest_available(
        dataset={"sync": {"availability": {"complete_month_after_day": 20}}},
        requested_end="2026-04-21",
    )

    assert result == "2026-03-31"


def test_chirps3_daily_latest_available_uses_month_before_previous_on_threshold_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 20)

    monkeypatch.setattr(availability, "utc_today", lambda: FixedDate(2026, 4, 20))

    result = availability.chirps3_daily_latest_available(
        dataset={"sync": {"availability": {"complete_month_after_day": 20}}},
        requested_end="2026-04-20",
    )

    assert result == "2026-02-28"


def test_lagged_latest_available_formats_hourly_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> "FixedDateTime":  # noqa: ANN401
            return cls(2026, 4, 21, 12, 34, tzinfo=UTC)

    monkeypatch.setattr(availability, "utc_now", lambda: FixedDateTime(2026, 4, 21, 12, 34, tzinfo=UTC))

    result = availability.lagged_latest_available(
        dataset={
            "period_type": "hourly",
            "sync": {"availability": {"lag_hours": 5}},
        },
        requested_end="2026-04-21T12:00:00",
    )

    assert result == "2026-04-21T07"


def test_lagged_latest_available_formats_daily_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 21)

    monkeypatch.setattr(availability, "utc_today", lambda: FixedDate(2026, 4, 21))

    result = availability.lagged_latest_available(
        dataset={
            "period_type": "daily",
            "sync": {"availability": {"lag_days": 2}},
        },
        requested_end="2026-04-21",
    )

    assert result == "2026-04-19"


def test_worldpop_release_latest_available_allows_configured_future_projection() -> None:
    result = availability.worldpop_release_latest_available(
        dataset={"period_type": "yearly", "sync": {"availability": {"allow_future": True}}},
        requested_end="2030",
    )

    assert result == "2030"


def test_lagged_latest_available_formats_yearly_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 21)

    monkeypatch.setattr(availability, "utc_today", lambda: FixedDate(2026, 4, 21))

    result = availability.lagged_latest_available(
        dataset={
            "period_type": "yearly",
            "sync": {"availability": {"latest_year_offset": 1}},
        },
        requested_end="2028",
    )

    assert result == "2025"
