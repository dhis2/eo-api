"""When a temporal dimension must list its timestamps instead of implying them (CLIM-950)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from open_climate_service.stac.services import (
    _implied_step_count,
    _temporal_values_needed,
)


def _ds(stamps: list[str]) -> xr.Dataset:
    times = np.array(stamps, dtype="datetime64[ns]")
    return xr.Dataset(
        {"v": (("t",), np.zeros(len(times), dtype="float32"))},
        coords={"t": times},
    )


def test_a_sparse_daily_axis_needs_its_timestamps_listed() -> None:
    """The event-scoped case: two daily acquisitions three months apart imply 92 positions."""
    ds = _ds(["2026-05-27", "2026-08-26"])

    assert _temporal_values_needed(ds, "t", "daily", "P1D") is True


def test_a_dense_daily_axis_stays_implied() -> None:
    """A complete run is described exactly by extent plus duration, so it must not grow."""
    stamps = [str(d.date()) for d in pd.date_range("2026-01-01", "2026-01-31", freq="D")]
    ds = _ds(stamps)

    assert _temporal_values_needed(ds, "t", "daily", "P1D") is False


def test_a_dense_monthly_axis_stays_implied_despite_unequal_month_lengths() -> None:
    """Calendar stepping, not 30-day arithmetic: otherwise every monthly store looks sparse."""
    stamps = [str(d.date()) for d in pd.date_range("2026-01-01", "2026-12-01", freq="MS")]
    ds = _ds(stamps)

    assert _temporal_values_needed(ds, "t", "monthly", "P1M") is False


def test_a_gap_in_a_monthly_axis_is_caught() -> None:
    ds = _ds(["2026-01-01", "2026-02-01", "2026-06-01"])

    assert _temporal_values_needed(ds, "t", "monthly", "P1M") is True


def test_an_irregular_cadence_still_needs_values_even_though_it_has_no_step() -> None:
    """Dekads carry no step at all, so the count comparison never runs for them."""
    ds = _ds(["2026-01-01", "2026-01-11", "2026-01-21"])

    assert _temporal_values_needed(ds, "t", "dekadal", None) is True


def test_a_single_slice_store_stays_implied() -> None:
    """One timestamp implies one position, so there is nothing to disagree about."""
    ds = _ds(["2026-05-27"])

    assert _temporal_values_needed(ds, "t", "daily", "P1D") is False


def test_a_step_that_cannot_be_walked_draws_no_conclusion() -> None:
    """A compound duration is not parsed, so the count must not be guessed from it."""
    ds = _ds(["2026-05-27", "2026-08-26"])

    assert _temporal_values_needed(ds, "t", "daily", "P1Y2M") is False


def test_a_dimension_the_store_does_not_have_is_left_alone() -> None:
    ds = _ds(["2026-05-27", "2026-08-26"])

    assert _temporal_values_needed(ds, "not_a_dim", "daily", "P1D") is False


@pytest.mark.parametrize(
    ("start", "end", "step", "expected"),
    [
        ("2026-05-27", "2026-08-26", "P1D", 92),
        ("2026-01-01", "2026-01-31", "P1D", 31),
        ("2026-01-01", "2026-12-01", "P1M", 12),
        ("2020-01-01", "2026-01-01", "P1Y", 7),
        ("2026-01-01", "2026-01-02", "PT1H", 25),
        ("2026-01-01", "2026-01-29", "P7D", 5),
    ],
)
def test_implied_step_count_matches_what_a_client_would_build(start: str, end: str, step: str, expected: int) -> None:
    assert _implied_step_count(pd.Timestamp(start), pd.Timestamp(end), step) == expected


def test_implied_step_count_refuses_a_duration_it_cannot_walk() -> None:
    assert _implied_step_count(pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01"), "P1Y2M") is None
