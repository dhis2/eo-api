"""Day-weighted aggregation of a dekadal cube to months or ISO weeks.

The point of the process is that dekads are unequal, so these tests pin the arithmetic
against hand-computed weights rather than against the implementation, and assert the
properties that make the result defensible: an exact February mean, a conserved annual
total, and no target period silently becoming NaN.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pytest
import xarray as xr

from open_climate_service.plugins.processes.aggregate_dekads import aggregate_dekads
from open_climate_service.shared.time import dekad_period_ids


@pytest.fixture
def ocs_logs(caplog: pytest.LogCaptureFixture) -> Any:
    """Capture warnings from the OCS package logger.

    ``startup.py`` sets ``propagate = False`` on it, so caplog's root handler never sees
    the records. Same shim as tests/test_raster_contract.py.
    """
    package_logger = logging.getLogger("open_climate_service")
    previous = package_logger.propagate
    package_logger.propagate = True
    try:
        with caplog.at_level(logging.WARNING, logger="open_climate_service"):
            yield caplog
    finally:
        package_logger.propagate = previous


def _cube(ids: list[str], values: np.ndarray | None = None, units: str = "gC/m2/day") -> xr.DataArray:
    """A (t, y, x) cube on the given dekad ids, one distinct value per timestep."""
    vals = np.arange(1, len(ids) + 1, dtype="f8") if values is None else values
    return xr.DataArray(
        vals[:, None, None] * np.ones((1, 2, 2)),
        dims=("t", "y", "x"),
        coords={"t": np.array(ids, dtype="datetime64[ns]"), "y": [1.0, 0.0], "x": [33.0, 34.0]},
        attrs={"units": units},
    )


def test_february_matches_a_hand_computed_day_weighted_mean() -> None:
    """The month the bias is worst: an 8-day third dekad against two 10-day ones."""
    cube = _cube(dekad_period_ids("2026-01-01", "2026-12-31"))
    monthly = aggregate_dekads(cube, period="month", method="mean")

    # February 2026 is dekads 4, 5, 6 -> values 4, 5, 6 with lengths 10, 10, 8.
    weights, values = [10, 10, 8], [4, 5, 6]
    expected = sum(w * v for w, v in zip(weights, values, strict=True)) / sum(weights)

    assert float(monthly.isel(t=1, y=0, x=0)) == pytest.approx(expected, abs=1e-12)
    # The bias being corrected: an equal-weight mean would read 5.0.
    assert expected != pytest.approx(sum(values) / 3, abs=1e-6)


def test_january_matches_a_hand_computed_day_weighted_mean() -> None:
    """A month with an 11-day third dekad, biased the other way."""
    cube = _cube(dekad_period_ids("2026-01-01", "2026-12-31"))
    monthly = aggregate_dekads(cube, period="month", method="mean")

    weights, values = [10, 10, 11], [1, 2, 3]
    expected = sum(w * v for w, v in zip(weights, values, strict=True)) / sum(weights)
    assert float(monthly.isel(t=0, y=0, x=0)) == pytest.approx(expected, abs=1e-12)


def test_a_full_year_gives_twelve_months_with_no_gaps() -> None:
    monthly = aggregate_dekads(_cube(dekad_period_ids("2026-01-01", "2026-12-31")), period="month")
    assert monthly.sizes["t"] == 12
    assert not bool(monthly.isnull().any())
    assert [str(v)[:10] for v in monthly["t"].values[:3]] == ["2026-01-01", "2026-02-01", "2026-03-01"]


def test_sum_conserves_the_annual_total() -> None:
    """A per-dekad total reallocated by day overlap must not create or lose anything."""
    ids = dekad_period_ids("2026-01-01", "2026-12-31")
    cube = _cube(ids, units="gC/m2")
    source_total = float(cube.isel(y=0, x=0).sum("t"))

    monthly = aggregate_dekads(cube, period="month", method="sum")

    assert float(monthly.isel(y=0, x=0).sum("t")) == pytest.approx(source_total, rel=1e-12)


def test_weekly_covers_every_iso_week_without_gaps() -> None:
    """Weeks never align with dekads, so this is the case that would strand NaNs."""
    weekly = aggregate_dekads(_cube(dekad_period_ids("2026-01-01", "2026-12-31")), period="week")
    assert weekly.sizes["t"] in (52, 53)
    assert not bool(weekly.isnull().any())


def test_a_missing_dekad_renormalises_per_pixel_rather_than_poisoning_the_month() -> None:
    ids = dekad_period_ids("2026-02-01", "2026-02-28")
    cube = _cube(ids)  # values 1, 2, 3 with lengths 10, 10, 8
    # Blank the middle dekad over half the grid only.
    holed = cube.where(~((cube["t"] == cube["t"][1]) & (cube["y"] == 1.0)))

    monthly = aggregate_dekads(holed, period="month", method="mean")

    # Where the dekad survives, the full three-way weighting applies.
    intact = (10 * 1 + 10 * 2 + 8 * 3) / 28
    assert float(monthly.isel(t=0, y=1, x=0)) == pytest.approx(intact, abs=1e-12)
    # Where it is missing, its weight is dropped and the rest renormalised — not NaN,
    # and not silently treated as zero.
    renormalised = (10 * 1 + 8 * 3) / 18
    assert float(monthly.isel(t=0, y=0, x=0)) == pytest.approx(renormalised, abs=1e-12)


def test_an_all_missing_period_stays_nan_rather_than_becoming_zero() -> None:
    ids = dekad_period_ids("2026-02-01", "2026-02-28")
    cube = _cube(ids).where(False)  # every value missing
    monthly = aggregate_dekads(cube, period="month", method="mean")
    assert bool(monthly.isnull().all())


def test_a_partially_covered_month_is_computed_from_the_dekads_that_exist() -> None:
    # Only the last two dekads of January.
    cube = _cube(["2026-01-11", "2026-01-21"])
    monthly = aggregate_dekads(cube, period="month", method="mean")
    assert monthly.sizes["t"] == 1
    expected = (10 * 1 + 11 * 2) / 21
    assert float(monthly.isel(t=0, y=0, x=0)) == pytest.approx(expected, abs=1e-12)


def test_a_non_dekadal_cube_is_refused() -> None:
    """Run on daily or monthly data the weighting would produce plausible nonsense."""
    daily = _cube(["2026-01-01", "2026-01-02", "2026-01-03"])
    with pytest.raises(ValueError, match="expects a dekadal cube"):
        aggregate_dekads(daily)


def test_unknown_period_and_method_are_rejected() -> None:
    cube = _cube(dekad_period_ids("2026-01-01", "2026-01-31"))
    with pytest.raises(ValueError, match="Unknown period"):
        aggregate_dekads(cube, period="fortnight")
    with pytest.raises(ValueError, match="Unknown method"):
        aggregate_dekads(cube, method="median")


def test_summing_a_per_day_rate_is_warned_about(ocs_logs: Any) -> None:
    """Adding daily rates does not produce a total, and the units say so."""
    cube = _cube(dekad_period_ids("2026-01-01", "2026-01-31"), units="gC/m2/day")
    aggregate_dekads(cube, method="sum")
    assert "per-day rate" in ocs_logs.text


def test_provenance_records_the_weighting() -> None:
    """A consumer must be able to tell a day-weighted aggregate from an observation."""
    cube = _cube(dekad_period_ids("2026-01-01", "2026-12-31"))
    monthly = aggregate_dekads(cube, period="month", method="mean")
    assert monthly.attrs["cell_methods"] == "time: mean (interval: 10 day comment: day-weighted mean from dekads)"
    assert monthly.attrs["units"] == "gC/m2/day"  # a mean of a rate keeps its units


def test_the_process_is_registered_and_the_workflow_wires_to_it() -> None:
    import json
    from pathlib import Path

    from open_climate_service.openeo.execution import _build_process_registry

    assert "aggregate_dekads" in _build_process_registry().store["predefined"]

    path = Path("open_climate_service/plugins/workflows/aggregate_dekads_to_period.json")
    graph = json.loads(path.read_text())["process_graph"]
    # A literal process_id, not one parameterised through `from_parameter` — the latter is
    # not openEO-spec-compliant and was removed from the other aggregate workflows.
    assert graph["aggregated"]["process_id"] == "aggregate_dekads"
    assert graph["aggregated"]["arguments"]["period"] == {"from_parameter": "period"}
