"""The published-store layout contract, enforced at the write boundary (CLIM-821).

Dimension naming, axis order and CRS consistency are properties of a published GeoZarr, not
of any one source. Enforced per-plugin they get forgotten one plugin at a time — that is how
datasets shipped on a ``time`` dim the map viewer could not find, and how Norway's WorldPop
store came to declare ``EPSG:32633`` over degree coordinates.

Deliberately absent: any assertion about the *direction* of ``y``. See
``shared/raster_contract`` for why, and ``docs/conventions.md`` for what readers must do
instead.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pytest

xr = pytest.importorskip("xarray")
zarr = pytest.importorskip("zarr")

from open_climate_service.shared.raster_contract import (  # noqa: E402
    _axis_units,
    normalize_dim_layout,
    normalize_longitudes,
    published_contract_violations,
    resolve_store_crs,
)

# Deciding that a CRS is projected needs a working PROJ database. When pyproj cannot read one
# (a polluted host PROJ_DATA is a common developer-machine failure — see
# `startup._ensure_proj_database`), `_axis_units` reports "unknown" and the CRS checks
# deliberately stand down rather than guessing. Skip the assertions that need a real answer.
_CRS_RESOLVABLE = _axis_units("EPSG:32633") == "linear"
_needs_proj = pytest.mark.skipif(not _CRS_RESOLVABLE, reason="pyproj cannot resolve EPSG:32633 here")


@pytest.fixture
def ocs_logs(caplog: pytest.LogCaptureFixture) -> Any:
    """Capture warnings from the OCS package logger.

    ``startup.py`` sets ``propagate = False`` on it so the app owns its own handler, which
    means caplog's root handler never sees the records. Re-enable propagation for the test.
    """
    package_logger = logging.getLogger("open_climate_service")
    previous = package_logger.propagate
    package_logger.propagate = True
    try:
        with caplog.at_level(logging.WARNING, logger="open_climate_service"):
            yield caplog
    finally:
        package_logger.propagate = previous


def _cube(*, dims: tuple[str, ...], sizes: dict[str, int], coords: dict[str, Any] | None = None) -> Any:
    shape = tuple(sizes[d] for d in dims)
    data = np.arange(int(np.prod(shape)), dtype="float32").reshape(shape)
    return xr.Dataset({"v": (dims, data)}, coords=coords or {})


# --- invariant 1: the temporal dimension is named `t` -------------------------------------


@pytest.mark.parametrize("alias", ["time", "valid_time", "date", "time_counter"])
def test_temporal_aliases_are_renamed_to_t(alias: str) -> None:
    """The map viewer looks for `t`; a store shipping `time` renders with no time control."""
    ds = _cube(dims=(alias, "y", "x"), sizes={alias: 2, "y": 3, "x": 4})
    out = normalize_dim_layout(ds)
    assert "t" in out.dims
    assert alias not in out.dims


def test_an_existing_t_dim_is_left_alone() -> None:
    """A cube with both `t` and `time` must not have `time` collapsed onto it."""
    ds = _cube(dims=("t", "y", "x"), sizes={"t": 2, "y": 3, "x": 4})
    ds = ds.assign_coords(time=("t", np.array(["2026-01-01", "2026-01-02"], dtype="datetime64[ns]")))
    out = normalize_dim_layout(ds)
    assert "t" in out.dims
    assert "time" in out.coords  # kept as an auxiliary coordinate, not renamed over `t`


# --- invariant 2: spatial dims are (…, y, x) ----------------------------------------------


def test_transposed_variable_is_reordered() -> None:
    ds = _cube(dims=("t", "x", "y"), sizes={"t": 2, "x": 4, "y": 3})
    out = normalize_dim_layout(ds)
    assert out["v"].dims == ("t", "y", "x")


def test_reordering_moves_data_not_just_labels() -> None:
    """A transpose that relabelled without moving values would silently mirror the raster."""
    values = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype="float32")  # (x=2, y=3)
    ds = xr.Dataset({"v": (("x", "y"), values)}, coords={"x": [10.0, 11.0], "y": [50.0, 51.0, 52.0]})

    out = normalize_dim_layout(ds)

    assert out["v"].dims == ("y", "x")
    np.testing.assert_array_equal(out["v"].values, values.T)
    # The value at a given coordinate pair is unchanged — only its position in the array.
    assert float(out["v"].sel(x=11.0, y=52.0)) == float(ds["v"].sel(x=11.0, y=52.0)) == 6.0


def test_spatial_dims_end_up_last_even_with_several_leading_dims() -> None:
    ds = _cube(dims=("y", "t", "x", "age"), sizes={"y": 3, "t": 2, "x": 4, "age": 5})
    out = normalize_dim_layout(ds)
    assert out["v"].dims[-2:] == ("y", "x")
    assert set(out["v"].dims[:-2]) == {"t", "age"}


def test_conformant_dataset_is_returned_untouched() -> None:
    """The common case must not pay for a copy of the data."""
    ds = _cube(dims=("t", "y", "x"), sizes={"t": 2, "y": 3, "x": 4})
    out = normalize_dim_layout(ds)
    assert out["v"].dims == ("t", "y", "x")
    assert out["v"].values is ds["v"].values


def test_non_spatial_variables_are_ignored() -> None:
    """A scalar CRS holder has no y/x to order, and must survive normalisation."""
    ds = _cube(dims=("t", "x", "y"), sizes={"t": 2, "x": 4, "y": 3})
    ds["spatial_ref"] = xr.DataArray(0)
    out = normalize_dim_layout(ds)
    assert "spatial_ref" in out
    assert out["v"].dims == ("t", "y", "x")


def test_missing_spatial_dims_are_left_for_the_caller_to_report() -> None:
    """The orchestrator raises a clearer error about missing y/x than a transpose would."""
    ds = _cube(dims=("time", "station"), sizes={"time": 2, "station": 5})
    out = normalize_dim_layout(ds)
    assert "t" in out.dims  # the temporal rename still happens
    assert out["v"].dims == ("t", "station")


# --- invariant 3: geographic longitudes run -180…180 --------------------------------------


def test_longitudes_are_rolled_off_the_0_360_frame() -> None:
    ds = _cube(dims=("y", "x"), sizes={"y": 2, "x": 4}, coords={"y": [1.0, 2.0], "x": [0.0, 90.0, 270.0, 350.0]})
    out = normalize_longitudes(ds, crs="EPSG:4326")
    assert out["x"].values.tolist() == [-90.0, -10.0, 0.0, 90.0]  # rolled and re-sorted


def test_rolling_carries_the_data_with_the_coordinates() -> None:
    """Rolling without re-sorting the data would shift every value by half the globe."""
    ds = xr.Dataset(
        {"v": (("y", "x"), np.array([[1.0, 2.0, 3.0, 4.0]], dtype="float32"))},
        coords={"y": [0.0], "x": [0.0, 90.0, 270.0, 350.0]},
    )
    out = normalize_longitudes(ds, crs="EPSG:4326")
    assert out["v"].sel(x=-90.0, y=0.0).item() == 3.0  # was 270°
    assert out["v"].sel(x=0.0, y=0.0).item() == 1.0


def test_already_rolled_longitudes_are_untouched() -> None:
    ds = _cube(dims=("y", "x"), sizes={"y": 2, "x": 3}, coords={"y": [1.0, 2.0], "x": [-10.0, 0.0, 10.0]})
    out = normalize_longitudes(ds, crs="EPSG:4326")
    assert out["x"].values.tolist() == [-10.0, 0.0, 10.0]


@_needs_proj
def test_projected_eastings_are_never_rolled() -> None:
    """A UTM easting is legitimately far larger than 180 — rolling it would be destructive."""
    ds = _cube(
        dims=("y", "x"),
        sizes={"y": 2, "x": 3},
        coords={"y": [6450500.0, 6451500.0], "x": [-74500.0, 500000.0, 1119500.0]},
    )
    out = normalize_longitudes(ds, crs="EPSG:32633")
    assert out["x"].values.tolist() == [-74500.0, 500000.0, 1119500.0]


# --- invariant 4: the declared CRS matches the coordinates --------------------------------


def _degree_grid(declared: str | None = None) -> Any:
    # Norway's WorldPop extent, the store that reported this bug.
    ds = _cube(
        dims=("y", "x"),
        sizes={"y": 2, "x": 2},
        coords={"y": [71.1846, 57.9596], "x": [4.5037, 31.1679]},
    )
    if declared is not None:
        ds.attrs["proj:code"] = declared
    return ds


@_needs_proj
def test_projected_crs_over_degree_coordinates_is_refused(ocs_logs: Any) -> None:
    """The reported failure: EPSG:32633 declared over lon/lat puts the store at the UTM origin."""
    ds = _degree_grid(declared="EPSG:4326")

    resolved = resolve_store_crs(ds, "EPSG:32633")

    assert resolved == "EPSG:4326"
    assert "EPSG:32633" in ocs_logs.text
    assert "±180/±90" in ocs_logs.text  # the log names the evidence, not just the verdict


@_needs_proj
def test_a_grid_still_on_the_0_360_frame_is_recognised_as_geographic(ocs_logs: Any) -> None:
    """Chicken-and-egg: the roll needs to know the axis is a longitude, but an unrolled axis
    exceeds 180. Testing longitude against 180 would make every 0…360 dataset look projected,
    so the check allows up to 360 and leans on latitude (±90) for the real evidence.
    """
    ds = _cube(
        dims=("y", "x"),
        sizes={"y": 2, "x": 3},
        coords={"y": [60.0, 59.0], "x": [0.0, 180.0, 350.0]},
    )
    assert resolve_store_crs(ds, "EPSG:32633") == "EPSG:4326"
    assert "EPSG:32633" in ocs_logs.text


@_needs_proj
def test_an_unresolvable_crs_is_left_as_declared() -> None:
    """When we cannot tell whether a CRS is projected, stand down rather than guess."""
    ds = _degree_grid()
    assert resolve_store_crs(ds, "EPSG:999999") == "EPSG:999999"


@_needs_proj
def test_a_consistent_projected_crs_is_kept() -> None:
    """Only a contradiction is overridden — a real projected grid must survive untouched."""
    ds = _cube(
        dims=("y", "x"),
        sizes={"y": 2, "x": 2},
        coords={"y": [6450500.0, 7999500.0], "x": [-74500.0, 1119500.0]},
    )
    assert resolve_store_crs(ds, "EPSG:32633") == "EPSG:32633"


@_needs_proj
def test_a_sticky_wrong_crs_falls_back_to_wgs84(ocs_logs: Any) -> None:
    """A wrong `proj:code` is copied forward on every rewrite, so the data's CRS can be wrong too.

    Norway's WorldPop store is in exactly this state: root says EPSG:32633, level 0 says
    EPSG:4326. Neither source agreeing with the coordinates must not propagate the bad code.
    """
    ds = _degree_grid(declared="EPSG:32633")

    assert resolve_store_crs(ds, "EPSG:32633") == "EPSG:4326"

    assert "also projected" in ocs_logs.text


def test_no_declared_crs_uses_the_datasets_own() -> None:
    assert resolve_store_crs(_degree_grid(declared="EPSG:4326"), None) == "EPSG:4326"


def test_crs84_aliases_are_canonicalised() -> None:
    assert resolve_store_crs(_degree_grid(), "OGC:CRS84") == "EPSG:4326"


def test_the_instance_config_crs_is_never_consulted(monkeypatch: pytest.MonkeyPatch) -> None:
    """The config CRS as a fallback is what created the mismatch in the first place."""
    from open_climate_service import config as api_config

    monkeypatch.setattr(api_config, "get_crs", lambda: "EPSG:32633")
    assert resolve_store_crs(_degree_grid(), None) == "EPSG:4326"


# --- the contract as a whole ---------------------------------------------------------------


def test_violations_lists_every_problem_at_once() -> None:
    ds = _cube(dims=("time", "x", "y"), sizes={"time": 2, "x": 4, "y": 3})
    ds = ds.assign_coords(x=[0.0, 90.0, 270.0, 350.0], y=[1.0, 2.0, 3.0])
    ds.attrs["proj:code"] = "EPSG:32633"

    problems = "\n".join(published_contract_violations(ds))

    assert "not named 't'" in problems
    assert "not (…, y, x)" in problems
    if _CRS_RESOLVABLE:
        assert "projected but the coordinates are degrees" in problems


def test_a_normalised_dataset_has_no_violations() -> None:
    ds = _cube(dims=("time", "x", "y"), sizes={"time": 2, "x": 4, "y": 3})
    ds = ds.assign_coords(x=[0.0, 90.0, 270.0, 350.0], y=[1.0, 2.0, 3.0])

    ds = normalize_dim_layout(ds)
    ds = normalize_longitudes(ds, crs="EPSG:4326")
    ds.attrs["proj:code"] = resolve_store_crs(ds, "EPSG:4326")

    assert published_contract_violations(ds) == []


def test_y_direction_is_not_a_violation() -> None:
    """Both directions are valid; readers honour the coordinate. See docs/conventions.md."""
    descending = _cube(dims=("t", "y", "x"), sizes={"t": 1, "y": 2, "x": 2}, coords={"y": [60.0, 59.0]})
    ascending = _cube(dims=("t", "y", "x"), sizes={"t": 1, "y": 2, "x": 2}, coords={"y": [59.0, 60.0]})
    assert published_contract_violations(descending) == []
    assert published_contract_violations(ascending) == []


def test_written_store_satisfies_the_contract(tmp_path: Path) -> None:
    """End to end through the real write path, on a dataset that violates every invariant."""
    import icechunk

    pytest.importorskip("rioxarray")
    from open_climate_service.data_manager.services.downloader import write_to_icechunk_store

    # (time, x, y) order, a `time` dim, 0…360 longitudes, and a projected CRS over degrees.
    ds = xr.Dataset(
        {"v": (("time", "x", "y"), np.ones((1, 4, 3), dtype="float32"))},
        coords={
            "time": np.array(["2026-01-01"], dtype="datetime64[ns]"),
            "x": [0.0, 10.0, 350.0, 355.0],
            "y": [60.0, 59.0, 58.0],
        },
    )

    store_path = tmp_path / "contract.icechunk"
    write_to_icechunk_store(ds, store_path, t_dim="time", crs="EPSG:32633")

    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_path)))
    written = xr.open_zarr(repo.readonly_session("main").store, consolidated=False, zarr_format=3)
    try:
        assert published_contract_violations(written) == []
        assert written["v"].dims == ("t", "y", "x")
        assert written["x"].values.min() < 0  # rolled off the 0…360 frame
        assert written.attrs["proj:code"] == "EPSG:4326"
    finally:
        written.close()
