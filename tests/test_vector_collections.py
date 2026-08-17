"""Named vector collections and geometry-preserving vector output (CLIM-836)."""

from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401  # pyright: ignore[reportUnusedImport]  # activates .rio
import xarray as xr
from fastapi.testclient import TestClient
from shapely.geometry import Polygon

from open_climate_service.openeo import jobs
from open_climate_service.plugins.processes.aggregate_spatial import aggregate_spatial
from open_climate_service.plugins.processes.load_vector_cube import load_vector_cube
from open_climate_service.shared import vectors
from open_climate_service.shared.vectors import GEOMETRY_WKT_COORD


@pytest.fixture
def vector_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point `vectors.vector_dir()` at a temporary directory and return it.

    `vector_dir()` derives from DOWNLOAD_DIR's parent, matching how the rest of the service
    locates per-instance data.
    """
    monkeypatch.setattr("open_climate_service.data_manager.services.downloader.DOWNLOAD_DIR", tmp_path / "downloads")
    target = tmp_path / "vector"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _write_districts(directory: Path, name: str = "districts", crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Two stacked boxes covering (0,0)-(4,4): north is y 2-4, south is y 0-2."""
    frame = gpd.GeoDataFrame(
        {"ou_code": ["MW.N", "MW.S"], "district": ["Northern", "Southern"], "pop": [1000, 2000]},
        geometry=[Polygon([(0, 2), (4, 2), (4, 4), (0, 4)]), Polygon([(0, 0), (4, 0), (4, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    if crs != "EPSG:4326":
        frame = frame.to_crs(crs)
    frame.to_parquet(directory / f"{name}.parquet")
    return frame


def _grid() -> xr.Dataset:
    """A 4x4 two-step cube whose values are 30/20/10/0 by row, north to south."""
    rows = np.arange(4.0)[::-1] * 10  # 30, 20, 10, 0
    data = np.tile(rows[:, None], (1, 4))[None, :, :].repeat(2, axis=0)
    return xr.Dataset(
        {"t2m": (("time", "y", "x"), data, {"units": "degC"})},
        coords={
            "time": pd.date_range("2024-01-01", periods=2),
            "y": np.arange(0.5, 4.5, 1.0)[::-1],
            "x": np.arange(0.5, 4.5, 1.0),
        },
    ).rio.write_crs("EPSG:4326")


def _mean(data: Any) -> float:
    return float(np.mean(data))


def _write(ds: xr.Dataset, results_dir: Path, fmt: str) -> Path:
    """`jobs._write_raster` in a form that asserts it wrote something, and returns the path."""
    written = jobs._write_raster(ds, results_dir, fmt)
    assert written is not None
    return Path(written)


# --- discovery -------------------------------------------------------------------------------


def test_lists_collections_with_metadata_from_the_file(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    (listed,) = vectors.list_collections()
    assert listed["id"] == "districts"
    assert listed["feature_count"] == 2
    assert listed["properties"] == ["ou_code", "district", "pop"]
    assert listed["geometry_types"] == ["Polygon"]
    assert listed["bbox"] == [0.0, 0.0, 4.0, 4.0]


def test_reports_a_short_crs_label_not_the_projjson_document(vector_data_dir: Path) -> None:
    """`str(crs)` on a CRS read from GeoParquet is the whole PROJJSON document."""
    _write_districts(vector_data_dir)
    info = vectors.describe("districts")
    assert info is not None
    assert info["crs"] == "EPSG:4326"


def test_lists_nothing_when_the_instance_ships_no_vector_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("open_climate_service.data_manager.services.downloader.DOWNLOAD_DIR", tmp_path / "downloads")
    assert vectors.list_collections() == []


def test_skips_an_unreadable_file_rather_than_failing_the_listing(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    (vector_data_dir / "corrupt.parquet").write_bytes(b"not parquet")
    assert [info["id"] for info in vectors.list_collections()] == ["districts"]


@pytest.mark.parametrize("bad_id", ["../secrets", "Districts", "with space", "", "-leading"])
def test_rejects_ids_that_are_not_collection_ids(vector_data_dir: Path, bad_id: str) -> None:
    """The id arrives from a URL or a process argument, so it cannot address arbitrary paths."""
    assert vectors.collection_path(bad_id) is None
    assert vectors.describe(bad_id) is None


def test_traversal_cannot_reach_a_parquet_file_outside_the_vector_dir(vector_data_dir: Path) -> None:
    outside = vector_data_dir.parent / "elsewhere.parquet"
    _write_districts(vector_data_dir.parent, name="elsewhere")
    assert outside.is_file()  # the file really is there, so only the id check keeps it out
    assert vectors.collection_path("../elsewhere") is None


# --- loading ---------------------------------------------------------------------------------


def test_loads_a_collection_as_a_feature_collection(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    payload = load_vector_cube("districts")
    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 2


def test_id_property_becomes_the_feature_id(vector_data_dir: Path) -> None:
    """The feature id becomes the geometry label, which is the DHIS2/CHAP location column."""
    _write_districts(vector_data_dir)
    payload = load_vector_cube("districts", id_property="ou_code")
    assert [feature["id"] for feature in payload["features"]] == ["MW.N", "MW.S"]


def test_properties_selects_the_columns_read(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    payload = load_vector_cube("districts", properties=["district"])
    assert list(payload["features"][0]["properties"]) == ["district"]


def test_reprojects_a_projected_collection_to_lonlat(vector_data_dir: Path) -> None:
    """aggregate_spatial masks against a lon/lat grid, so metre coordinates would match no pixel."""
    _write_districts(vector_data_dir, name="utm", crs="EPSG:32736")
    payload = load_vector_cube("utm")
    lon, lat = payload["features"][0]["geometry"]["coordinates"][0][0]
    assert -180 <= lon <= 180
    assert -90 <= lat <= 90


def test_unknown_collection_names_the_available_ones(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    with pytest.raises(ValueError, match="Unknown vector collection 'nope'.*districts"):
        load_vector_cube("nope")


def test_unknown_id_property_names_the_available_columns(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    with pytest.raises(ValueError, match="no property 'code'.*ou_code"):
        load_vector_cube("districts", id_property="code")


# --- routes ----------------------------------------------------------------------------------


def test_route_lists_and_describes_collections(client: TestClient, vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    listing = client.get("/vector-collections")
    assert listing.status_code == 200
    assert [info["id"] for info in listing.json()] == ["districts"]

    detail = client.get("/vector-collections/districts")
    assert detail.status_code == 200
    assert detail.json()["feature_count"] == 2


def test_route_404s_for_an_unknown_or_unsafe_id(client: TestClient, vector_data_dir: Path) -> None:
    assert client.get("/vector-collections/nope").status_code == 404
    assert client.get("/vector-collections/..%2F..%2Fetc%2Fpasswd").status_code == 404


# --- geometry survives the aggregation and reaches vector output -------------------------------


def test_aggregation_keeps_the_shapes_alongside_the_labels(vector_data_dir: Path) -> None:
    _write_districts(vector_data_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    # The labels stay the feature ids -- the exports key their location column on them.
    assert list(result.geometry.values) == ["MW.N", "MW.S"]
    # ...and the shapes ride alongside as WKT.
    wkt = list(result[GEOMETRY_WKT_COORD].values)
    assert all(text.startswith("POLYGON") for text in wkt)
    assert result.t2m.values.tolist() == [[25.0, 25.0], [5.0, 5.0]]  # north (30+20)/2, south (10+0)/2


def test_parquet_output_is_geoparquet_with_real_geometry(vector_data_dir: Path, tmp_path: Path) -> None:
    """The CLIM-836 bug: PARQUET on a vector cube silently wrote a Zarr store instead.

    The geometry labels are feature ids, so parsing them as WKT failed; the failure was swallowed
    and the raster writer took over, leaving `result.zarr` for a client that asked for Parquet.
    """
    _write_districts(vector_data_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    written = _write(result, tmp_path, "PARQUET")
    assert written.suffix == ".parquet"
    assert written.is_file()

    frame = gpd.read_parquet(written)
    assert len(frame) == 4  # two districts x two time steps
    assert sorted(frame.geom_type.unique()) == ["Polygon"]
    assert sorted(set(frame["geometry_id"])) == ["MW.N", "MW.S"]
    assert [float(v) for v in frame.total_bounds] == [0.0, 0.0, 4.0, 4.0]


def test_geojson_output_carries_the_geometry_too(vector_data_dir: Path, tmp_path: Path) -> None:
    _write_districts(vector_data_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    written = _write(result, tmp_path, "GEOJSON")
    assert written.suffix == ".geojson"
    frame = gpd.read_file(written)
    assert sorted(frame.geom_type.unique()) == ["Polygon"]


@pytest.mark.parametrize(("fmt", "suffix"), [("ZARR", ".zarr"), ("NETCDF", ".nc"), ("CSV", ".csv")])
def test_non_vector_formats_are_still_honoured_for_a_vector_cube(
    vector_data_dir: Path, tmp_path: Path, fmt: str, suffix: str
) -> None:
    """A vector cube asked for a non-vector format must not be diverted to vector output."""
    _write_districts(vector_data_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    results_dir = tmp_path / fmt
    results_dir.mkdir()
    written = _write(result, results_dir, fmt)
    assert written.suffix == suffix


def test_csv_output_does_not_leak_the_wkt_carrier_column(vector_data_dir: Path, tmp_path: Path) -> None:
    """CSV keeps the columns it always had: the WKT coordinate is an internal carrier.

    The tabular exports identify their value column by elimination, so a stray coordinate either
    becomes a bogus value column or makes the export refuse the cube.
    """
    _write_districts(vector_data_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    written = _write(result, tmp_path, "CSV")
    header = written.read_text(encoding="utf-8").splitlines()[0]
    assert GEOMETRY_WKT_COORD not in header
    assert "geometry" in header  # the labels are still there -- they are the location column
    assert "t2m" in header


def test_a_client_supplied_feature_collection_still_works(tmp_path: Path) -> None:
    """Named collections are an addition -- posting GeoJSON must keep working unchanged."""
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "north",
                "properties": {},
                "geometry": {"type": "Polygon", "coordinates": [[[0, 2], [4, 2], [4, 4], [0, 4], [0, 2]]]},
            }
        ],
    }
    result = aggregate_spatial(_grid(), geojson, _mean)
    assert list(result.geometry.values) == ["north"]
    written = _write(result, tmp_path, "PARQUET")
    assert sorted(gpd.read_parquet(written).geom_type.unique()) == ["Polygon"]
