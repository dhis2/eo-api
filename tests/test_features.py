"""Named feature collections and geometry-preserving vector output (CLIM-836)."""

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
from open_climate_service.shared import features
from open_climate_service.shared.features import GEOMETRY_WKT_COORD


@pytest.fixture
def features_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point `features.features_dir()` at a temporary directory and return it.

    `features_dir()` derives from DOWNLOAD_DIR's parent, matching how the rest of the service
    locates per-instance data.
    """
    monkeypatch.setattr("open_climate_service.data_manager.services.downloader.DOWNLOAD_DIR", tmp_path / "downloads")
    target = tmp_path / "features"
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


def test_lists_collections_with_metadata_from_the_file(features_dir: Path) -> None:
    _write_districts(features_dir)
    (listed,) = features.list_collections()
    assert listed["id"] == "districts"
    assert listed["feature_count"] == 2
    assert listed["properties"] == ["ou_code", "district", "pop"]
    assert listed["geometry_types"] == ["Polygon"]
    assert listed["bbox"] == [0.0, 0.0, 4.0, 4.0]


def test_reports_a_short_crs_label_not_the_projjson_document(features_dir: Path) -> None:
    """`str(crs)` on a CRS read from GeoParquet is the whole PROJJSON document."""
    _write_districts(features_dir)
    info = features.describe("districts")
    assert info is not None
    assert info["crs"] == "EPSG:4326"


def test_lists_nothing_when_the_instance_ships_no_features_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("open_climate_service.data_manager.services.downloader.DOWNLOAD_DIR", tmp_path / "downloads")
    assert features.list_collections() == []


def test_skips_an_unreadable_file_rather_than_failing_the_listing(features_dir: Path) -> None:
    _write_districts(features_dir)
    (features_dir / "corrupt.parquet").write_bytes(b"not parquet")
    assert [info["id"] for info in features.list_collections()] == ["districts"]


@pytest.mark.parametrize("bad_id", ["../secrets", "Districts", "with space", "", "-leading"])
def test_rejects_ids_that_are_not_collection_ids(features_dir: Path, bad_id: str) -> None:
    """The id arrives from a URL or a process argument, so it cannot address arbitrary paths."""
    assert features.collection_path(bad_id) is None
    assert features.describe(bad_id) is None


def test_traversal_cannot_reach_a_parquet_file_outside_the_features_dir(features_dir: Path) -> None:
    outside = features_dir.parent / "elsewhere.parquet"
    _write_districts(features_dir.parent, name="elsewhere")
    assert outside.is_file()  # the file really is there, so only the id check keeps it out
    assert features.collection_path("../elsewhere") is None


def test_describe_reads_bounds_from_the_footer_not_every_geometry(
    features_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`GET /features` describes every collection, so this must not scale with features.

    Bounds and geometry types are in the GeoParquet footer. Reading the geometry column instead
    is ~400x slower on 300k features, and the listing pays it per collection per request.
    """
    _write_districts(features_dir)

    def _refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("describe() must not read the geometry column when metadata suffices")

    monkeypatch.setattr(gpd, "read_parquet", _refuse)

    info = features.describe("districts")
    assert info is not None
    assert info["bbox"] == [0.0, 0.0, 4.0, 4.0]
    assert info["geometry_types"] == ["Polygon"]
    assert info["crs"] == "EPSG:4326"
    assert info["feature_count"] == 2


def test_describe_reports_whether_windowed_reads_are_cheap(features_dir: Path) -> None:
    """A covering bbox is the difference between pruning row groups and reading everything."""
    _write_districts(features_dir, name="plain")
    frame = _write_districts(features_dir, name="unused")
    (features_dir / "unused.parquet").unlink()
    frame.to_parquet(features_dir / "covered.parquet", write_covering_bbox=True)

    plain = features.describe("plain")
    covered = features.describe("covered")
    assert plain is not None and covered is not None
    assert plain["supports_bbox_filter"] is False
    assert covered["supports_bbox_filter"] is True


# --- loading ---------------------------------------------------------------------------------


def test_loads_a_collection_as_a_feature_collection(features_dir: Path) -> None:
    _write_districts(features_dir)
    payload = load_vector_cube("districts")
    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 2


def test_id_property_becomes_the_feature_id(features_dir: Path) -> None:
    """The feature id becomes the geometry label, which is the DHIS2/CHAP location column."""
    _write_districts(features_dir)
    payload = load_vector_cube("districts", id_property="ou_code")
    assert [feature["id"] for feature in payload["features"]] == ["MW.N", "MW.S"]


def test_properties_selects_the_columns_read(features_dir: Path) -> None:
    _write_districts(features_dir)
    payload = load_vector_cube("districts", properties=["district"])
    assert list(payload["features"][0]["properties"]) == ["district"]


def test_reprojects_a_projected_collection_to_lonlat(features_dir: Path) -> None:
    """aggregate_spatial masks against a lon/lat grid, so metre coordinates would match no pixel."""
    _write_districts(features_dir, name="utm", crs="EPSG:32736")
    payload = load_vector_cube("utm")
    lon, lat = payload["features"][0]["geometry"]["coordinates"][0][0]
    assert -180 <= lon <= 180
    assert -90 <= lat <= 90


def test_unknown_collection_names_the_available_ones(features_dir: Path) -> None:
    _write_districts(features_dir)
    with pytest.raises(ValueError, match="Unknown feature collection 'nope'.*districts"):
        load_vector_cube("nope")


def test_unknown_id_property_names_the_available_columns(features_dir: Path) -> None:
    _write_districts(features_dir)
    with pytest.raises(ValueError, match="no property 'code'.*ou_code"):
        load_vector_cube("districts", id_property="code")


def _write_ids(directory: Path, ids: list[Any], name: str = "wonky") -> None:
    """A two-feature collection whose `ou_code` column is whatever the test needs it to be."""
    gpd.GeoDataFrame(
        {"ou_code": ids},
        geometry=[Polygon([(0, 2), (4, 2), (4, 4), (0, 4)]), Polygon([(0, 0), (4, 0), (4, 2), (0, 2)])],
        crs="EPSG:4326",
    ).to_parquet(directory / f"{name}.parquet")


def test_duplicate_ids_are_refused_rather_than_exported_as_one_location(features_dir: Path) -> None:
    """Two features under one id aggregate to one label, and DHIS2 keeps whichever push lands last.

    Wrong values rather than missing ones, so this has to fail here and not at the export.
    """
    _write_ids(features_dir, ["MW.N", "MW.N"])
    with pytest.raises(ValueError, match="sharing a 'ou_code'.*MW.N"):
        load_vector_cube("wonky", id_property="ou_code")


def test_null_ids_are_refused_rather_than_written_as_the_string_nan(features_dir: Path) -> None:
    _write_ids(features_dir, ["MW.N", None])
    with pytest.raises(ValueError, match="1 feature.* with no 'ou_code'"):
        load_vector_cube("wonky", id_property="ou_code")


def test_distinct_ids_pass(features_dir: Path) -> None:
    _write_ids(features_dir, ["MW.N", "MW.S"])
    payload = load_vector_cube("wonky", id_property="ou_code")
    assert [feature["id"] for feature in payload["features"]] == ["MW.N", "MW.S"]


def test_read_features_serves_a_file_outside_the_collection_store(tmp_path: Path) -> None:
    """The reader takes a path so a resolved feature cache (CLIM-926) can share it.

    The cache is deliberately not a collection — not listed, not loadable by id — so the read has
    to work without the file living in the vector directory or having a valid collection id.
    """
    cache = tmp_path / "cache" / "features"
    cache.mkdir(parents=True)
    _write_ids(cache, ["MW.N", "MW.S"], name="dhis2 level=2")

    payload = features.read_features(
        cache / "dhis2 level=2.parquet", source="Feature cache 'districts'", id_property="ou_code"
    )

    assert [feature["id"] for feature in payload["features"]] == ["MW.N", "MW.S"]
    assert features.collection_path("dhis2 level=2") is None


def test_read_features_names_its_source_in_errors_rather_than_calling_it_a_collection(tmp_path: Path) -> None:
    _write_ids(tmp_path, ["MW.N", "MW.N"], name="cached")
    with pytest.raises(ValueError, match="Feature cache 'districts' has"):
        features.read_features(tmp_path / "cached.parquet", source="Feature cache 'districts'", id_property="ou_code")


def test_a_collection_too_large_to_materialise_is_refused(features_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`load_vector_cube` builds Python dicts per feature, so an unbounded read is a foot-gun.

    `/features` advertises every collection and the process accepts any id, so once a
    building-footprint collection exists, one unqualified call would pull millions of features
    into memory. 300k polygons already cost ~5.6 s and ~105 MB as dicts.
    """
    _write_districts(features_dir)
    monkeypatch.setattr(features, "MAX_FEATURES", 1)

    with pytest.raises(ValueError, match="more than the 1 that can be loaded"):
        load_vector_cube("districts")

    # ...but a windowed read is allowed, since its cost is bounded by the window.
    payload = load_vector_cube("districts", bbox=[0.0, 0.0, 4.0, 1.0])
    assert payload["type"] == "FeatureCollection"


def test_bbox_restricts_the_features_returned(features_dir: Path) -> None:
    """A window over the southern half returns only the southern district."""
    _write_districts(features_dir)
    payload = load_vector_cube("districts", id_property="ou_code", bbox=[0.0, 0.0, 4.0, 1.0])
    assert [feature["id"] for feature in payload["features"]] == ["MW.S"]


def test_bbox_uses_row_group_pruning_when_the_file_supports_it(features_dir: Path) -> None:
    """With a covering bbox the read is pushed down; without one it falls back to read-and-clip.

    Both must return the same features -- the covering bbox is a performance property, not a
    semantic one.
    """
    frame = _write_districts(features_dir, name="plain")
    frame.to_parquet(features_dir / "covered.parquet", write_covering_bbox=True)

    window = [0.0, 0.0, 4.0, 1.0]
    plain = load_vector_cube("plain", id_property="ou_code", bbox=window)
    covered = load_vector_cube("covered", id_property="ou_code", bbox=window)
    assert [f["id"] for f in plain["features"]] == [f["id"] for f in covered["features"]] == ["MW.S"]


# --- routes ----------------------------------------------------------------------------------


def test_route_lists_and_describes_collections(client: TestClient, features_dir: Path) -> None:
    _write_districts(features_dir)
    listing = client.get("/features")
    assert listing.status_code == 200
    assert [info["id"] for info in listing.json()] == ["districts"]

    detail = client.get("/features/districts")
    assert detail.status_code == 200
    assert detail.json()["feature_count"] == 2


def test_route_404s_for_an_unknown_or_unsafe_id(client: TestClient, features_dir: Path) -> None:
    assert client.get("/features/nope").status_code == 404
    assert client.get("/features/..%2F..%2Fetc%2Fpasswd").status_code == 404


# --- geometry survives the aggregation and reaches vector output -------------------------------


def test_aggregation_keeps_the_shapes_alongside_the_labels(features_dir: Path) -> None:
    _write_districts(features_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    # The labels stay the feature ids -- the exports key their location column on them.
    assert list(result.geometry.values) == ["MW.N", "MW.S"]
    # ...and the shapes ride alongside as WKT.
    wkt = list(result[GEOMETRY_WKT_COORD].values)
    assert all(text.startswith("POLYGON") for text in wkt)
    assert result.t2m.values.tolist() == [[25.0, 25.0], [5.0, 5.0]]  # north (30+20)/2, south (10+0)/2


def test_parquet_output_is_geoparquet_with_real_geometry(features_dir: Path, tmp_path: Path) -> None:
    """The CLIM-836 bug: PARQUET on a vector cube silently wrote a Zarr store instead.

    The geometry labels are feature ids, so parsing them as WKT failed; the failure was swallowed
    and the raster writer took over, leaving `result.zarr` for a client that asked for Parquet.
    """
    _write_districts(features_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    written = _write(result, tmp_path, "PARQUET")
    assert written.suffix == ".parquet"
    assert written.is_file()

    frame = gpd.read_parquet(written)
    assert len(frame) == 4  # two districts x two time steps
    assert sorted(frame.geom_type.unique()) == ["Polygon"]
    assert sorted(set(frame["geometry_id"])) == ["MW.N", "MW.S"]
    assert [float(v) for v in frame.total_bounds] == [0.0, 0.0, 4.0, 4.0]


def test_geojson_output_carries_the_geometry_too(features_dir: Path, tmp_path: Path) -> None:
    _write_districts(features_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    written = _write(result, tmp_path, "GEOJSON")
    assert written.suffix == ".geojson"
    frame = gpd.read_file(written)
    assert sorted(frame.geom_type.unique()) == ["Polygon"]


@pytest.mark.parametrize(("fmt", "suffix"), [("ZARR", ".zarr"), ("NETCDF", ".nc"), ("CSV", ".csv")])
def test_non_vector_formats_are_still_honoured_for_a_vector_cube(
    features_dir: Path, tmp_path: Path, fmt: str, suffix: str
) -> None:
    """A vector cube asked for a non-vector format must not be diverted to vector output."""
    _write_districts(features_dir)
    result = aggregate_spatial(_grid(), load_vector_cube("districts", id_property="ou_code"), _mean)

    results_dir = tmp_path / fmt
    results_dir.mkdir()
    written = _write(result, results_dir, fmt)
    assert written.suffix == suffix


def test_csv_output_does_not_leak_the_wkt_carrier_column(features_dir: Path, tmp_path: Path) -> None:
    """CSV keeps the columns it always had: the WKT coordinate is an internal carrier.

    The tabular exports identify their value column by elimination, so a stray coordinate either
    becomes a bogus value column or makes the export refuse the cube.
    """
    _write_districts(features_dir)
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


def test_a_bbox_is_expressed_in_the_collections_own_crs(features_dir: Path) -> None:
    """The window is always lon/lat, but the filter runs before the reprojection to EPSG:4326.

    Applied unchanged to a projected collection it selects a region in metres near the origin and
    returns nothing — and above MAX_FEATURES a bbox is required, so that is the only call path such
    a collection has.
    """
    _write_districts(features_dir, name="utm", crs="EPSG:32736")

    payload = load_vector_cube("utm", bbox=[0, 0, 4, 4])

    assert len(payload["features"]) == 2


def test_a_bbox_still_excludes_what_it_should_on_a_projected_collection(features_dir: Path) -> None:
    """Transforming the window must not turn it into "everything"."""
    _write_districts(features_dir, name="utm", crs="EPSG:32736")

    payload = load_vector_cube("utm", bbox=[20, 20, 24, 24])

    assert payload["features"] == []


def test_the_covering_bbox_is_not_reported_as_a_feature_property(features_dir: Path) -> None:
    """It is a struct column, so the Parquet leaf view calls it xmin/ymin/xmax/ymax."""
    frame = _write_districts(features_dir, name="unused")
    (features_dir / "unused.parquet").unlink()
    frame.to_parquet(features_dir / "covered.parquet", write_covering_bbox=True)

    info = features.describe("covered")

    assert info is not None
    assert info["properties"] == ["ou_code", "district", "pop"]
