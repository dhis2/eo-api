"""Overture ingestion: a bbox extract straight to GeoParquet (CLIM-893).

Tested against a local GeoParquet standing in for the S3 partitions. The remote path differs only
in the URL DuckDB opens, so what these cover is the part that can be wrong: the bbox predicate, the
covering-bbox declaration, and the store recording a file it never decoded.
"""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from open_climate_service.features import resolver, store
from open_climate_service.features.config import FeatureTemplates
from open_climate_service.plugins.features.overture import overture, source_url


def _building(x: float, y: float) -> Polygon:
    return Polygon([(x, y), (x + 0.5, y), (x + 0.5, y + 0.5), (x, y + 0.5)])


@pytest.fixture
def source(tmp_path: Path) -> Path:
    """Three buildings, spread out enough that a bbox selects exactly one."""
    path = tmp_path / "source.parquet"
    gpd.GeoDataFrame(
        {"id": ["west-1", "middle-1", "east-1"], "height": [3.0, 9.0, 12.0], "class": ["house", "shed", "tower"]},
        geometry=[_building(0, 0), _building(5, 5), _building(9, 9)],
        crs="EPSG:4326",
    ).to_parquet(path, write_covering_bbox=True)
    return path


@pytest.fixture
def instance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr("open_climate_service.data_manager.services.downloader.DOWNLOAD_DIR", tmp_path / "downloads")
    monkeypatch.setattr("open_climate_service.config.get_data_root", lambda: tmp_path)
    (tmp_path / "features").mkdir(parents=True, exist_ok=True)
    return tmp_path


def _declare(monkeypatch: pytest.MonkeyPatch, *templates: dict[str, Any]) -> None:
    loaded = FeatureTemplates.model_validate({"templates": list(templates)})
    monkeypatch.setattr("open_climate_service.features.config.get_feature_templates", lambda: loaded)
    monkeypatch.setattr("open_climate_service.features.resolver.get_feature_templates", lambda: loaded)


# --- the extract ---------------------------------------------------------------------------------


def test_the_bbox_selects_only_overlapping_features(tmp_path: Path, source: Path) -> None:
    out = tmp_path / "buildings.parquet"
    overture(path=out, release="2026-08", bbox=[4.0, 4.0, 7.0, 7.0], source=str(source))

    assert list(gpd.read_parquet(out)["id"]) == ["middle-1"]


def test_a_feature_straddling_the_edge_is_kept(tmp_path: Path, source: Path) -> None:
    """Intersects, not contains — a district on the border of the extent is still in it."""
    out = tmp_path / "buildings.parquet"
    overture(path=out, release="2026-08", bbox=[5.25, 5.25, 8.0, 8.0], source=str(source))

    assert list(gpd.read_parquet(out)["id"]) == ["middle-1"]


def test_the_extract_declares_a_covering_bbox(tmp_path: Path, source: Path) -> None:
    """Without this a windowed read scans the whole file, which is the cost this exists to avoid."""
    import json

    import pyarrow.parquet as pq

    out = tmp_path / "buildings.parquet"
    overture(path=out, release="2026-08", bbox=[-1.0, -1.0, 20.0, 20.0], source=str(source))

    geo = json.loads(pq.read_metadata(out).metadata[b"geo"])
    covering = (geo["columns"][geo["primary_column"]].get("covering") or {}).get("bbox")
    assert covering, "DuckDB writes no covering entry; the ingestion must add one"
    assert len(gpd.read_parquet(out, bbox=(4, 4, 7, 7))) == 1


def test_the_release_becomes_the_version(tmp_path: Path, source: Path) -> None:
    out = tmp_path / "buildings.parquet"
    _, version = overture(path=out, release="2026-08", bbox=[-1.0, -1.0, 20.0, 20.0], source=str(source))
    assert version == "2026-08"


def test_selected_columns_are_the_only_ones_carried(tmp_path: Path, source: Path) -> None:
    """A country's buildings carry attributes nobody asked for; naming columns keeps the file small."""
    out = tmp_path / "buildings.parquet"
    overture(
        path=out,
        release="2026-08",
        bbox=[-1.0, -1.0, 20.0, 20.0],
        columns=["id", "height", "geometry"],
        source=str(source),
    )

    # geopandas hides the covering-bbox column, so it does not appear here even though it is written.
    assert set(gpd.read_parquet(out).columns) == {"id", "height", "geometry"}


@pytest.mark.parametrize("bad", [[1.0, 1.0, 0.0, 2.0], [0.0, 5.0, 2.0, 1.0], [0.0, 0.0, 1.0]])
def test_an_empty_or_malformed_bbox_is_refused(tmp_path: Path, source: Path, bad: list[float]) -> None:
    with pytest.raises(ValueError, match="bbox"):
        overture(path=tmp_path / "x.parquet", release="2026-08", bbox=bad, source=str(source))


def test_an_unknown_theme_names_the_known_ones(tmp_path: Path, source: Path) -> None:
    with pytest.raises(ValueError, match="Unknown Overture theme 'roads'.*buildings"):
        overture(path=tmp_path / "x.parquet", release="2026-08", bbox=[0.0, 0.0, 1.0, 1.0], theme="roads")


def test_the_s3_location_is_built_from_the_release_and_theme() -> None:
    assert source_url("2026-08", "buildings", "building").endswith("release/2026-08/theme=buildings/type=building/*")


# --- through the store ---------------------------------------------------------------------------


def test_a_file_writing_provider_is_recorded_without_decoding_geometry(
    instance: Path, monkeypatch: pytest.MonkeyPatch, source: Path
) -> None:
    """The whole point of the Path form: the store records the file, it does not read it back in."""
    _declare(
        monkeypatch,
        {
            "id": "buildings",
            "provider": "overture",
            "license": "ODbL-1.0",
            "attribution": "© OpenStreetMap contributors, © Overture Maps Foundation",
            "params": {"release": "2026-08", "bbox": [-1.0, -1.0, 20.0, 20.0], "source": str(source)},
        },
    )

    version = resolver.ensure_current("buildings")

    assert version == "2026-08"
    assert (instance / "features" / "buildings.parquet").is_file()
    sidecar = store.metadata("buildings")
    assert sidecar["provider"] == "overture"
    assert sidecar["feature_count"] == 3
    assert sidecar["id_property"] == "id", "Overture's own id column is the identity, not a rewritten one"


def test_a_second_call_within_the_ttl_does_not_re_extract(
    instance: Path, monkeypatch: pytest.MonkeyPatch, source: Path
) -> None:
    _declare(
        monkeypatch,
        {
            "id": "buildings",
            "provider": "overture",
            "params": {"release": "2026-08", "bbox": [-1.0, -1.0, 20.0, 20.0], "source": str(source)},
        },
    )
    first = resolver.ensure_current("buildings")
    written = (instance / "features" / "buildings.parquet").stat().st_mtime_ns

    second = resolver.ensure_current("buildings")

    assert first == second
    assert (instance / "features" / "buildings.parquet").stat().st_mtime_ns == written


def test_a_new_release_re_extracts_and_changes_the_version(
    instance: Path, monkeypatch: pytest.MonkeyPatch, source: Path
) -> None:
    """A release *is* a version, so changing it is a different question and must refetch."""
    params = {"bbox": [-1.0, -1.0, 20.0, 20.0], "source": str(source)}
    _declare(monkeypatch, {"id": "buildings", "provider": "overture", "params": {"release": "2026-08", **params}})
    assert resolver.ensure_current("buildings") == "2026-08"

    _declare(monkeypatch, {"id": "buildings", "provider": "overture", "params": {"release": "2026-09", **params}})
    assert resolver.ensure_current("buildings") == "2026-09"


def test_duplicate_ids_in_an_extract_are_refused(
    instance: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The identity contract applies to a file-writing provider too, read from the id column alone."""
    broken = tmp_path / "broken.parquet"
    gpd.GeoDataFrame({"id": ["same", "same"]}, geometry=[_building(0, 0), _building(5, 5)], crs="EPSG:4326").to_parquet(
        broken
    )
    _declare(
        monkeypatch,
        {
            "id": "buildings",
            "provider": "overture",
            "params": {"release": "2026-08", "bbox": [-1.0, -1.0, 20.0, 20.0], "source": str(broken)},
        },
    )

    with pytest.raises(ValueError, match="sharing a 'id'.*same"):
        resolver.ensure_current("buildings")


def test_an_extract_will_not_overwrite_a_curated_collection(
    instance: Path, monkeypatch: pytest.MonkeyPatch, source: Path
) -> None:
    gpd.GeoDataFrame({"id": ["kept"]}, geometry=[_building(0, 0)], crs="EPSG:4326").to_parquet(
        instance / "features" / "buildings.parquet"
    )
    _declare(
        monkeypatch,
        {
            "id": "buildings",
            "provider": "overture",
            "params": {"release": "2026-08", "bbox": [-1.0, -1.0, 20.0, 20.0], "source": str(source)},
        },
    )

    with pytest.raises(ValueError, match="not maintained by a provider"):
        resolver.ensure_current("buildings")
