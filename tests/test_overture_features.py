"""Overture ingestion: a bbox extract streamed to GeoParquet (CLIM-893).

The Overture client is stubbed with a reader built from a local GeoParquet, which is the same shape
it really returns — a `bbox` struct column plus `geo` metadata declaring it as the covering. What
these cover is therefore what this module is actually responsible for: validating the request,
projecting columns without losing that metadata, streaming batches out, and reporting the release as
the version. The client owns partition selection, and there is nothing useful to assert about that
from here — it was measured against the live bucket instead.
"""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Polygon

from open_climate_service.features import resolver, store
from open_climate_service.features.config import FeatureTemplates
from open_climate_service.plugins.features.overture import overture, theme_types


def _building(x: float, y: float) -> Polygon:
    return Polygon([(x, y), (x + 0.5, y), (x + 0.5, y + 0.5), (x, y + 0.5)])


def _overture_like(tmp_path: Path, ids: list[Any], name: str = "src", subtypes: list[str] | None = None) -> pa.Table:
    """A table shaped like the client's output: a bbox struct and a declared covering."""
    path = tmp_path / f"{name}.parquet"
    gpd.GeoDataFrame(
        {
            "id": ids,
            "height": [3.0] * len(ids),
            "class": ["house"] * len(ids),
            "subtype": subtypes or ["county"] * len(ids),
        },
        geometry=[_building(i * 5.0, i * 5.0) for i in range(len(ids))],
        crs="EPSG:4326",
    ).to_parquet(path, write_covering_bbox=True)
    return pq.read_table(path)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    """Stub `overturemaps.core.record_batch_reader`, recording how it was called."""
    import overturemaps.core as core

    calls: list[dict[str, Any]] = []
    state: dict[str, Any] = {"table": _overture_like(tmp_path, ["a", "b", "c"])}

    def fake(overture_type: str, **kwargs: Any) -> Any:
        calls.append({"type": overture_type, **kwargs})
        table = state["table"]
        if table is None:
            return None
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    monkeypatch.setattr(core, "record_batch_reader", fake)
    return type("Client", (), {"calls": calls, "state": state})


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


# --- the request -------------------------------------------------------------------------------


def test_the_window_and_release_reach_the_client(tmp_path: Path, client: Any) -> None:
    overture(path=tmp_path / "out.parquet", release="2026-08-19.0", bbox=[-13.5, 6.9, -10.1, 10.0])

    assert client.calls == [
        {"type": "building", "bbox": (-13.5, 6.9, -10.1, 10.0), "release": "2026-08-19.0", "stac": True}
    ]


def test_the_stac_catalogue_is_used_by_default(tmp_path: Path, client: Any) -> None:
    """Without it the client opens every partition in the theme: ~20 minutes against ~30 seconds."""
    overture(path=tmp_path / "out.parquet", release="r", bbox=[0.0, 0.0, 1.0, 1.0])
    assert client.calls[0]["stac"] is True


def test_the_release_becomes_the_version(tmp_path: Path, client: Any) -> None:
    _, version = overture(path=tmp_path / "out.parquet", release="2026-08-19.0", bbox=[0.0, 0.0, 1.0, 1.0])
    assert version == "2026-08-19.0"


@pytest.mark.parametrize("bad", [[1.0, 1.0, 0.0, 2.0], [0.0, 5.0, 2.0, 1.0], [0.0, 0.0, 1.0]])
def test_an_empty_or_malformed_bbox_is_refused(tmp_path: Path, client: Any, bad: list[float]) -> None:
    with pytest.raises(ValueError, match="bbox"):
        overture(path=tmp_path / "out.parquet", release="r", bbox=bad)


def test_an_unknown_theme_names_the_known_ones(tmp_path: Path, client: Any) -> None:
    with pytest.raises(ValueError, match="Unknown Overture theme 'roads'.*buildings"):
        overture(path=tmp_path / "out.parquet", release="r", bbox=[0.0, 0.0, 1.0, 1.0], theme="roads")


def test_every_known_theme_maps_to_a_type() -> None:
    assert theme_types()["buildings"] == "building"


def test_a_theme_with_no_data_in_the_release_is_refused(tmp_path: Path, client: Any) -> None:
    client.state["table"] = None
    with pytest.raises(ValueError, match="returned no data"):
        overture(path=tmp_path / "out.parquet", release="r", bbox=[0.0, 0.0, 1.0, 1.0])


# --- the written file --------------------------------------------------------------------------


def test_the_extract_keeps_a_working_covering_bbox(tmp_path: Path, client: Any) -> None:
    """Without a covering, a windowed read of the extract scans the whole file.

    Nothing recomputes it: carrying the client's `geo` metadata through means the extract is valid
    GeoParquet with a usable covering for free.
    """
    import json

    out = tmp_path / "out.parquet"
    overture(path=out, release="r", bbox=[-1.0, -1.0, 30.0, 30.0])

    geo = json.loads(pq.read_metadata(out).metadata[b"geo"])
    assert (geo["columns"][geo["primary_column"]].get("covering") or {}).get("bbox")
    assert len(gpd.read_parquet(out, bbox=(4, 4, 7, 7))) == 1


def test_selected_columns_keep_the_geometry_and_the_covering(tmp_path: Path, client: Any) -> None:
    """A country's buildings carry attributes nobody asked for; naming columns keeps the file small.

    Dropping geometry or bbox would invalidate the result, so both survive a selection regardless.
    """
    out = tmp_path / "out.parquet"
    overture(path=out, release="r", bbox=[-1.0, -1.0, 30.0, 30.0], columns=["id"])

    assert pq.read_schema(out).names == ["id", "geometry", "bbox"]
    assert len(gpd.read_parquet(out, bbox=(4, 4, 7, 7))) == 1, "the covering still prunes"


def test_an_unknown_column_names_what_is_missing(tmp_path: Path, client: Any) -> None:
    with pytest.raises(ValueError, match=r"no column\(s\): nope"):
        overture(path=tmp_path / "out.parquet", release="r", bbox=[0.0, 0.0, 1.0, 1.0], columns=["nope"])


def test_output_without_a_bbox_column_is_refused(tmp_path: Path, client: Any) -> None:
    """The extract would have no covering, so every windowed read of it would scan the whole file."""
    plain = tmp_path / "plain.parquet"
    gpd.GeoDataFrame({"id": ["a"]}, geometry=[_building(0, 0)], crs="EPSG:4326").to_parquet(plain)
    client.state["table"] = pq.read_table(plain)

    with pytest.raises(ValueError, match="has no 'bbox' column"):
        overture(path=tmp_path / "out.parquet", release="r", bbox=[0.0, 0.0, 1.0, 1.0])


# --- through the store -------------------------------------------------------------------------


def test_a_file_writing_provider_is_recorded_without_decoding_geometry(
    instance: Path, monkeypatch: pytest.MonkeyPatch, client: Any
) -> None:
    """The point of the Path form: the store records the file, it does not read it back in."""
    _declare(
        monkeypatch,
        {
            "id": "buildings",
            "provider": "overture",
            "license": "ODbL-1.0",
            "attribution": "© OpenStreetMap contributors, © Overture Maps Foundation",
            "params": {"release": "2026-08-19.0", "bbox": [-1.0, -1.0, 30.0, 30.0]},
        },
    )

    version = resolver.ensure_current("buildings")

    assert version == "2026-08-19.0"
    assert (instance / "features" / "buildings.parquet").is_file()
    sidecar = store.metadata("buildings")
    assert sidecar["provider"] == "overture"
    assert sidecar["feature_count"] == 3
    assert sidecar["id_property"] == "id", "Overture's own id is the identity, not a rewritten one"


def test_a_second_call_within_the_ttl_does_not_re_extract(
    instance: Path, monkeypatch: pytest.MonkeyPatch, client: Any
) -> None:
    _declare(
        monkeypatch,
        {"id": "buildings", "provider": "overture", "params": {"release": "r", "bbox": [-1.0, -1.0, 30.0, 30.0]}},
    )
    first = resolver.ensure_current("buildings")
    calls = len(client.calls)

    second = resolver.ensure_current("buildings")

    assert first == second
    assert len(client.calls) == calls, "a second call within the TTL must not reach the client"


def test_a_new_release_re_extracts_and_changes_the_version(
    instance: Path, monkeypatch: pytest.MonkeyPatch, client: Any
) -> None:
    """A release *is* a version, so changing it is a different question and must refetch."""
    box = [-1.0, -1.0, 30.0, 30.0]
    _declare(
        monkeypatch, {"id": "buildings", "provider": "overture", "params": {"release": "2026-07-22.0", "bbox": box}}
    )
    assert resolver.ensure_current("buildings") == "2026-07-22.0"

    _declare(
        monkeypatch, {"id": "buildings", "provider": "overture", "params": {"release": "2026-08-19.0", "bbox": box}}
    )
    assert resolver.ensure_current("buildings") == "2026-08-19.0"


def test_duplicate_ids_in_an_extract_are_refused(
    instance: Path, monkeypatch: pytest.MonkeyPatch, client: Any, tmp_path: Path
) -> None:
    """The identity contract applies to a file-writing provider too, read from the id column alone."""
    client.state["table"] = _overture_like(tmp_path, ["same", "same"], name="broken")
    _declare(
        monkeypatch,
        {"id": "buildings", "provider": "overture", "params": {"release": "r", "bbox": [-1.0, -1.0, 30.0, 30.0]}},
    )

    with pytest.raises(ValueError, match="sharing a 'id'.*same"):
        resolver.ensure_current("buildings")


def test_an_extract_will_not_overwrite_a_curated_collection(
    instance: Path, monkeypatch: pytest.MonkeyPatch, client: Any
) -> None:
    gpd.GeoDataFrame({"id": ["kept"]}, geometry=[_building(0, 0)], crs="EPSG:4326").to_parquet(
        instance / "features" / "buildings.parquet"
    )
    _declare(
        monkeypatch,
        {"id": "buildings", "provider": "overture", "params": {"release": "r", "bbox": [-1.0, -1.0, 30.0, 30.0]}},
    )

    with pytest.raises(ValueError, match="not maintained by a provider"):
        resolver.ensure_current("buildings")


# --- row filters -------------------------------------------------------------------------------


def test_a_filter_keeps_only_the_matching_rows(tmp_path: Path, client: Any) -> None:
    """A bbox window returns every level that overlaps -- for divisions, neighbourhoods to countries.

    Aggregating over a mixture of admin levels is rarely what anyone means, so a template says which
    one it wants.
    """
    client.state["table"] = _overture_like(
        tmp_path, ["a", "b", "c"], name="mixed", subtypes=["county", "country", "county"]
    )
    out = tmp_path / "out.parquet"

    overture(path=out, release="r", bbox=[-1.0, -1.0, 30.0, 30.0], filters={"subtype": "county"})

    assert list(gpd.read_parquet(out)["id"]) == ["a", "c"]


def test_a_filter_accepts_several_values(tmp_path: Path, client: Any) -> None:
    client.state["table"] = _overture_like(
        tmp_path, ["a", "b", "c"], name="mixed", subtypes=["county", "region", "locality"]
    )
    out = tmp_path / "out.parquet"

    overture(path=out, release="r", bbox=[-1.0, -1.0, 30.0, 30.0], filters={"subtype": ["county", "region"]})

    assert list(gpd.read_parquet(out)["id"]) == ["a", "b"]


def test_a_filter_may_name_a_column_the_output_drops(tmp_path: Path, client: Any) -> None:
    """Filtering happens before projection, so selecting columns cannot break a filter."""
    client.state["table"] = _overture_like(tmp_path, ["a", "b"], name="mixed", subtypes=["county", "country"])
    out = tmp_path / "out.parquet"

    overture(path=out, release="r", bbox=[-1.0, -1.0, 30.0, 30.0], columns=["id"], filters={"subtype": "county"})

    assert pq.read_schema(out).names == ["id", "geometry", "bbox"]
    assert list(gpd.read_parquet(out)["id"]) == ["a"]


def test_an_unknown_filter_column_is_refused(tmp_path: Path, client: Any) -> None:
    with pytest.raises(ValueError, match="no column\\(s\\) to filter on: nope"):
        overture(path=tmp_path / "out.parquet", release="r", bbox=[0.0, 0.0, 1.0, 1.0], filters={"nope": "x"})


def test_a_filter_matching_nothing_is_refused_rather_than_writing_an_empty_file(tmp_path: Path, client: Any) -> None:
    """An empty collection would be recorded as a real one and aggregate to nothing."""
    with pytest.raises(ValueError, match="returned no rows"):
        overture(
            path=tmp_path / "out.parquet",
            release="r",
            bbox=[-1.0, -1.0, 30.0, 30.0],
            filters={"subtype": "nowhere"},
        )
