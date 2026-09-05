"""Declared feature sets: providers, cache snapshots and trigger references (CLIM-926)."""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from open_climate_service.automation import service as automation_service
from open_climate_service.automation.config import AutomationConfig
from open_climate_service.features import cache, resolver
from open_climate_service.features.config import FeatureDeclaration, get_features_config
from open_climate_service.features.provider import feature_provider, registry, resolve_provider
from open_climate_service.plugins.processes.load_features import load_features

_NORTH = Polygon([(0, 2), (4, 2), (4, 4), (0, 4)])
_SOUTH = Polygon([(0, 0), (4, 0), (4, 2), (0, 2)])


def _collection(ids: list[Any]) -> dict[str, Any]:
    """A two-feature FeatureCollection whose ids are whatever the test needs."""
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": ids[0], "properties": {"name": "Northern"}, "geometry": _NORTH.__geo_interface__},
            {"type": "Feature", "id": ids[1], "properties": {"name": "Southern"}, "geometry": _SOUTH.__geo_interface__},
        ],
    }


@pytest.fixture
def instance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the data root and the `features:` config at a temporary instance."""
    monkeypatch.setattr("open_climate_service.data_manager.services.downloader.DOWNLOAD_DIR", tmp_path / "downloads")
    monkeypatch.setattr("open_climate_service.config.get_data_root", lambda: tmp_path)
    (tmp_path / "features").mkdir(parents=True, exist_ok=True)
    return tmp_path


def _declare(monkeypatch: pytest.MonkeyPatch, *declarations: dict[str, Any]) -> None:
    monkeypatch.setattr("open_climate_service.config.get_config", lambda: {"features": list(declarations)})


def _register(monkeypatch: pytest.MonkeyPatch, name: str, func: Any) -> None:
    """Put one provider in the registry without going through plugin discovery."""
    decorated = feature_provider(name)(func)
    monkeypatch.setattr("open_climate_service.features.provider.registry", lambda: {name: decorated})


# --- providers -------------------------------------------------------------------------------


def test_the_stored_provider_is_discovered_as_a_builtin() -> None:
    """`features/` is scanned like the other plugin folders, so the shipped provider is found."""
    assert "stored" in registry()


def test_an_unknown_provider_names_the_available_ones() -> None:
    with pytest.raises(ValueError, match="Unknown feature provider 'nope'.*stored"):
        resolve_provider("nope")


def test_the_stored_provider_reads_the_instance_feature_store(instance: Path) -> None:
    """`stored` and `load_vector_cube` reach the same file through the same call."""
    gpd.GeoDataFrame({"ou_code": ["MW.N", "MW.S"]}, geometry=[_NORTH, _SOUTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    collection = resolve_provider("stored")(id="districts", id_property="ou_code")
    assert [feature["id"] for feature in collection["features"]] == ["MW.N", "MW.S"]


# --- declarations ----------------------------------------------------------------------------


def test_duplicate_feature_ids_are_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "d", "provider": "stored"}, {"id": "d", "provider": "stored"})
    with pytest.raises(ValueError, match="feature ids must be unique"):
        get_features_config()


def test_an_undeclared_id_names_what_is_declared(monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "stored"})
    with pytest.raises(ValueError, match="Unknown feature id 'nope'.*districts"):
        resolver.declaration("nope")


# --- caching and snapshots -------------------------------------------------------------------


def test_a_resolved_set_is_cached_and_the_provider_called_once(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def provider(**params: Any) -> dict[str, Any]:
        calls.append(params)
        return _collection(["MW.N", "MW.S"])

    _declare(monkeypatch, {"id": "districts", "provider": "counting", "params": {"level": 2}})
    _register(monkeypatch, "counting", provider)

    first = resolver.current_snapshot("districts")
    second = resolver.current_snapshot("districts")

    assert first == second
    assert len(calls) == 1, "a second call within the TTL must not refetch"
    assert calls[0] == {"level": 2}


def test_changing_params_invalidates_the_cache(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`level: 2` and `level: 3` are different questions and must not share an answer."""
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))

    _declare(monkeypatch, {"id": "districts", "provider": "counting", "params": {"level": 2}})
    level_2 = resolver.current_snapshot("districts")
    _declare(monkeypatch, {"id": "districts", "provider": "counting", "params": {"level": 3}})
    level_3 = resolver.current_snapshot("districts")

    assert level_2 != level_3


def test_an_expired_ttl_refetches(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    _register(monkeypatch, "counting", lambda **_: calls.append(1) or _collection(["MW.N", "MW.S"]))
    _declare(monkeypatch, {"id": "districts", "provider": "counting", "ttl_seconds": 0})

    resolver.current_snapshot("districts")
    resolver.current_snapshot("districts")

    assert len(calls) == 2


def test_a_pinned_snapshot_is_read_back_not_refetched(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A recorded snapshot must return the boundaries that run saw, not today's."""
    _declare(monkeypatch, {"id": "districts", "provider": "shifting", "ttl_seconds": 0})
    _register(monkeypatch, "shifting", lambda **_: _collection(["MW.N", "MW.S"]))
    pinned = resolver.current_snapshot("districts")

    # The upstream hierarchy changes: one district is renamed.
    _register(monkeypatch, "shifting", lambda **_: _collection(["MW.N", "MW.CENTRAL"]))

    assert [f["id"] for f in load_features("districts", snapshot=pinned)["features"]] == ["MW.N", "MW.S"]
    assert [f["id"] for f in load_features("districts")["features"]] == ["MW.N", "MW.CENTRAL"]


def test_a_dropped_snapshot_fails_clearly(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))
    snapshot = resolver.current_snapshot("districts")
    (instance / "cache" / "features" / "districts" / f"{snapshot}.parquet").unlink()

    with pytest.raises(ValueError, match="no longer cached"):
        load_features("districts", snapshot=snapshot)


@pytest.mark.parametrize("bad_snapshot", ["../../features/districts", "nope", "", "deadbeef1234-not-a-time"])
def test_a_snapshot_id_cannot_address_an_arbitrary_path(instance: Path, bad_snapshot: str) -> None:
    """The id arrives from a persisted process graph, so it is validated rather than trusted."""
    assert cache.snapshot_path("districts", bad_snapshot) is None


def test_eviction_keeps_the_newest_snapshots(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "counting", "ttl_seconds": 0})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))
    declared = FeatureDeclaration(id="districts", provider="counting", ttl_seconds=0)
    for index in range(4):
        cache.write(declared, _collection([f"A{index}", f"B{index}"]))

    removed = cache.evict("districts", keep=2)

    assert len(removed) == 2
    assert len(list((instance / "cache" / "features" / "districts").glob("*.parquet"))) == 2


# --- the identity contract, applied to providers ----------------------------------------------


def test_a_provider_returning_duplicate_ids_is_refused(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The same rule that guards a stored collection guards a provider's output."""
    _declare(monkeypatch, {"id": "districts", "provider": "broken"})
    _register(monkeypatch, "broken", lambda **_: _collection(["MW.N", "MW.N"]))

    with pytest.raises(ValueError, match="sharing a 'id'.*MW.N"):
        resolver.current_snapshot("districts")


def test_a_provider_returning_a_null_id_is_refused(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "broken"})
    _register(monkeypatch, "broken", lambda **_: _collection(["MW.N", None]))

    with pytest.raises(ValueError, match="1 feature.* with no 'id'"):
        resolver.current_snapshot("districts")


def test_a_provider_returning_something_other_than_a_collection_is_refused(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "broken"})
    _register(monkeypatch, "broken", lambda **_: {"type": "Feature"})

    with pytest.raises(ValueError, match="must return a GeoJSON FeatureCollection"):
        resolver.current_snapshot("districts")


# --- trigger references ------------------------------------------------------------------------


def test_a_trigger_reference_becomes_a_node_not_geometry(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole point: the persisted process graph names a snapshot, it does not carry polygons."""
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))

    resolved = automation_service._resolve_feature_references(
        {"dataset_id": "chirps", "geometries": {"from_features": "districts"}}
    )

    assert resolved["dataset_id"] == "chirps"
    assert resolved["geometries"]["process_id"] == "load_features"
    assert resolved["geometries"]["arguments"]["id"] == "districts"
    assert resolved["geometries"]["arguments"]["snapshot"]
    assert "coordinates" not in str(resolved), "geometry must not reach the job record"


def test_the_rewritten_node_stays_small(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A level-3 hierarchy is megabytes; the record that replaces it must not grow with it."""
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))

    resolved = automation_service._resolve_feature_references({"geometries": {"from_features": "districts"}})

    assert len(str(resolved)) < 200


def test_a_literal_argument_is_left_alone(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    inline = {"type": "FeatureCollection", "features": []}
    assert automation_service._resolve_feature_references({"geometries": inline}) == {"geometries": inline}
    assert automation_service._resolve_feature_references({"from_features": 3}) == {"from_features": 3}


def test_a_trigger_referencing_an_undeclared_feature_fails_at_startup(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A typo in a schedule should be a boot error, not a job that fails at 3am."""
    _declare(monkeypatch, {"id": "districts", "provider": "stored"})
    config = AutomationConfig.model_validate(
        {
            "workflow_triggers": [
                {
                    "id": "t",
                    "on_update_of": "chirps",
                    "workflow_id": "w",
                    "arguments": {"geometries": {"from_features": "distrcits"}},
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="references feature 'distrcits'.*Declared: districts"):
        automation_service._validate_feature_references(config)
