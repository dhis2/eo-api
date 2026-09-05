"""Declared feature sets: providers, store updates and trigger references (CLIM-926)."""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from open_climate_service.automation import service as automation_service
from open_climate_service.automation.config import AutomationConfig
from open_climate_service.features import resolver, store
from open_climate_service.features.config import get_features_config
from open_climate_service.features.provider import feature_provider, registry, resolve_provider
from open_climate_service.plugins.processes.load_features import load_features
from open_climate_service.plugins.processes.load_vector_cube import load_vector_cube

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


# --- store updates and freshness -------------------------------------------------------------------


def test_a_resolved_set_is_cached_and_the_provider_called_once(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def provider(**params: Any) -> dict[str, Any]:
        calls.append(params)
        return _collection(["MW.N", "MW.S"])

    _declare(monkeypatch, {"id": "districts", "provider": "counting", "params": {"level": 2}})
    _register(monkeypatch, "counting", provider)

    first = resolver.ensure_current("districts")
    second = resolver.ensure_current("districts")

    assert first == second
    assert len(calls) == 1, "a second call within the TTL must not refetch"
    assert calls[0] == {"level": 2}


def test_changing_params_refetches(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`level: 2` and `level: 3` are different questions and must not share an answer."""
    calls: list[Any] = []
    _register(monkeypatch, "counting", lambda **params: calls.append(params) or _collection(["MW.N", "MW.S"]))

    _declare(monkeypatch, {"id": "districts", "provider": "counting", "params": {"level": 2}})
    level_2 = resolver.ensure_current("districts")
    _declare(monkeypatch, {"id": "districts", "provider": "counting", "params": {"level": 3}})
    level_3 = resolver.ensure_current("districts")

    assert calls == [{"level": 2}, {"level": 3}], "a params change must reach the provider"
    assert level_2 != level_3, "the recorded version must distinguish what was fetched"


def test_an_expired_ttl_refetches(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    _register(monkeypatch, "counting", lambda **_: calls.append(1) or _collection(["MW.N", "MW.S"]))
    _declare(monkeypatch, {"id": "districts", "provider": "counting", "ttl_seconds": 0})

    resolver.ensure_current("districts")
    resolver.ensure_current("districts")

    assert len(calls) == 2


def test_a_refresh_updates_the_entry_in_place(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One store, one file per set: a refresh replaces the entry rather than adding beside it."""
    _declare(monkeypatch, {"id": "districts", "provider": "shifting", "ttl_seconds": 0})
    _register(monkeypatch, "shifting", lambda **_: _collection(["MW.N", "MW.S"]))
    resolver.ensure_current("districts")

    # The upstream hierarchy changes: one district is renamed.
    _register(monkeypatch, "shifting", lambda **_: _collection(["MW.N", "MW.CENTRAL"]))

    assert [f["id"] for f in load_features("districts")["features"]] == ["MW.N", "MW.CENTRAL"]
    assert len(list((instance / "features").glob("*.parquet"))) == 1


def test_the_sidecar_makes_the_store_self_describing(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A reader knowing only the id still gets the right ids — including `load_vector_cube`."""
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))
    resolver.ensure_current("districts")

    assert [f["id"] for f in load_vector_cube("districts")["features"]] == ["MW.N", "MW.S"]
    assert store.metadata("districts")["provider"] == "counting"


def test_a_stored_declaration_reads_through_without_rewriting_the_store(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A `stored` set is an alias for a file already in the store, not an ingestion into it.

    Writing its result back would have the entry rewrite itself on every refresh, and would trip the
    ownership guard on the most natural config — declaration id equal to the collection id.
    """
    gpd.GeoDataFrame({"ou_code": ["MW.N", "MW.S"]}, geometry=[_NORTH, _SOUTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    _declare(
        monkeypatch,
        {"id": "districts", "provider": "stored", "params": {"id": "districts", "id_property": "ou_code"}},
    )

    assert resolver.ensure_current("districts") == resolver.LIVE_VERSION
    assert [f["id"] for f in load_features("districts")["features"]] == ["MW.N", "MW.S"]
    assert store.metadata("districts") == {}, "a read-through must not claim ownership of the file"


def test_a_provider_will_not_overwrite_a_curated_collection(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An admin's file has no sidecar, so nothing claims ownership of it — refuse rather than clobber."""
    gpd.GeoDataFrame({"ou_code": ["MW.N", "MW.S"]}, geometry=[_NORTH, _SOUTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["A", "B"]))

    with pytest.raises(ValueError, match="not maintained by a provider"):
        resolver.ensure_current("districts")


def test_a_provider_will_not_overwrite_another_providers_collection(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "first"})
    _register(monkeypatch, "first", lambda **_: _collection(["MW.N", "MW.S"]))
    resolver.ensure_current("districts")

    _declare(monkeypatch, {"id": "districts", "provider": "second"})
    _register(monkeypatch, "second", lambda **_: _collection(["A", "B"]))

    with pytest.raises(ValueError, match="maintained by provider 'first'"):
        resolver.ensure_current("districts")


# --- the identity contract, applied to providers ----------------------------------------------


def test_a_provider_returning_duplicate_ids_is_refused(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The same rule that guards a stored collection guards a provider's output."""
    _declare(monkeypatch, {"id": "districts", "provider": "broken"})
    _register(monkeypatch, "broken", lambda **_: _collection(["MW.N", "MW.N"]))

    with pytest.raises(ValueError, match="sharing a 'id'.*MW.N"):
        resolver.ensure_current("districts")


def test_a_provider_returning_a_null_id_is_refused(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "broken"})
    _register(monkeypatch, "broken", lambda **_: _collection(["MW.N", None]))

    with pytest.raises(ValueError, match="1 feature.* with no 'id'"):
        resolver.ensure_current("districts")


def test_a_provider_returning_something_other_than_a_collection_is_refused(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _declare(monkeypatch, {"id": "districts", "provider": "broken"})
    _register(monkeypatch, "broken", lambda **_: {"type": "Feature"})

    with pytest.raises(ValueError, match="must return a GeoJSON FeatureCollection"):
        resolver.ensure_current("districts")


# --- trigger references ------------------------------------------------------------------------


def test_a_trigger_reference_becomes_a_node_not_geometry(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole point: the persisted process graph names the set, it does not carry polygons."""
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))

    nodes: dict[str, Any] = {}
    resolved = automation_service._resolve_feature_references(
        {"dataset_id": "chirps", "geometries": {"from_features": "districts"}}, nodes
    )

    assert resolved["dataset_id"] == "chirps"
    assert resolved["geometries"] == {"from_node": "features_districts"}
    assert nodes == {"features_districts": {"process_id": "load_features", "arguments": {"id": "districts"}}}
    assert "coordinates" not in str(resolved) + str(nodes), "geometry must not reach the job record"


def test_the_reference_is_a_node_the_executor_actually_resolves(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An inline `{"process_id": ...}` in an argument is *not* evaluated — it is passed through.

    The graph parser leaves such a dict untouched, so `aggregate_spatial` would receive it and try
    to read it as GeoJSON. Only a sibling node plus `from_node` becomes a `ResultReference` the
    executor resolves, so this asserts the wiring is that shape rather than the inline one.
    """
    from openeo_pg_parser_networkx import OpenEOProcessGraph
    from openeo_pg_parser_networkx.pg_schema import ResultReference

    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))

    nodes: dict[str, Any] = {}
    arguments = automation_service._resolve_feature_references({"geometries": {"from_features": "districts"}}, nodes)
    graph = {"process_graph": {**nodes, "workflow": {"process_id": "add", "arguments": arguments, "result": True}}}

    parsed = OpenEOProcessGraph(pg_data=graph)
    workflow = next(attrs for _, attrs in parsed.nodes if attrs["process_id"] == "add")

    assert isinstance(workflow["resolved_kwargs"]["geometries"], ResultReference)


def test_load_features_runs_through_the_graph_executor(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`load_features` is registered and callable by the real engine, not only as a Python function."""
    from open_climate_service.openeo.execution import run_process_graph

    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: _collection(["MW.N", "MW.S"]))

    result = run_process_graph(
        {"process_graph": {"feat": {"process_id": "load_features", "arguments": {"id": "districts"}, "result": True}}}
    )

    assert result["type"] == "FeatureCollection"
    assert [feature["id"] for feature in result["features"]] == ["MW.N", "MW.S"]


def test_the_job_records_which_feature_version_it_ran_against(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Explaining why one run covered 47 districts and the next covered 48 needs the version."""
    _declare(monkeypatch, {"id": "districts", "provider": "counting"})
    _register(monkeypatch, "counting", lambda **_: (_collection(["MW.N", "MW.S"]), "release-2026-09"))

    provenance = automation_service._feature_provenance({"geometries": {"from_features": "districts"}})

    assert provenance == " against features districts@release-2026-09"


def test_an_unreachable_provider_does_not_fail_the_submission(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A schedule that stops dispatching is worse than a job with no provenance line."""

    def unreachable(**_: Any) -> dict[str, Any]:
        raise ConnectionError("DHIS2 unreachable")

    _declare(monkeypatch, {"id": "districts", "provider": "flaky"})
    _register(monkeypatch, "flaky", unreachable)

    assert automation_service._feature_provenance({"geometries": {"from_features": "districts"}}) == (
        " against features districts@unresolved"
    )


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
