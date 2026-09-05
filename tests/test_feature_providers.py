"""Declared feature sets: providers, store updates and trigger references (CLIM-926)."""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Polygon

from open_climate_service.features import resolver, store
from open_climate_service.features.config import FeatureTemplates
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


def _declare(monkeypatch: pytest.MonkeyPatch, *templates: dict[str, Any]) -> None:
    """Stand in for the `features/*.yaml` templates a real instance would ship."""
    loaded = FeatureTemplates.model_validate({"templates": list(templates)})
    monkeypatch.setattr("open_climate_service.features.config.get_feature_templates", lambda: loaded)
    monkeypatch.setattr("open_climate_service.features.resolver.get_feature_templates", lambda: loaded)
    monkeypatch.setattr("open_climate_service.features.routes.get_feature_templates", lambda: loaded)


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


def test_duplicate_feature_ids_are_refused() -> None:
    with pytest.raises(ValueError, match="feature template ids must be unique"):
        FeatureTemplates.model_validate({"templates": [{"id": "d"}, {"id": "d"}]})


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


# --- through the graph executor --------------------------------------------------------------


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


# --- catalogue metadata --------------------------------------------------------------------------


def test_the_listing_carries_the_licence_and_provenance_a_template_declares(
    client: TestClient, instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A file knows its CRS and bounds; only a template knows its licence and what it is for."""
    gpd.GeoDataFrame({"ou_code": ["MW.N", "MW.S"]}, geometry=[_NORTH, _SOUTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    _declare(
        monkeypatch,
        {
            "id": "districts",
            "name": "Malawi districts",
            "license": "CC-BY-4.0",
            "attribution": "National Statistical Office of Malawi",
            "source_url": "https://example.org/districts",
        },
    )

    listed = client.get("/features").json()

    assert listed[0]["name"] == "Malawi districts"
    assert listed[0]["license"] == "CC-BY-4.0"
    assert listed[0]["attribution"] == "National Statistical Office of Malawi"
    assert listed[0]["feature_count"] == 2, "file-derived facts survive the merge"


def test_a_collection_with_no_template_is_still_listed(
    client: TestClient, instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An admin's file is a real collection; having authored nothing about it is not an error."""
    gpd.GeoDataFrame({"ou_code": ["MW.N", "MW.S"]}, geometry=[_NORTH, _SOUTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    _declare(monkeypatch)

    listed = client.get("/features").json()

    assert [info["id"] for info in listed] == ["districts"]
    assert "license" not in listed[0]


# --- providers that write their own file ---------------------------------------------------------


def _write_parquet(path: Path, ids: list[Any]) -> None:
    gpd.GeoDataFrame({"id": ids}, geometry=[_NORTH, _SOUTH][: len(ids)], crs="EPSG:4326").to_parquet(path)


def test_a_provider_may_write_the_file_itself(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A FeatureCollection is Python dicts; some sources are far too large for that.

    Such a provider is handed `path`, writes GeoParquet, and returns the path. The store records it
    without ever decoding the geometry.
    """

    def writer(path: Path, **_: Any) -> tuple[Path, str]:
        _write_parquet(path, ["MW.N", "MW.S"])
        return path, "release-7"

    _declare(monkeypatch, {"id": "big", "provider": "writes"})
    _register(monkeypatch, "writes", writer)

    version = resolver.ensure_current("big")

    assert version == "release-7"
    assert (instance / "features" / "big.parquet").is_file()
    assert store.metadata("big")["feature_count"] == 2
    assert [f["id"] for f in load_features("big")["features"]] == ["MW.N", "MW.S"]


def test_a_written_file_is_validated_by_its_id_column_alone(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The identity contract still applies — read from one narrow column, not from the geometry."""

    def writer(path: Path, **_: Any) -> Path:
        _write_parquet(path, ["same", "same"])
        return path

    _declare(monkeypatch, {"id": "big", "provider": "writes"})
    _register(monkeypatch, "writes", writer)

    with pytest.raises(ValueError, match="sharing a 'id'.*same"):
        resolver.ensure_current("big")


def test_a_written_file_with_no_id_column_is_refused(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def writer(path: Path, **_: Any) -> Path:
        gpd.GeoDataFrame({"name": ["a"]}, geometry=[_NORTH], crs="EPSG:4326").to_parquet(path)
        return path

    _declare(monkeypatch, {"id": "big", "provider": "writes"})
    _register(monkeypatch, "writes", writer)

    with pytest.raises(ValueError, match="no 'id' column and the template sets no id_property"):
        resolver.ensure_current("big")


def test_a_provider_claiming_to_write_but_not_writing_is_refused(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _declare(monkeypatch, {"id": "big", "provider": "writes"})
    _register(monkeypatch, "writes", lambda path, **_: path)

    with pytest.raises(ValueError, match="reported writing.*missing or empty"):
        resolver.ensure_current("big")


def test_ownership_is_checked_before_a_writing_provider_runs(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Checking afterwards compares the provider's own output against itself and always refuses.

    It also comes too late to matter: by then the curated file it would have protected is gone.
    """
    curated = instance / "features" / "big.parquet"
    _write_parquet(curated, ["kept"])
    original = curated.read_bytes()

    _declare(monkeypatch, {"id": "big", "provider": "writes"})
    _register(monkeypatch, "writes", lambda path, **_: (_write_parquet(path, ["MW.N"]), path)[1])

    with pytest.raises(ValueError, match="not maintained by a provider"):
        resolver.ensure_current("big")
    assert curated.read_bytes() == original, "the curated file must be untouched, not restored after"


def test_id_property_is_read_from_the_features_properties(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """GeoJSON keeps attributes under `properties`; only `id` is top-level.

    A lookup that checked the top level alone would find nothing and fall back to the feature's own
    id — keying the whole export on the wrong column while appearing to work. That is exactly the
    field a DHIS2 template points at its org-unit UID column.
    """
    collection = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": "f0", "properties": {"ou_code": "MW.N"}, "geometry": _NORTH.__geo_interface__},
            {"type": "Feature", "id": "f1", "properties": {"ou_code": "MW.S"}, "geometry": _SOUTH.__geo_interface__},
        ],
    }
    _declare(monkeypatch, {"id": "districts", "provider": "coded", "id_property": "ou_code"})
    _register(monkeypatch, "coded", lambda **_: collection)

    resolver.ensure_current("districts")

    assert [f["id"] for f in load_features("districts")["features"]] == ["MW.N", "MW.S"]


def test_a_missing_id_property_is_refused_rather_than_falling_back(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Silently using the feature's own id would hide the template being wrong."""
    collection = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": "f0", "properties": {"name": "Northern"}, "geometry": _NORTH.__geo_interface__},
            {"type": "Feature", "id": "f1", "properties": {"name": "Southern"}, "geometry": _SOUTH.__geo_interface__},
        ],
    }
    _declare(monkeypatch, {"id": "districts", "provider": "coded", "id_property": "ou_code"})
    _register(monkeypatch, "coded", lambda **_: collection)

    with pytest.raises(ValueError, match="with no 'ou_code'"):
        resolver.ensure_current("districts")


def test_a_template_with_no_provider_describes_a_curated_file(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The documented metadata-only case: nothing to fetch, nothing that can be stale."""
    gpd.GeoDataFrame({"ou_code": ["MW.N", "MW.S"]}, geometry=[_NORTH, _SOUTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    _declare(monkeypatch, {"id": "districts", "name": "Districts", "license": "CC-BY-4.0"})

    assert resolver.ensure_current("districts") == resolver.CURATED_VERSION
    assert len(load_features("districts")["features"]) == 2


# --- read-only instances -------------------------------------------------------------------------


def test_a_read_only_instance_does_not_fetch_or_write(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`POST /result` is allowed on a read-only instance, and a graph may contain `load_features`.

    Resolving one must not reach upstream or touch the store: that would be a write, and an outbound
    request, driven by a route whose whole point is that it changes nothing.
    """
    calls: list[Any] = []
    _declare(monkeypatch, {"id": "zones", "provider": "remote", "ttl_seconds": 0})
    _register(monkeypatch, "remote", lambda **_: calls.append(1) or _collection(["A", "B"]))
    monkeypatch.setattr("open_climate_service.config.is_read_only", lambda: True)

    with pytest.raises(ValueError, match="read-only"):
        resolver.ensure_current("zones")

    assert calls == [], "the provider must not be called"
    assert list((instance / "features").iterdir()) == [], "nothing may be written"


def test_a_read_only_instance_serves_a_stale_entry_rather_than_refusing(
    instance: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stale boundaries are far better than a failed job on an instance that cannot refresh them."""
    calls: list[Any] = []
    _declare(monkeypatch, {"id": "zones", "provider": "remote", "ttl_seconds": 0})
    _register(monkeypatch, "remote", lambda **_: calls.append(1) or _collection(["A", "B"]))
    first = resolver.ensure_current("zones")  # writable: populates the store

    monkeypatch.setattr("open_climate_service.config.is_read_only", lambda: True)
    served = resolver.ensure_current("zones")

    assert served == first
    assert len(calls) == 1, "the stale entry is served, not refetched"
    assert [f["id"] for f in load_features("zones")["features"]] == ["A", "B"]


# --- discovery from a real plugins_dir -----------------------------------------------------------


@pytest.fixture
def isolated_plugin_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """Forget any previously imported `features` package before and after a discovery test.

    Discovery imports an instance's providers as `features.<stem>` after putting `plugins_dir` on
    `sys.path` — the same scheme the `processes/` loader uses. That name is global and Python caches
    it, so the first `plugins_dir` a process sees is the only one it can ever see. One process
    serves one instance, so this never arises in production; it does arise across tests, and
    clearing it is what lets each test observe its own directory rather than the previous one's.
    """
    import sys

    def _forget() -> None:
        for name in [n for n in sys.modules if n == "features" or n.startswith("features.")]:
            del sys.modules[name]

    original_path = list(sys.path)
    _forget()
    yield
    _forget()
    # Discovery appends `plugins_dir` to sys.path and never removes it, so without this a later
    # test resolves `features` against an earlier test's directory even with the cache cleared.
    sys.path[:] = original_path


def _write_plugin_dir(root: Path, provider_name: str, template_id: str, module: str = "national") -> Path:
    """A plugins_dir holding a features/ folder with both halves, as a country would ship it.

    ``module`` differs per test on purpose. Discovery imports these as ``features.<stem>``, a global
    module name that Python caches for the life of the process — the same scheme the `processes/`
    loader uses. Reusing a stem across tests would resolve to whichever file was imported first.
    """
    features = root / "plugins" / "features"
    features.mkdir(parents=True, exist_ok=True)
    (features / "__init__.py").write_text("")
    (features / f"{module}.py").write_text(
        "from open_climate_service.features.provider import feature_provider\n\n\n"
        f'@feature_provider("{provider_name}")\n'
        "def national(**params):\n"
        '    return {"type": "FeatureCollection", "features": []}\n'
    )
    (features / f"{module}.yaml").write_text(
        f"- id: {template_id}\n  name: National districts\n  license: CC-BY-4.0\n  provider: {provider_name}\n"
    )
    return root


def test_a_features_folder_in_plugins_dir_is_discovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_plugin_import: None
) -> None:
    """CLIM-926's acceptance: both halves come from the same folder, discovered like the other three."""
    from open_climate_service.features.config import get_feature_templates
    from open_climate_service.features.provider import registry

    _write_plugin_dir(tmp_path, "national", "national-districts")
    monkeypatch.setattr("open_climate_service.config.get_config", lambda: {"plugins_dir": "./plugins/"})
    monkeypatch.setattr("open_climate_service.config.get_config_path", lambda: tmp_path / "climate-service.yaml")

    assert "national" in registry(), "the provider is not discovered"
    template = get_feature_templates().get("national-districts")
    assert template.provider == "national"
    assert template.license == "CC-BY-4.0", "authored metadata comes from the template file"


def test_plugins_dir_overrides_a_builtin_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_plugin_import: None
) -> None:
    """An instance replaces a shipped provider without forking, as it can for the other three."""
    from open_climate_service.features.provider import registry

    _write_plugin_dir(tmp_path, "stored", "anything", module="override")
    monkeypatch.setattr("open_climate_service.config.get_config", lambda: {"plugins_dir": "./plugins/"})
    monkeypatch.setattr("open_climate_service.config.get_config_path", lambda: tmp_path / "climate-service.yaml")

    found = registry()["stored"]

    assert found.__module__ == "features.override", f"plugins_dir must win, got {found.__module__}"


# --- concurrent refresh --------------------------------------------------------------------------


def test_a_refresh_never_exposes_a_half_written_collection(instance: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A scheduled refresh and a running job touch the same file at the same time.

    Writing in place let a reader open a partially written parquet -- "magic bytes not found in
    footer", which is a failed job and an unintelligible reason. Reproduced before the fix at 4
    failures in 36 operations. Staging and replacing means a reader sees the old file or the new
    one, never a partial one.
    """
    import threading
    import time

    from open_climate_service.shared import features as shared

    zones = 60
    payload = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": f"z{i}", "properties": {}, "geometry": _NORTH.__geo_interface__}
            for i in range(zones)
        ],
    }

    def slow(**_: Any) -> dict[str, Any]:
        time.sleep(0.01)  # a real provider is not instant
        return payload

    _declare(monkeypatch, {"id": "zones", "provider": "racy", "ttl_seconds": 0})
    _register(monkeypatch, "racy", slow)
    resolver.ensure_current("zones")

    failures: list[str] = []
    seen: list[int] = []

    def refresher() -> None:
        for _ in range(8):
            try:
                resolver.ensure_current("zones", refresh=True)
            except Exception as exc:  # noqa: BLE001 — the point is that none occur
                failures.append(f"refresh: {type(exc).__name__}")

    def reader() -> None:
        for _ in range(8):
            try:
                seen.append(len(shared.load_feature_collection("zones")["features"]))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"read: {type(exc).__name__}")

    threads = [threading.Thread(target=refresher) for _ in range(2)]
    threads += [threading.Thread(target=reader) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failures == [], f"a concurrent read or refresh failed: {sorted(set(failures))}"
    assert set(seen) == {zones}, f"a reader saw a partial collection: {sorted(set(seen))}"


def test_a_staged_file_is_not_mistaken_for_a_collection(instance: Path) -> None:
    """A crash mid-write leaves the staging file behind; the directory scan must not list it."""
    (instance / "features" / "districts.staging").write_bytes(b"half a parquet")
    gpd.GeoDataFrame({"id": ["a"]}, geometry=[_NORTH], crs="EPSG:4326").to_parquet(
        instance / "features" / "districts.parquet"
    )
    from open_climate_service.shared import features as shared

    assert [info["id"] for info in shared.list_collections()] == ["districts"]
