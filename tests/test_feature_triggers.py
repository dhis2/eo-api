"""Referencing a declared feature set from a workflow trigger (CLIM-926).

The trigger layer's job is to turn `{"from_features": "districts"}` into something the process graph
can resolve, *without* putting geometry into the persisted job record. These cover both halves of
that: the rewrite produces a node the executor evaluates, and the job still records which version of
each set it ran against.
"""

from pathlib import Path
from typing import Any

import pytest
from shapely.geometry import Polygon

from open_climate_service.automation import service as automation_service
from open_climate_service.automation.config import AutomationConfig
from open_climate_service.features.config import FeatureTemplates
from open_climate_service.features.provider import feature_provider

_NORTH = Polygon([(0, 2), (4, 2), (4, 4), (0, 4)])
_SOUTH = Polygon([(0, 0), (4, 0), (4, 2), (0, 2)])


def _collection(ids: list[Any]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": ids[0], "properties": {}, "geometry": _NORTH.__geo_interface__},
            {"type": "Feature", "id": ids[1], "properties": {}, "geometry": _SOUTH.__geo_interface__},
        ],
    }


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


def _register(monkeypatch: pytest.MonkeyPatch, name: str, func: Any) -> None:
    decorated = feature_provider(name)(func)
    monkeypatch.setattr("open_climate_service.features.provider.registry", lambda: {name: decorated})


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
