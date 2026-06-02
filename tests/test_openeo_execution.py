"""Tests for the openEO process graph execution layer."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from open_climate_service.openeo.execution import (
    _augment_with_udps,
    _bbox_to_dict,
    _RegistryOverlay,
    _temporal_to_list,
    run_process_graph,
)
from open_climate_service.openeo.jobs import OpenEOJobService, _result_assets
from open_climate_service.openeo.schemas import OpenEOJobCreate, OpenEOJobRecord, OpenEOJobStatus
from open_climate_service.shared.time import utc_now

# ---------------------------------------------------------------------------
# _bbox_to_dict
# ---------------------------------------------------------------------------


def test_bbox_to_dict_none_returns_none() -> None:
    assert _bbox_to_dict(None) is None


def test_bbox_to_dict_plain_dict_lowercases_keys() -> None:
    result = _bbox_to_dict({"West": 1.0, "East": 2.0, "South": 3.0, "North": 4.0})
    assert result == {"west": 1.0, "east": 2.0, "south": 3.0, "north": 4.0}


def test_bbox_to_dict_pydantic_object() -> None:
    bbox = MagicMock()
    bbox.west, bbox.south, bbox.east, bbox.north = 1.0, 2.0, 3.0, 4.0
    result = _bbox_to_dict(bbox)
    assert result == {"west": 1.0, "south": 2.0, "east": 3.0, "north": 4.0}


# ---------------------------------------------------------------------------
# _temporal_to_list
# ---------------------------------------------------------------------------


def test_temporal_to_list_none_returns_none() -> None:
    assert _temporal_to_list(None) is None


def test_temporal_to_list_plain_list_strips_tz() -> None:
    result = _temporal_to_list(["2020-01-01T00:00:00Z", "2023-01-01T00:00:00+00:00"])
    assert result == ["2020-01-01T00:00:00", "2023-01-01T00:00:00"]


def test_temporal_to_list_none_element_preserved() -> None:
    result = _temporal_to_list(["2020-01-01", None])
    assert result == ["2020-01-01", None]


def test_temporal_to_list_temporal_interval_object() -> None:
    from openeo_pg_parser_networkx.pg_schema import TemporalInterval

    ti = TemporalInterval.model_validate(["2020-01-01", "2023-06-15"])
    result = _temporal_to_list(ti)
    assert result is not None
    assert result[0] is not None and "2020-01-01" in result[0]
    assert result[1] is not None and "2023-06-15" in result[1]
    # No timezone suffix
    for v in result:
        if v is not None:
            assert "Z" not in v and "+00:00" not in v


# ---------------------------------------------------------------------------
# _RegistryOverlay
# ---------------------------------------------------------------------------


def _make_process(impl: Any) -> Any:
    from openeo_pg_parser_networkx.process_registry import Process

    return Process(spec={}, implementation=impl)


def test_registry_overlay_falls_back_to_base() -> None:
    base = {"add": _make_process(lambda x, y: x + y)}
    overlay = _RegistryOverlay(base, {})
    assert overlay["add"] is base["add"]


def test_registry_overlay_udp_shadows_base() -> None:
    udp_proc = _make_process(lambda: "udp")
    base = {"foo": _make_process(lambda: "base")}
    overlay = _RegistryOverlay(base, {"foo": udp_proc})
    assert overlay["foo"] is udp_proc


def test_registry_overlay_tuple_key_uses_name() -> None:
    proc = _make_process(lambda: None)
    base: dict[Any, Any] = {}
    overlay = _RegistryOverlay(base, {"bar": proc})
    assert overlay[("predefined", "bar")] is proc


# ---------------------------------------------------------------------------
# _augment_with_udps
# ---------------------------------------------------------------------------


def test_augment_with_udps_returns_base_when_no_udps(monkeypatch: pytest.MonkeyPatch) -> None:
    import open_climate_service.openeo.udps as udp_module

    monkeypatch.setattr(udp_module, "list_udps", lambda: MagicMock(processes=[]))
    base = object()
    result = _augment_with_udps(base)
    assert result is base


def test_augment_with_udps_registers_udp(monkeypatch: pytest.MonkeyPatch) -> None:
    import open_climate_service.openeo.udps as udp_module

    udp = MagicMock()
    udp.id = "my_udp"
    udp.process_graph = {
        "result": {"process_id": "save_result", "arguments": {"data": 42, "format": "Zarr"}, "result": True}
    }
    monkeypatch.setattr(udp_module, "list_udps", lambda: MagicMock(processes=[udp]))

    from openeo_pg_parser_networkx.process_registry import Process, ProcessRegistry

    base = ProcessRegistry()
    base["save_result"] = Process(spec={}, implementation=lambda data, **kw: data)

    overlay = _augment_with_udps(base)
    assert isinstance(overlay, _RegistryOverlay)
    assert "my_udp" in overlay._udps


# ---------------------------------------------------------------------------
# _persist_result — DataArray and GeoDataFrame handling
# ---------------------------------------------------------------------------


@pytest.fixture()
def job_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> OpenEOJobService:
    monkeypatch.setattr("open_climate_service.openeo.jobs._JOBS_DIR", tmp_path)
    return OpenEOJobService(max_workers=1)


def _sample_dataarray() -> xr.DataArray:
    return xr.DataArray(
        np.ones((3, 4, 5), dtype=np.float32),
        dims=["t", "y", "x"],
        name="temperature",
    )


def test_persist_result_dataarray_writes_zarr(job_service: OpenEOJobService, tmp_path: Path) -> None:
    da = _sample_dataarray()
    output_path = job_service._persist_result("job-1", da)

    assert output_path is not None
    assert output_path.endswith(".zarr")
    ds = xr.open_zarr(output_path)
    assert "temperature" in ds


def test_persist_result_dataset_writes_zarr(job_service: OpenEOJobService) -> None:
    ds = _sample_dataarray().to_dataset(name="ta")
    output_path = job_service._persist_result("job-2", ds)

    assert output_path is not None
    assert output_path.endswith(".zarr")


def test_persist_result_unsupported_type_raises(job_service: OpenEOJobService) -> None:
    with pytest.raises(TypeError, match="Unsupported result type"):
        job_service._persist_result("job-3", {"value": 42})


def test_persist_result_geodataframe_writes_geojson(job_service: OpenEOJobService) -> None:
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame({"value": [1.0, 2.0]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326")
    output_path = job_service._persist_result("job-4", gdf)

    assert output_path is not None
    assert output_path.endswith(".geojson")


def test_openeo_job_service_create_execute_and_get_results(
    job_service: OpenEOJobService, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "open_climate_service.openeo.execution.run_process_graph",
        lambda process: {"ignored": True},
    )
    monkeypatch.setattr(
        job_service,
        "_persist_result",
        lambda job_id, result: f"/tmp/{job_id}/result.geojson",
    )

    record = job_service.create_job(
        OpenEOJobCreate(
            process={"process_graph": {"result": {"process_id": "constant", "arguments": {"x": 1}, "result": True}}}
        )
    )

    job_service._execute(record.id)
    results = job_service.get_results(record.id)

    assert results.id == record.id
    assert results.assets["result"]["href"].endswith("result.geojson")


# ---------------------------------------------------------------------------
# _result_assets
# ---------------------------------------------------------------------------


def _record(output_path: str | None) -> OpenEOJobRecord:
    return OpenEOJobRecord(
        id="job-1",
        status=OpenEOJobStatus.FINISHED,
        created=utc_now(),
        updated=utc_now(),
        usage={"output_path": output_path} if output_path else {},
    )


def test_result_assets_zarr() -> None:
    assets = _result_assets(_record("/some/path/result.zarr"))
    assert assets["result"]["type"] == "application/x-zarr"
    assert assets["result"]["href"].endswith("result.zarr/")


def test_result_assets_geojson() -> None:
    assets = _result_assets(_record("/some/path/result.geojson"))
    assert assets["result"]["type"] == "application/geo+json"
    assert assets["result"]["href"].endswith("result.geojson")


def test_result_assets_none_output_returns_empty() -> None:
    assert _result_assets(_record(None)) == {}


def test_create_job_does_not_advertise_missing_logs_endpoint(client: TestClient) -> None:
    response = client.post(
        "/jobs",
        json={
            "process": {"process_graph": {"result": {"process_id": "constant", "arguments": {"x": 1}, "result": True}}}
        },
    )

    assert response.status_code == 201
    links = response.json()["links"]
    assert all(link["rel"] != "logs" for link in links)


def test_put_udp_rejects_predefined_process_id(client: TestClient) -> None:
    response = client.put(
        "/process_graphs/load_collection",
        json={"summary": "Bad override", "process_graph": {}},
    )

    assert response.status_code == 400
    assert "conflicts with a predefined process" in response.json()["detail"]


def test_run_process_graph_maps_invalid_graph_errors_to_400(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeGraph:
        def __init__(self, process_graph: dict[str, Any]) -> None:
            self.process_graph = process_graph

        def to_callable(self, registry: Any) -> Any:
            def _runner() -> Any:
                raise ValueError("unknown process id")

            return _runner

    monkeypatch.setattr("openeo_pg_parser_networkx.OpenEOProcessGraph", FakeGraph)

    with pytest.raises(Exception) as exc_info:
        run_process_graph({"process_graph": {"result": {"process_id": "missing", "result": True}}})

    exc = exc_info.value
    assert getattr(exc, "status_code", None) == 400
    assert "Invalid process graph" in str(getattr(exc, "detail", exc))


def test_run_process_graph_keeps_runtime_failures_as_500(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeGraph:
        def __init__(self, process_graph: dict[str, Any]) -> None:
            self.process_graph = process_graph

        def to_callable(self, registry: Any) -> Any:
            def _runner() -> Any:
                raise RuntimeError("boom")

            return _runner

    monkeypatch.setattr("openeo_pg_parser_networkx.OpenEOProcessGraph", FakeGraph)

    with pytest.raises(Exception) as exc_info:
        run_process_graph({"process_graph": {"result": {"process_id": "add", "result": True}}})

    exc = exc_info.value
    assert getattr(exc, "status_code", None) == 500
    assert "Process graph execution failed" in str(getattr(exc, "detail", exc))


def test_result_route_rejects_synchronous_zarr_datacube(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "open_climate_service.openeo.execution.run_process_graph",
        lambda process, request=None: xr.Dataset(
            {"temperature": xr.DataArray(np.ones((2, 2), dtype=np.float32), dims=["y", "x"])}
        ),
    )

    response = client.post(
        "/result",
        json={"process_graph": {"result": {"process_id": "load_collection", "result": True}}},
    )

    assert response.status_code == 400
    assert "do not support ZARR output" in response.json()["detail"]
