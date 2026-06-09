"""Tests for the built-in org-unit aggregation workflows and UDP parameter substitution."""

from __future__ import annotations

import csv
from io import StringIO
from typing import Any

import numpy as np
import pytest
import xarray as xr
from openeo_pg_parser_networkx.process_registry import Process

from open_climate_service.openeo import jobs
from open_climate_service.openeo import workflows as workflow_store
from open_climate_service.openeo.execution import (
    _augment_with_workflows,
    _build_process_registry,
    _resolve_workflow_parameters,
)

# ---------------------------------------------------------------------------
# _resolve_workflow_parameters
# ---------------------------------------------------------------------------


def test_resolve_workflow_parameters_substitutes_declared_refs() -> None:
    pg = {
        "load": {"process_id": "load_collection", "arguments": {"id": {"from_parameter": "dataset_id"}}},
        "save": {
            "process_id": "save_result",
            "arguments": {"options": {"period_type": {"from_parameter": "period_type"}}},
        },
    }
    out = _resolve_workflow_parameters(pg, {"dataset_id": "ds1", "period_type": "month"})
    assert out["load"]["arguments"]["id"] == "ds1"
    assert out["save"]["arguments"]["options"]["period_type"] == "month"


def test_resolve_workflow_parameters_leaves_callback_refs_untouched() -> None:
    # A reducer's `data` callback parameter is not a workflow parameter and must be left
    # for the engine to resolve at call time.
    pg = {"mean": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}}}
    out = _resolve_workflow_parameters(pg, {"dataset_id": "ds1"})
    assert out["mean"]["arguments"]["data"] == {"from_parameter": "data"}


# ---------------------------------------------------------------------------
# End-to-end workflow execution (UDP path) with a mocked load_collection
# ---------------------------------------------------------------------------


def _overlay_with_mock_dataset() -> Any:
    ds = xr.Dataset(
        {"tp": (("t", "y", "x"), np.arange(2 * 4 * 5, dtype="float32").reshape(2, 4, 5))},
        coords={
            "t": np.array(["2025-01-01", "2025-02-01"], dtype="datetime64[ns]"),
            "y": [1.0, 2.0, 3.0, 4.0],
            "x": [1.0, 2.0, 3.0, 4.0, 5.0],
        },
    )
    reg = _build_process_registry()
    reg["load_collection"] = Process(spec={}, implementation=lambda id=None, temporal_extent=None, **k: ds["tp"])
    return _augment_with_workflows(reg)


def _box(xmin: float, ymin: float, xmax: float, ymax: float) -> dict:
    return {
        "type": "Polygon",
        "coordinates": [[[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin]]],
    }


_GEOMETRIES = {
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "OU_A", "geometry": _box(0.5, 0.5, 2.5, 2.5)},
        {"type": "Feature", "id": "OU_B", "geometry": _box(3.5, 2.5, 5.5, 4.5)},
    ],
}


def test_both_workflows_registered_as_processes() -> None:
    ids = {wf.id for wf in workflow_store.list_workflows().processes}
    assert {"aggregate_to_dhis2_json", "aggregate_to_chap_csv"} <= ids


def test_aggregate_to_org_units_dhis2json() -> None:
    overlay = _overlay_with_mock_dataset()
    # period_type omitted -> declared default "month" applies
    envelope = overlay["aggregate_to_dhis2_json"].implementation(
        dataset_id="my_dataset",
        temporal_extent=["2025-01-01", "2025-02-28"],
        geometries=_GEOMETRIES,
        data_element_id="DE_TEMP",
    )
    assert envelope.format == "DHIS2JSON"
    payload = jobs._build_dhis2_json_payload(envelope.data.to_dataframe().reset_index(), envelope.options)
    dv = payload["dataValues"]
    assert {d["orgUnit"] for d in dv} == {"OU_A", "OU_B"}
    assert {d["period"] for d in dv} == {"202501", "202502"}
    assert all(d["dataElement"] == "DE_TEMP" for d in dv)
    assert len(dv) == 4


def test_aggregate_to_org_units_chap_csv() -> None:
    overlay = _overlay_with_mock_dataset()
    envelope = overlay["aggregate_to_chap_csv"].implementation(
        dataset_id="my_dataset",
        temporal_extent=["2025-01-01", "2025-02-28"],
        geometries=_GEOMETRIES,
        period_type="month",
    )
    assert envelope.format == "CHAPCSV"
    frame = jobs._build_chap_csv_frame(envelope.data.to_dataframe().reset_index(), envelope.options)
    rows = list(csv.DictReader(StringIO(frame.to_csv(index=False))))
    assert list(frame.columns) == ["time_period", "location", "tp"]
    assert {r["location"] for r in rows} == {"OU_A", "OU_B"}
    assert {r["time_period"] for r in rows} == {"202501", "202502"}


# OU_A covers pixels with t=0 values [0, 1, 5, 6] -> mean 3, sum 12, min 0, max 6.
@pytest.mark.parametrize(("method", "expected"), [("mean", "3"), ("sum", "12"), ("min", "0"), ("max", "6")])
def test_method_selects_reducer(method: str, expected: str) -> None:
    overlay = _overlay_with_mock_dataset()
    envelope = overlay["aggregate_to_dhis2_json"].implementation(
        dataset_id="my_dataset",
        temporal_extent=["2025-01-01", "2025-02-28"],
        geometries=_GEOMETRIES,
        data_element_id="DE",
        method=method,
    )
    dv = jobs._build_dhis2_json_payload(envelope.data.to_dataframe().reset_index(), envelope.options)["dataValues"]
    ou_a_jan = next(d["value"] for d in dv if d["orgUnit"] == "OU_A" and d["period"] == "202501")
    assert ou_a_jan == expected
