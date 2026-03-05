import datetime as dt
from typing import Any

from eo_api.routers.ogcapi.plugins.processes.chirps3_workflow import Chirps3WorkflowProcessor


def test_chirps3_workflow_uses_component_orchestration(monkeypatch: Any) -> None:
    from eo_api.routers.ogcapi.plugins.processes import chirps3_workflow as module

    monkeypatch.setattr(
        module,
        "resolve_features",
        lambda inputs: {
            "valid_features": [{"orgUnit": "OU_1", "geometry": {"type": "Polygon", "coordinates": []}}],
            "effective_bbox": (1.0, 2.0, 3.0, 4.0),
        },
    )
    monkeypatch.setattr(
        module,
        "download_chirps3",
        lambda **kwargs: {
            "files": ["/tmp/data/chirps3_cache/final_rnl_bbox/file_2025-01.nc"],
            "cache": {"hit": True, "key": "dummy", "downloaded_delta_count": 0, "reused_count": 1, "dir": "/tmp"},
        },
    )
    monkeypatch.setattr(
        module,
        "aggregate_chirps_rows",
        lambda **kwargs: {
            "rows": [{"orgUnit": "OU_1", "period": "202501", "value": 12.345}],
            "cache": {"key": "agg", "file": "/tmp/cache.csv", "cached_rows_reused": 0, "computed_rows_delta": 1},
        },
    )
    monkeypatch.setattr(
        module,
        "build_data_value_set",
        lambda **kwargs: {
            "dataValueSet": {
                "dataValues": [
                    {
                        "dataElement": "DE_UID",
                        "orgUnit": "OU_1",
                        "period": "202501",
                        "value": "12.345",
                    }
                ]
            },
            "table": {"columns": ["orgUnit", "period", "value"], "rows": [{"orgUnit": "OU_1", "period": "202501"}]},
        },
    )

    processor = Chirps3WorkflowProcessor({"name": "chirps3-dhis2-workflow"})
    mimetype, output = processor.execute(
        {
            "start_date": dt.date(2025, 1, 1),
            "end_date": dt.date(2025, 1, 31),
            "features_geojson": {"type": "FeatureCollection", "features": []},
            "data_element": "DE_UID",
            "stage": "final",
            "flavor": "rnl",
        }
    )

    assert mimetype == "application/json"
    assert output["status"] == "completed"
    assert output["summary"]["feature_count"] == 1
    assert output["summary"]["data_value_count"] == 1
    assert output["summary"]["feature_source"] == "inline_geojson"
    assert output["summary"]["effective_bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert output["summary"]["download_cache"]["key"] == "dummy"
    assert output["summary"]["aggregate_cache"]["key"] == "agg"
    assert output["summary"]["dry_run"] is True
    assert "import not performed" in output["message"]
    assert len(output["workflowTrace"]) == 4


def test_chirps3_workflow_selector_feature_source(monkeypatch: Any) -> None:
    from eo_api.routers.ogcapi.plugins.processes import chirps3_workflow as module

    monkeypatch.setattr(
        module,
        "resolve_features",
        lambda inputs: {
            "valid_features": [{"orgUnit": "OU_2", "geometry": {"type": "Polygon", "coordinates": []}}],
            "effective_bbox": (10.0, 20.0, 30.0, 40.0),
        },
    )
    monkeypatch.setattr(
        module,
        "download_chirps3",
        lambda **kwargs: {"files": ["/tmp/data/chirps.nc"], "cache": {"key": "k", "hit": True}},
    )
    monkeypatch.setattr(
        module,
        "aggregate_chirps_rows",
        lambda **kwargs: {"rows": [{"orgUnit": "OU_2", "period": "202501", "value": 1.2}], "cache": {"key": "a"}},
    )
    monkeypatch.setattr(
        module,
        "build_data_value_set",
        lambda **kwargs: {"dataValueSet": {"dataValues": [{"orgUnit": "OU_2"}]}, "table": {"columns": [], "rows": []}},
    )

    processor = Chirps3WorkflowProcessor({"name": "chirps3-dhis2-workflow"})
    _, output = processor.execute(
        {
            "start_date": dt.date(2025, 1, 1),
            "end_date": dt.date(2025, 1, 31),
            "org_unit_level": 3,
            "data_element": "DE_UID",
            "dry_run": False,
        }
    )

    assert output["summary"]["feature_source"] == "dhis2_selectors"
    assert output["summary"]["dry_run"] is False
