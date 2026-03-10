import datetime as dt
from typing import Any

import pytest
from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.orchestration.registry import ComponentDescriptor, ComponentRegistry
from eo_api.routers.ogcapi.plugins.processes.generic_dhis2_workflow import GenericDhis2WorkflowProcessor


def _build_mock_registry() -> ComponentRegistry:
    registry = ComponentRegistry()
    registry.register(
        ComponentDescriptor(
            component_id="workflow.features",
            description="features",
            fn=lambda params, context: {
                "valid_features": [{"orgUnit": "OU_1", "geometry": {"type": "Polygon", "coordinates": []}}],
                "effective_bbox": [1.0, 2.0, 3.0, 4.0],
                "feature_collection": {"type": "FeatureCollection", "features": []},
            },
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.download",
            description="download",
            fn=lambda params, context: (
                {"files": ["/tmp/fake_2026.tif"], "source_temporal_resolution": "daily"}
                if params["dataset"] == "chirps3"
                else {
                    "files": ["/tmp/fake_2026.tif"],
                    "source_temporal_resolution": "yearly",
                }
            ),
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.temporal_aggregation",
            description="temporal",
            fn=lambda params, context: (
                {"rows": [{"orgUnit": "OU_1", "period": "202501", "value": 1.23}]}
                if params["dataset"] == "chirps3"
                else {
                    "files": params["files"],
                    "_step_control": {"action": "pass_through", "reason": "yearly"},
                }
            ),
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.spatial_aggregation",
            description="spatial",
            fn=lambda params, context: (
                {"rows": params["rows"]}
                if params["dataset"] == "chirps3"
                else {
                    "rows": [{"orgUnit": "OU_1", "period": "2026", "value": 123.0}],
                    "summary": {"years_processed": [2026], "yearly": [{"year": 2026, "row_count": 1}]},
                }
            ),
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.dhis2_payload_builder",
            description="payload",
            fn=lambda params, context: {
                "dataValueSet": {"dataValues": [{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}]},
                "table": {"columns": ["orgUnit", "period", "value"], "rows": [{"orgUnit": "OU_1"}]},
            },
        )
    )
    return registry


def test_generic_workflow_chirps3_branch(monkeypatch: Any) -> None:
    from eo_api.routers.ogcapi.plugins.processes import generic_dhis2_workflow as module

    monkeypatch.setattr(module, "build_default_component_registry", lambda: _build_mock_registry())
    processor = GenericDhis2WorkflowProcessor({"name": "generic-dhis2-workflow"})
    mimetype, output = processor.execute(
        {
            "dataset_type": "chirps3",
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
    assert output["summary"]["dataset_type"] == "chirps3"
    assert len(output["workflowTrace"]) == 5
    assert output["dataValueSet"]["dataValues"][0]["orgUnit"] == "OU_1"
    assert any(link["href"] == "/ogcapi/collections/generic-chirps3-source" for link in output["links"])


def test_generic_workflow_worldpop_branch(monkeypatch: Any) -> None:
    from eo_api.routers.ogcapi.plugins.processes import generic_dhis2_workflow as module

    monkeypatch.setattr(module, "build_default_component_registry", lambda: _build_mock_registry())
    processor = GenericDhis2WorkflowProcessor({"name": "generic-dhis2-workflow"})
    _, output = processor.execute(
        {
            "dataset_type": "worldpop",
            "start_year": 2026,
            "end_year": 2026,
            "country_code": "SLE",
            "features_geojson": {"type": "FeatureCollection", "features": []},
            "data_element": "DE_UID",
        }
    )

    assert output["status"] == "completed"
    assert output["summary"]["dataset_type"] == "worldpop"
    assert len(output["workflowTrace"]) == 5
    assert output["workflowTrace"][2]["status"] == "passed_through"
    assert any(link["href"] == "/ogcapi/collections/generic-worldpop-source" for link in output["links"])


def test_generic_workflow_invalid_dataset_type() -> None:
    processor = GenericDhis2WorkflowProcessor({"name": "generic-dhis2-workflow"})
    with pytest.raises(ProcessorExecuteError, match="dataset_type"):
        processor.execute(
            {
                "dataset_type": "era5",
                "features_geojson": {"type": "FeatureCollection", "features": []},
                "data_element": "DE_UID",
            }
        )
