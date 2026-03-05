import json
from pathlib import Path

import pytest
from pygeoapi.process.base import ProcessorExecuteError

from eo_api.routers.ogcapi.plugins.processes.worldpop_dhis2_workflow import WorldPopDhis2WorkflowProcessor


def _load_geojson(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_worldpop_dhis2_workflow_with_provided_raster_files() -> None:
    root = Path(__file__).resolve().parents[1]
    features_geojson = _load_geojson(root / "tests" / "data" / "sierra_leone_districts.geojson")
    raster_path = str(root / "tests" / "data" / "sle_pop_2026_CN_1km_R2025A_UA_v1.tif")

    processor = WorldPopDhis2WorkflowProcessor({"name": "worldpop-dhis2-workflow"})
    mimetype, output = processor.execute(
        {
            "raster_files": [raster_path],
            "start_year": 2026,
            "end_year": 2026,
            "features_geojson": features_geojson,
            "data_element": "DATAELEMENT_UID",
            "reducer": "sum",
        }
    )

    assert mimetype == "application/json"
    assert output["status"] == "completed"
    assert len(output["files"]) == 1
    assert output["summary"]["sync"]["mode"] == "provided-raster-files"
    assert len(output["workflowTrace"]) >= 3
    assert output["workflowTrace"][0]["step"] == "feature_fetch"
    assert len(output["dataValueSet"]["dataValues"]) > 0
    first = output["dataValueSet"]["dataValues"][0]
    assert first["dataElement"] == "DATAELEMENT_UID"
    assert first["period"] == "2026"


def test_worldpop_dhis2_workflow_requires_scope_without_raster_files() -> None:
    processor = WorldPopDhis2WorkflowProcessor({"name": "worldpop-dhis2-workflow"})
    with pytest.raises(ProcessorExecuteError, match="exactly one of country_code or bbox"):
        processor.execute(
            {
                "start_year": 2026,
                "end_year": 2026,
                "features_geojson": {"type": "FeatureCollection", "features": []},
                "data_element": "DATAELEMENT_UID",
            }
        )


def test_worldpop_dhis2_workflow_accepts_org_unit_level_without_features_geojson(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    raster_path = str(root / "tests" / "data" / "sle_pop_2026_CN_1km_R2025A_UA_v1.tif")

    from eo_api.routers.ogcapi.plugins.processes import worldpop_dhis2_workflow as workflow_module

    monkeypatch.setattr(
        workflow_module,
        "resolve_features",
        lambda inputs: {
            "valid_features": [
                {
                    "orgUnit": "OU_1",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-13.0, 8.0], [-12.0, 8.0], [-12.0, 9.0], [-13.0, 9.0], [-13.0, 8.0]]],
                    },
                }
            ],
            "effective_bbox": (-13.0, 8.0, -12.0, 9.0),
        },
    )

    processor = WorldPopDhis2WorkflowProcessor({"name": "worldpop-dhis2-workflow"})
    _, output = processor.execute(
        {
            "raster_files": [raster_path],
            "start_year": 2026,
            "end_year": 2026,
            "org_unit_level": 3,
            "data_element": "DATAELEMENT_UID",
            "reducer": "sum",
        }
    )

    assert output["status"] == "completed"
    assert output["summary"]["features"]["feature_count"] == 1
    assert len(output["workflowTrace"]) >= 3
    assert len(output["dataValueSet"]["dataValues"]) > 0
