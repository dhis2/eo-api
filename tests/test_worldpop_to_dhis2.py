import json
from pathlib import Path

from eo_api.integrations.dhis2_datavalues import build_data_value_set
from eo_api.integrations.worldpop_to_dhis2 import build_worldpop_datavalueset


def _load_geojson(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_build_worldpop_datavalueset_returns_valid_shape() -> None:
    root = Path(__file__).resolve().parents[1]
    features_geojson = _load_geojson(root / "tests" / "data" / "sierra_leone_districts.geojson")
    raster_path = str(root / "tests" / "data" / "sle_pop_2026_CN_1km_R2025A_UA_v1.tif")

    result = build_worldpop_datavalueset(
        features_geojson=features_geojson,
        raster_path=raster_path,
        year=2026,
        data_element="DATAELEMENT_UID",
        reducer="sum",
    )

    assert "dataValueSet" in result
    assert "dataValues" in result["dataValueSet"]
    assert len(result["dataValueSet"]["dataValues"]) > 0
    first = result["dataValueSet"]["dataValues"][0]
    assert first["dataElement"] == "DATAELEMENT_UID"
    assert first["period"] == "2026"
    assert "orgUnit" in first
    assert "value" in first
    assert result["summary"]["row_count"] == len(result["dataValueSet"]["dataValues"])


def test_build_data_value_set_component() -> None:
    output = build_data_value_set(
        rows=[{"orgUnit": "OU_1", "period": "2026", "value": 123.456}],
        data_element="DATAELEMENT_UID",
    )

    assert output["dataValueSet"]["dataValues"][0]["dataElement"] == "DATAELEMENT_UID"
    assert output["dataValueSet"]["dataValues"][0]["orgUnit"] == "OU_1"
    assert output["dataValueSet"]["dataValues"][0]["period"] == "2026"
