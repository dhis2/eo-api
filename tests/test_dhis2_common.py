from datetime import datetime

import pytest

from eo_api.routers.ogcapi.plugins.providers import dhis2_common


def test_fetch_bbox_from_feature_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(dhis2_common, "create_client", lambda **_: FakeClient())
    monkeypatch.setattr(
        dhis2_common,
        "get_org_units_geojson",
        lambda *_args, **_kwargs: {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[10.0, 1.0], [12.0, 1.0], [12.0, 3.0], [10.0, 3.0], [10.0, 1.0]]],
                    },
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[8.0, -2.0], [9.0, -2.0], [9.0, -1.0], [8.0, -1.0], [8.0, -2.0]]],
                    },
                },
            ],
        },
    )

    assert dhis2_common.fetch_bbox() == [8.0, -2.0, 12.0, 3.0]


def test_fetch_org_units_parses_models(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(dhis2_common, "create_client", lambda **_: FakeClient())
    monkeypatch.setattr(
        dhis2_common,
        "list_organisation_units",
        lambda *_args, **_kwargs: [
            {
                "id": "OU_1",
                "name": "Org Unit 1",
                "level": 2,
                "openingDate": "2020-01-01T00:00:00.000",
                "geometry": {"type": "Point", "coordinates": [12.0, 9.0]},
            }
        ],
    )

    result = dhis2_common.fetch_org_units()
    assert len(result) == 1
    assert result[0].id == "OU_1"
    assert result[0].name == "Org Unit 1"
    assert result[0].level == 2


def test_get_single_org_unit_returns_feature(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(dhis2_common, "create_client", lambda **_: FakeClient())
    monkeypatch.setattr(
        dhis2_common,
        "get_organisation_unit",
        lambda *_args, **_kwargs: {
            "id": "OU_1",
            "name": "Org Unit 1",
            "code": "A1",
            "shortName": "OU1",
            "level": 2,
            "openingDate": datetime(2020, 1, 1).isoformat(),
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0.0, 0.0], [2.0, 0.0], [2.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
            },
        },
    )

    feature = dhis2_common.get_single_org_unit("OU_1")
    assert feature["type"] == "Feature"
    assert feature["id"] == "OU_1"
    assert feature["bbox"] == (0.0, 0.0, 2.0, 1.0)
    assert feature["properties"]["name"] == "Org Unit 1"
