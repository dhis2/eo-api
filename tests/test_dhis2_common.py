from datetime import datetime

import pytest
from pygeofilter.parsers.ecql import parse

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
        "query_organisation_units",
        lambda *_args, **_kwargs: {
            "organisationUnits": [
                {
                    "id": "OU_1",
                    "name": "Org Unit 1",
                    "level": 2,
                    "openingDate": "2020-01-01T00:00:00.000",
                    "geometry": {"type": "Point", "coordinates": [12.0, 9.0]},
                }
            ]
        },
    )

    result = dhis2_common.fetch_org_units()
    assert len(result) == 1
    assert result[0].id == "OU_1"
    assert result[0].name == "Org Unit 1"
    assert result[0].level == 2


def test_extract_dhis2_query_options_accepts_native_params() -> None:
    fields, params, fetch_all = dhis2_common.extract_dhis2_query_options(
        properties=[
            ("dhis2_fields", "id,name,level"),
            ("dhis2_filter", "level:eq:3,name:ilike:K*"),
            ("dhis2_page", "2"),
            ("dhis2_pageSize", "50"),
            ("unknown", "ignored"),
        ],
        kwargs={},
    )

    assert fields == "id,name,level"
    assert params["filter"] == ["level:eq:3", "name:ilike:K*"]
    assert params["page"] == "2"
    assert params["pageSize"] == "50"
    assert "unknown" not in params
    assert fetch_all is False


def test_extract_dhis2_query_options_all_true_sets_paging_false() -> None:
    fields, params, fetch_all = dhis2_common.extract_dhis2_query_options(
        properties=[
            ("all", "true"),
            ("dhis2_filter", "level:eq:3"),
        ],
        kwargs={},
    )

    assert fields == dhis2_common.DHIS2_FIELDS
    assert params["filter"] == "level:eq:3"
    assert params["paging"] == "false"
    assert fetch_all is True


def test_extract_dhis2_query_options_enforces_id_in_dhis2_fields() -> None:
    fields, _params, _fetch_all = dhis2_common.extract_dhis2_query_options(
        properties=[
            ("dhis2_fields", "name,code,level,path"),
        ],
        kwargs={},
    )
    assert fields == "id,name,code,level,path"


def test_extract_dhis2_query_options_preserves_nested_dhis2_fields() -> None:
    fields, _params, _fetch_all = dhis2_common.extract_dhis2_query_options(
        properties=[
            ("dhis2_fields", "name,parent[id,name],level"),
        ],
        kwargs={},
    )
    assert fields == "id,name,parent[id,name],level"


def test_fields_from_select_properties() -> None:
    fields = dhis2_common.fields_from_select_properties(["name", "level"], skip_geometry=True)
    assert fields == "id,level,name"


def test_fields_from_select_properties_passthrough_unknown_field() -> None:
    fields = dhis2_common.fields_from_select_properties(["name", "displayName", "fooBar"], skip_geometry=True)
    assert fields == "displayName,fooBar,id,name"


def test_cql_to_dhis2_filters_supported_subset() -> None:
    filterq = parse("level = 3 AND code ILIKE 'VN-%'")
    translated = dhis2_common.cql_to_dhis2_filters(filterq)
    assert translated == ["level:eq:3", "code:ilike:VN-%"]


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
            "displayName": "Org Unit One",
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
    assert feature["properties"]["displayName"] == "Org Unit One"
