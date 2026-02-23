from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.collections import router as collections_router
from eoapi.endpoints.coverages import router as coverages_router
from eoapi.endpoints.edr import router as edr_router


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(collections_router)
    app.include_router(coverages_router)
    app.include_router(edr_router)
    return TestClient(app)


def test_edr_position_success() -> None:
    client = create_client()

    response = client.get(
        "/collections/chirps-daily/position",
        params={
            "coords": "POINT(30 -1)",
            "datetime": "2026-01-31T00:00:00Z",
            "parameter-name": "precip",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["features"][0]["geometry"]["type"] == "Point"
    assert payload["features"][0]["properties"]["parameters"] == ["precip"]


def test_edr_position_invalid_coords() -> None:
    client = create_client()

    response = client.get(
        "/collections/chirps-daily/position",
        params={"coords": "30,-1"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "InvalidParameterValue"


def test_edr_position_unknown_collection() -> None:
    client = create_client()

    response = client.get(
        "/collections/unknown/position",
        params={"coords": "POINT(30 -1)"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "NotFound"


def test_edr_area_success() -> None:
    client = create_client()

    response = client.get(
        "/collections/era5-land-daily/area",
        params={
            "bbox": "36,-2,38,0",
            "parameter-name": "2m_temperature",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["features"][0]["geometry"]["type"] == "Polygon"
    assert payload["features"][0]["properties"]["parameters"] == ["2m_temperature"]


def test_edr_area_invalid_bbox() -> None:
    client = create_client()

    response = client.get(
        "/collections/chirps-daily/area",
        params={"bbox": "30,-5,30,2"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "InvalidParameterValue"
