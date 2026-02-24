from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.features import router as features_router


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(features_router)
    return TestClient(app)


def test_features_collections_list() -> None:
    client = create_client()

    response = client.get("/features")

    assert response.status_code == 200
    payload = response.json()
    ids = {collection["id"] for collection in payload["collections"]}
    assert {"dhis2-org-units", "aggregated-results"}.issubset(ids)


def test_features_org_units_items() -> None:
    client = create_client()

    response = client.get("/features/dhis2-org-units/items", params={"level": 2})

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["numberReturned"] >= 1


def test_features_org_units_items_from_dhis2(monkeypatch) -> None:
    client = create_client()

    monkeypatch.setattr(
        "eoapi.endpoints.features.fetch_org_units_from_dhis2",
        lambda level: [
            {
                "type": "Feature",
                "id": "ou-demo",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[10.0, 10.0], [11.0, 10.0], [11.0, 11.0], [10.0, 11.0], [10.0, 10.0]]],
                },
                "properties": {"name": "Demo", "level": level},
            }
        ],
    )

    response = client.get("/features/dhis2-org-units/items", params={"level": 2})

    assert response.status_code == 200
    payload = response.json()
    assert payload["numberReturned"] == 1
    assert payload["features"][0]["id"] == "ou-demo"
