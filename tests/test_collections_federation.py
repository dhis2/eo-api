from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.collections import router as collections_router


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(collections_router)
    return TestClient(app)


def test_collections_includes_external_federated_entries(monkeypatch) -> None:
    client = create_client()

    monkeypatch.setattr(
        "eoapi.endpoints.collections.list_external_collections",
        lambda: [
            {
                "id": "ext:demo-provider:rainfall-collection",
                "title": "External Rainfall",
                "description": "Remote OGC collection",
                "keywords": ["external"],
                "extent": {
                    "spatial": {"bbox": [[-10.0, -10.0, 10.0, 10.0]]},
                    "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
                },
                "itemType": "coverage",
                "federation": {
                    "providerId": "demo-provider",
                    "providerUrl": "https://example-ogc.test",
                    "sourceCollectionId": "rainfall-collection",
                },
            }
        ],
    )

    response = client.get("/collections")

    assert response.status_code == 200
    payload = response.json()
    ids = {collection["id"] for collection in payload["collections"]}
    assert "chirps-daily" in ids
    assert "ext:demo-provider:rainfall-collection" in ids

    local = next(collection for collection in payload["collections"] if collection["id"] == "chirps-daily")
    rels = {link["rel"] for link in local["links"]}
    assert "process" in rels
    assert "process-execute" in rels


def test_get_external_collection_details(monkeypatch) -> None:
    client = create_client()

    monkeypatch.setattr(
        "eoapi.endpoints.collections.get_external_collection",
        lambda collection_id: {
            "id": collection_id,
            "title": "External Rainfall",
            "description": "Remote OGC collection",
            "extent": {
                "spatial": {"bbox": [[-10.0, -10.0, 10.0, 10.0]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
            "itemType": "coverage",
            "federation": {
                "providerId": "demo-provider",
                "providerUrl": "https://example-ogc.test",
                "sourceCollectionId": "rainfall-collection",
            },
        },
    )

    response = client.get("/collections/ext:demo-provider:rainfall-collection")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "ext:demo-provider:rainfall-collection"
    rels = {link["rel"] for link in payload["links"]}
    assert "source" in rels
