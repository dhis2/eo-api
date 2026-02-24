from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.collections import router as collections_router
from eoapi.endpoints.coverages import router as coverages_router


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(collections_router)
    app.include_router(coverages_router)
    return TestClient(app)


def test_external_coverage_proxy_success(monkeypatch) -> None:
    client = create_client()

    monkeypatch.setattr(
        "eoapi.endpoints.coverages.proxy_external_collection_request",
        lambda collection_id, operation, query_params: {
            "type": "Coverage",
            "title": "External coverage",
            "federation": {
                "providerId": "demo-provider",
                "sourceCollectionId": "rainfall-collection",
                "operation": operation,
            },
        },
    )

    response = client.get("/collections/ext:demo-provider:rainfall-collection/coverage", params={"f": "json"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "Coverage"
    assert payload["federation"]["operation"] == "coverage"


def test_external_coverage_proxy_not_found(monkeypatch) -> None:
    client = create_client()

    monkeypatch.setattr(
        "eoapi.endpoints.coverages.proxy_external_collection_request",
        lambda collection_id, operation, query_params: None,
    )

    response = client.get("/collections/ext:demo-provider:missing-collection/coverage")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "NotFound"


def test_external_coverage_proxy_disabled_operation(monkeypatch) -> None:
    client = create_client()

    monkeypatch.setattr(
        "eoapi.endpoints.coverages.is_external_operation_enabled",
        lambda collection_id, operation: False,
    )

    response = client.get("/collections/ext:demo-provider:rainfall-collection/coverage")

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "InvalidParameterValue"
