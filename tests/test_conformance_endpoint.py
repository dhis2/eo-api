from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.conformance import CONFORMANCE_CLASSES
from eoapi.endpoints.conformance import router as conformance_router


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(conformance_router)
    return TestClient(app)


def test_conformance_endpoint_returns_classes() -> None:
    client = create_client()

    response = client.get("/conformance")

    assert response.status_code == 200
    payload = response.json()
    assert payload["conformsTo"] == CONFORMANCE_CLASSES
    assert any(link["rel"] == "self" for link in payload["links"])
    assert any(link["rel"] == "root" for link in payload["links"])
