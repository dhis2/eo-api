from fastapi.testclient import TestClient

from eo_api.schemas import HealthStatus, RootResponse


def test_root_returns_200(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200


def test_root_returns_welcome_message(client: TestClient) -> None:
    response = client.get("/")
    result = RootResponse.model_validate(response.json())
    assert result.message == "Welcome to DHIS2 EO API"


def test_root_returns_links(client: TestClient) -> None:
    response = client.get("/")
    result = RootResponse.model_validate(response.json())
    rels = [link.rel for link in result.links]
    assert "ogcapi" in rels
    assert "prefect" in rels
    assert "docs" in rels


def test_health_returns_200(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_healthy_status(client: TestClient) -> None:
    response = client.get("/health")
    result = HealthStatus.model_validate(response.json())
    assert result.status == "healthy"
