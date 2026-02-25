from eo_api.schemas import HealthStatus, StatusMessage


def test_root_returns_200(client):
    response = client.get("/")
    assert response.status_code == 200


def test_root_returns_welcome_message(client):
    response = client.get("/")
    result = StatusMessage.model_validate(response.json())
    assert result.message == "Welcome to DHIS2 EO API"


def test_health_returns_200(client):
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_healthy_status(client):
    response = client.get("/health")
    result = HealthStatus.model_validate(response.json())
    assert result.status == "healthy"
