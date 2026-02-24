from eo_api.schemas import StatusMessage


def test_root_returns_200(client):
    response = client.get("/")
    assert response.status_code == 200


def test_root_returns_welcome_message(client):
    response = client.get("/")
    result = StatusMessage.model_validate(response.json())
    assert result.message == "Welcome to DHIS2 EO API"
