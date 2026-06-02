import pytest
from fastapi.testclient import TestClient

from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.system.schemas import HealthStatus


def test_root_returns_html_for_browser_request(client: TestClient) -> None:
    response = client.get("/", headers={"accept": "text/html,application/xhtml+xml,*/*;q=0.8"})
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Open Climate Service" in response.text


def test_root_html_shows_extent(client: TestClient) -> None:
    response = client.get("/", headers={"accept": "text/html"})
    assert "Sierra Leone" in response.text


def test_root_html_links_to_key_endpoints(client: TestClient) -> None:
    response = client.get("/", headers={"accept": "text/html"})
    assert "/docs" in response.text
    assert "/stac/catalog.json" in response.text


def test_root_f_json_returns_openeo_capabilities(client: TestClient) -> None:
    response = client.get("/?f=json")
    assert response.status_code == 200
    assert "application/json" in response.headers["content-type"]
    payload = response.json()
    assert payload["api_version"] == "1.2.0"
    assert "backend_version" in payload
    assert "endpoints" in payload


def test_root_accept_json_returns_openeo_capabilities(client: TestClient) -> None:
    response = client.get("/", headers={"accept": "application/json"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["api_version"] == "1.2.0"


def test_root_accept_json_and_html_equal_q_returns_json(client: TestClient) -> None:
    response = client.get("/", headers={"accept": "application/json, text/html"})
    assert response.status_code == 200
    assert "application/json" in response.headers["content-type"]


def test_root_accept_html_higher_q_returns_html(client: TestClient) -> None:
    response = client.get("/", headers={"accept": "application/json;q=0.9, text/html;q=1.0"})
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_root_json_contains_openeo_links(client: TestClient) -> None:
    response = client.get("/?f=json")
    payload = response.json()
    rels = [link["rel"] for link in payload["links"]]
    assert "self" in rels
    assert "data" in rels


def test_health_returns_200(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_healthy_status(client: TestClient) -> None:
    response = client.get("/health")
    result = HealthStatus.model_validate(response.json())
    assert result.status == "healthy"


def test_zarr_route_echoes_origin_for_browser_access(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "get_dataset_zarr_store_info_or_404",
        lambda _: {"kind": "ZarrListing", "dataset_id": "dataset-1", "path": ".", "entries": []},
    )

    response = client.get("/zarr/dataset-1", headers={"Origin": "https://inspect.geozarr.org"})

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://inspect.geozarr.org"


def test_zarr_route_does_not_allow_unconfigured_origin(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "get_dataset_zarr_store_info_or_404",
        lambda _: {"kind": "ZarrListing", "dataset_id": "dataset-1", "path": ".", "entries": []},
    )

    response = client.get("/zarr/dataset-1", headers={"Origin": "https://example.org"})

    assert response.status_code == 200
    assert "access-control-allow-private-network" not in response.headers


def test_zarr_route_allows_private_network_preflight(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "get_dataset_zarr_store_info_or_404",
        lambda _: {"kind": "ZarrListing", "dataset_id": "dataset-1", "path": ".", "entries": []},
    )

    response = client.options(
        "/zarr/dataset-1",
        headers={
            "Origin": "https://inspect.geozarr.org",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Private-Network": "true",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://inspect.geozarr.org"
    assert response.headers["access-control-allow-private-network"] == "true"


def test_zarr_route_preserves_existing_vary_values(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "get_dataset_zarr_store_info_or_404",
        lambda _: {"kind": "ZarrListing", "dataset_id": "dataset-1", "path": ".", "entries": []},
    )

    response = client.options(
        "/zarr/dataset-1",
        headers={
            "Origin": "https://inspect.geozarr.org",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "X-Test",
            "Access-Control-Request-Private-Network": "true",
        },
    )

    assert response.status_code == 200
    vary_values = {value.strip() for value in response.headers["vary"].split(",")}
    assert "Origin" in vary_values
