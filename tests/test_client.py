from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from open_climate_service.client import Client, _id_from_href, list_datasets, open_dataset


def _make_catalog(hrefs: list[str]) -> dict:
    return {
        "links": [{"rel": "child", "href": href, "title": href.split("/")[-1]} for href in hrefs]
        + [{"rel": "root", "href": "http://localhost/stac/catalog.json"}]
    }


def _make_collection(zarr_href: str) -> dict:
    return {
        "assets": {
            "zarr": {
                "href": zarr_href,
                "xarray:open_kwargs": {"consolidated": True},
            }
        }
    }


def _make_response(json_body: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


# ── _id_from_href ──────────────────────────────────────────────────────────────


def test_id_from_href_extracts_last_path_segment() -> None:
    assert _id_from_href("http://localhost/stac/collections/chirps3_daily_rwa") == "chirps3_daily_rwa"


def test_id_from_href_ignores_query_and_fragment() -> None:
    assert _id_from_href("http://localhost/stac/collections/ds?foo=bar#section") == "ds"


def test_id_from_href_strips_trailing_slash() -> None:
    assert _id_from_href("http://localhost/stac/collections/ds/") == "ds"


# ── module-level list_datasets ─────────────────────────────────────────────────


def test_list_datasets_returns_child_links() -> None:
    catalog = _make_catalog(["http://localhost/stac/collections/chirps3_precipitation_daily_rwa"])
    with patch("open_climate_service.client.httpx.get", return_value=_make_response(catalog)) as mock_get:
        result = list_datasets("http://localhost")

    mock_get.assert_called_once_with("http://localhost/stac/catalog.json", timeout=30)
    assert len(result) == 1
    assert result[0]["rel"] == "child"
    assert "chirps3" in result[0]["href"]


def test_list_datasets_returns_empty_for_no_children() -> None:
    catalog = {"links": [{"rel": "root", "href": "http://localhost/stac/catalog.json"}]}
    with patch("open_climate_service.client.httpx.get", return_value=_make_response(catalog)):
        result = list_datasets("http://localhost")

    assert result == []


def test_list_datasets_raises_on_http_error() -> None:
    with patch("open_climate_service.client.httpx.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )
        with pytest.raises(httpx.HTTPStatusError):
            list_datasets("http://localhost")


# ── module-level open_dataset ──────────────────────────────────────────────────


def test_open_dataset_fetches_collection_and_opens_zarr(tmp_path: Path) -> None:
    zarr_path = tmp_path / "test.zarr"
    ds = xr.Dataset(
        {"precip": (["time", "latitude", "longitude"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "time": pd.date_range("2024-01-01", periods=2, freq="D"),
            "latitude": [3.0, 2.0, 1.0],
            "longitude": [10.0, 11.0, 12.0],
        },
    )
    ds.to_zarr(str(zarr_path), mode="w", consolidated=True)

    collection = _make_collection(str(zarr_path))
    with patch("open_climate_service.client.httpx.get", return_value=_make_response(collection)):
        result = open_dataset("chirps3_precipitation_daily_rwa", base_url="http://localhost")

    try:
        assert "precip" in result.data_vars
        assert result.sizes["time"] == 2
        assert "latitude" in result.coords
        assert "longitude" in result.coords
    finally:
        result.close()


def test_open_dataset_raises_on_http_error() -> None:
    with patch("open_climate_service.client.httpx.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )
        with pytest.raises(httpx.HTTPStatusError):
            open_dataset("nonexistent", base_url="http://localhost")


def test_open_dataset_uses_default_base_url() -> None:
    collection = _make_collection("/dev/null")
    with patch("open_climate_service.client.httpx.get", return_value=_make_response(collection)) as mock_get:
        with patch("open_climate_service.client.xr.open_zarr", return_value=MagicMock()):
            open_dataset("any_dataset")

    mock_get.assert_called_once_with("http://127.0.0.1:8000/stac/collections/any_dataset", timeout=30)


def test_open_dataset_uses_env_var_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIMATE_SERVICE_BASE_URL", "http://env-host:9000")
    collection = _make_collection("/dev/null")
    with patch("open_climate_service.client.httpx.get", return_value=_make_response(collection)) as mock_get:
        with patch("open_climate_service.client.xr.open_zarr", return_value=MagicMock()):
            open_dataset("any_dataset")

    mock_get.assert_called_once_with("http://env-host:9000/stac/collections/any_dataset", timeout=30)


# ── Client class ───────────────────────────────────────────────────────────────


def _make_client(base_url: str = "http://localhost") -> tuple[Client, MagicMock]:
    """Return a Client with a mocked internal httpx.Client."""
    client = Client(base_url)
    mock_http = MagicMock()
    client._http = mock_http
    return client, mock_http


def test_client_catalog_uses_persistent_http_session() -> None:
    catalog = _make_catalog(["http://localhost/stac/collections/chirps3_precipitation_daily_rwa"])
    client, mock_http = _make_client()
    mock_http.get.return_value = _make_response(catalog)

    result = client.catalog()

    mock_http.get.assert_called_once_with("http://localhost/stac/catalog.json")
    assert len(result) == 1
    assert result[0]["rel"] == "child"
    assert result[0]["id"] == "chirps3_precipitation_daily_rwa"


def test_client_open_uses_persistent_http_session(tmp_path: Path) -> None:
    zarr_path = tmp_path / "test.zarr"
    ds = xr.Dataset(
        {"precip": (["time", "latitude", "longitude"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "time": pd.date_range("2024-01-01", periods=2, freq="D"),
            "latitude": [3.0, 2.0, 1.0],
            "longitude": [10.0, 11.0, 12.0],
        },
    )
    ds.to_zarr(str(zarr_path), mode="w", consolidated=True)

    client, mock_http = _make_client()
    mock_http.get.return_value = _make_response(_make_collection(str(zarr_path)))

    result = client.open("chirps3_precipitation_daily_rwa")
    try:
        assert "precip" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()

    mock_http.get.assert_called_once_with("http://localhost/stac/collections/chirps3_precipitation_daily_rwa")


def test_client_context_manager_closes_http_session() -> None:
    with Client("http://localhost") as client:
        mock_http = MagicMock()
        client._http = mock_http

    mock_http.close.assert_called_once()


def test_client_accepts_custom_timeout() -> None:
    with patch("open_climate_service.client.httpx.Client") as mock_cls:
        Client("http://localhost", timeout=60)
    mock_cls.assert_called_once_with(timeout=60)


def test_client_strips_trailing_slash() -> None:
    client, mock_http = _make_client("http://localhost/")
    mock_http.get.return_value = _make_response(_make_catalog(["http://localhost/stac/collections/ds"]))
    client.catalog()
    mock_http.get.assert_called_once_with("http://localhost/stac/catalog.json")
