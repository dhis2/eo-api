"""Lightweight client for discovering and opening published Open Climate Service datasets."""

import os
from urllib.parse import urlparse

import httpx
import xarray as xr

_FALLBACK_BASE_URL = "http://127.0.0.1:8000"
_DEFAULT_TIMEOUT = 30.0


def _default_base_url() -> str:
    return os.environ.get("CLIMATE_SERVICE_BASE_URL", _FALLBACK_BASE_URL)


def _id_from_href(href: str) -> str:
    """Extract the dataset id from a STAC child href by reading the last URL path segment."""
    return urlparse(href).path.rstrip("/").rsplit("/", 1)[-1]


class Client:
    """Client for a Open Climate Service instance.

    Maintains a persistent HTTP connection for efficient repeated calls.
    Use as a context manager to ensure the connection is closed:

        with Client("http://localhost:8000") as client:
            datasets = client.catalog()

    Args:
        base_url: Base URL of the running Open Climate Service (e.g. ``http://localhost:8000``).
        timeout: HTTP request timeout in seconds (default: 30).
    """

    def __init__(self, base_url: str, *, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=timeout)

    def close(self) -> None:
        """Close the underlying HTTP connection."""
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def catalog(self) -> list[dict]:
        """Return all published datasets from the STAC catalog.

        Each entry is a STAC child link dict with at least ``id``, ``title``, and ``href``.
        """
        response = self._http.get(f"{self.base_url}/stac/catalog.json")
        response.raise_for_status()
        catalog = response.json()
        raw_links = catalog.get("links")
        if not isinstance(raw_links, list):
            raise ValueError(f"Invalid STAC catalog response from {self.base_url}: missing or non-list 'links' field")
        links = []
        for link in raw_links:
            if isinstance(link, dict) and link.get("rel") == "child":
                href = link.get("href")
                if not isinstance(href, str) or not href:
                    raise ValueError(f"STAC child link from {self.base_url} has a missing or invalid href")
                links.append({**link, "id": _id_from_href(href)})
        return links

    def open(self, dataset_id: str) -> xr.Dataset:
        """Open a published dataset as an xarray Dataset.

        Fetches the STAC collection for ``dataset_id``, reads the Zarr asset
        metadata, and returns the opened dataset. Coordinates are always
        ``time``, ``latitude``, and ``longitude``.
        """
        response = self._http.get(f"{self.base_url}/stac/collections/{dataset_id}")
        response.raise_for_status()
        collection = response.json()
        assets = collection.get("assets")
        if not isinstance(assets, dict):
            raise ValueError(
                f"STAC collection for '{dataset_id}' from {self.base_url} has a missing or invalid 'assets' field"
            )
        asset = assets.get("zarr")
        if not isinstance(asset, dict):
            raise ValueError(f"Dataset '{dataset_id}' has no Zarr asset in the STAC collection")
        href = asset.get("href")
        if not isinstance(href, str) or not href:
            raise ValueError(f"Zarr asset for '{dataset_id}' has a missing or invalid href")
        open_kwargs = asset.get("xarray:open_kwargs", {})
        if not isinstance(open_kwargs, dict):
            raise ValueError(f"Zarr asset for '{dataset_id}' has a malformed xarray:open_kwargs field")
        return xr.open_zarr(href, **open_kwargs)  # type: ignore[no-any-return]


def list_datasets(base_url: str | None = None) -> list[dict]:
    """Return all published datasets from the STAC catalog.

    Each entry is a STAC child link dict with at least ``id``, ``title``, and ``href``.
    ``base_url`` defaults to the ``CLIMATE_SERVICE_BASE_URL`` environment variable,
    falling back to ``http://127.0.0.1:8000``.
    """
    url = (base_url or _default_base_url()).rstrip("/")
    response = httpx.get(f"{url}/stac/catalog.json", timeout=_DEFAULT_TIMEOUT)
    response.raise_for_status()
    catalog = response.json()
    raw_links = catalog.get("links")
    if not isinstance(raw_links, list):
        raise ValueError(f"Invalid STAC catalog response from {url}: missing or non-list 'links' field")
    links = []
    for link in raw_links:
        if isinstance(link, dict) and link.get("rel") == "child":
            href = link.get("href")
            if not isinstance(href, str) or not href:
                raise ValueError(f"STAC child link from {url} has a missing or invalid href")
            links.append({**link, "id": _id_from_href(href)})
    return links


def open_dataset(dataset_id: str, *, base_url: str | None = None) -> xr.Dataset:
    """Open a published dataset as an xarray Dataset.

    Fetches the STAC collection for ``dataset_id``, reads the Zarr asset
    metadata, and returns the opened dataset. Coordinates are always
    ``time``, ``latitude``, and ``longitude``.
    ``base_url`` defaults to the ``CLIMATE_SERVICE_BASE_URL`` environment variable,
    falling back to ``http://127.0.0.1:8000``.
    """
    url = (base_url or _default_base_url()).rstrip("/")
    response = httpx.get(f"{url}/stac/collections/{dataset_id}", timeout=_DEFAULT_TIMEOUT)
    response.raise_for_status()
    collection = response.json()
    assets = collection.get("assets")
    if not isinstance(assets, dict):
        raise ValueError(f"STAC collection for '{dataset_id}' from {url} has a missing or invalid 'assets' field")
    asset = assets.get("zarr")
    if not isinstance(asset, dict):
        raise ValueError(f"Dataset '{dataset_id}' has no Zarr asset in the STAC collection")
    href = asset.get("href")
    if not isinstance(href, str) or not href:
        raise ValueError(f"Zarr asset for '{dataset_id}' has a missing or invalid href")
    open_kwargs = asset.get("xarray:open_kwargs", {})
    if not isinstance(open_kwargs, dict):
        raise ValueError(f"Zarr asset for '{dataset_id}' has a malformed xarray:open_kwargs field")
    return xr.open_zarr(href, **open_kwargs)  # type: ignore[no-any-return]
