"""TiTiler tile provider plugin for pygeoapi."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from pygeoapi.models.provider.base import TileMatrixSetEnum, TilesMetadataFormat
from pygeoapi.provider.base import (
    ProviderConnectionError,
    ProviderGenericError,
    ProviderInvalidQueryError,
)
from pygeoapi.provider.tile import BaseTileProvider, ProviderTileNotFoundError
from pygeoapi.util import is_url, url_join

_DEFAULT_TITILER_BASE_URL = "http://127.0.0.1:8000"
_DEFAULT_TITILER_ENDPOINT = "/cog/tiles"


class TiTilerProvider(BaseTileProvider):
    """TiTiler-backed OGC API Tiles provider."""

    def __init__(self, provider_def: dict[str, Any]) -> None:
        """Initialize provider from pygeoapi provider configuration."""
        format_name = provider_def["format"]["name"].lower()
        if format_name not in {"png", "jpeg", "jpg", "webp"}:
            raise RuntimeError("TiTiler format must be png, jpeg, jpg, or webp")

        options = dict(provider_def.get("options") or {})
        scheme = options.get("scheme")
        schemes = options.get("schemes")
        if not schemes:
            options["schemes"] = [scheme] if scheme else ["WebMercatorQuad"]

        zoom = dict(options.get("zoom") or {})
        zoom.setdefault("min", 0)
        zoom.setdefault("max", 24)
        options["zoom"] = zoom

        options.setdefault("endpoint", _DEFAULT_TITILER_ENDPOINT)
        options.setdefault(
            "endpoint_base",
            os.getenv("TITILER_BASE_URL", _DEFAULT_TITILER_BASE_URL),
        )
        options.setdefault("timeout", 30)

        provider_def = {**provider_def, "options": options}
        super().__init__(provider_def)

        self.tile_type = "raster"
        self._layer = Path(self.data).stem

    def __repr__(self) -> str:
        """Return repr string."""
        return f"<TiTilerProvider> {self.data}"

    def get_layer(self) -> str:
        """Return provider layer name."""
        return self._layer

    def get_fields(self) -> dict[str, Any]:
        """Return field metadata for provider."""
        return {}

    @property
    def endpoint(self) -> str:
        """Return absolute TiTiler endpoint."""
        configured = str(self.options.get("endpoint", _DEFAULT_TITILER_ENDPOINT))
        if is_url(configured):
            return configured.rstrip("/")

        base = str(self.options.get("endpoint_base", _DEFAULT_TITILER_BASE_URL)).rstrip("/")
        if configured.startswith("/"):
            return f"{base}{configured}".rstrip("/")
        return f"{base}/{configured}".rstrip("/")

    def get_tiling_schemes(self) -> list[Any]:
        """Return configured tiling schemes."""
        configured = set(self.options.get("schemes", []))
        tile_matrix_set_links = [
            enum.value
            for enum in TileMatrixSetEnum
            if enum.value.tileMatrixSet in configured
        ]
        if not tile_matrix_set_links:
            raise ProviderConnectionError("Could not identify any valid tiling scheme")
        return tile_matrix_set_links

    def get_tiles_service(
        self,
        baseurl: str | None = None,
        servicepath: str | None = None,
        dirpath: str | None = None,
        tile_type: str | None = None,
    ) -> dict[str, list[dict[str, str]]]:
        """Return links describing tile access endpoints."""
        del dirpath, tile_type

        format_name = self.format_type
        if servicepath is None:
            servicepath = (
                "collections/{dataset}/tiles/{tileMatrixSetId}/"
                "{tileMatrix}/{tileRow}/{tileCol}?f="
                f"{format_name}"
            )

        if baseurl and not servicepath.startswith("http"):
            self._service_url = url_join(baseurl, servicepath)
        else:
            self._service_url = servicepath

        tileset_endpoint = self.endpoint
        default_scheme = self.options["schemes"][0]
        query = urlencode(self._titiler_query_params())
        titiler_href = (
            f"{tileset_endpoint}/{default_scheme}/{{tileMatrix}}/{{tileCol}}/"
            f"{{tileRow}}.{format_name}?{query}"
        )

        metadata_href = self._service_url.split("/{tileMatrix}/{tileRow}/{tileCol}")[0]
        metadata_href = url_join(metadata_href, "metadata")

        return {
            "links": [
                {
                    "type": self.mimetype,
                    "rel": "item",
                    "title": "This collection as image tiles",
                    "href": self._service_url,
                },
                {
                    "type": self.mimetype,
                    "rel": "alternate",
                    "title": "Direct TiTiler tile URL template",
                    "href": titiler_href,
                },
                {
                    "type": "application/json",
                    "rel": "describedby",
                    "title": "Collection metadata in TileJSON format",
                    "href": f"{metadata_href}?f=tilejson",
                },
            ]
        }

    def get_tiles(
        self,
        layer: str | None = None,
        tileset: str | None = None,
        z: int | None = None,
        y: int | None = None,
        x: int | None = None,
        format_: str | None = None,
    ) -> bytes | None:
        """Fetch and return tile bytes from TiTiler."""
        del layer

        if tileset is None:
            raise ProviderInvalidQueryError("Missing tileset identifier")
        if z is None or y is None or x is None:
            raise ProviderInvalidQueryError("Missing tile coordinates")

        tms = self.get_tilematrixset(tileset)
        if tms is None or not self.is_in_limits(tms, z, x, y):
            raise ProviderTileNotFoundError

        tile_format = (format_ or self.format_type).lower()
        if tile_format == "jpg":
            tile_format = "jpeg"

        request_url = f"{self.endpoint}/{tileset}/{z}/{x}/{y}.{tile_format}"
        timeout = int(self.options.get("timeout", 30))

        try:
            response = requests.get(
                request_url,
                params=self._titiler_query_params(),
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise ProviderConnectionError(str(exc)) from exc

        if response.status_code == 204:
            return None
        if response.status_code == 404:
            raise ProviderTileNotFoundError
        if response.status_code < 500 and not response.ok:
            raise ProviderInvalidQueryError(response.text)
        if response.status_code >= 500:
            raise ProviderGenericError(response.text)

        return response.content

    def get_metadata(
        self,
        dataset: str,
        server_url: str,
        layer: str | None = None,
        tileset: str | None = None,
        metadata_format: str | None = None,
        title: str | None = None,
        description: str | None = None,
        keywords: list[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return tile metadata for a collection tileset."""
        del layer, kwargs

        requested_format = str(metadata_format or TilesMetadataFormat.TILEJSON).upper()
        if requested_format in {TilesMetadataFormat.HTML, "HTML"}:
            return self._get_html_metadata(dataset, server_url, tileset, title)
        if requested_format in {
            TilesMetadataFormat.TILEJSON,
            TilesMetadataFormat.JSON,
            "TILEJSON",
            "JSON",
        }:
            return self._get_tilejson_metadata(
                dataset,
                server_url,
                tileset,
                title,
                description,
                keywords,
            )
        raise NotImplementedError(f"{requested_format} metadata is not supported")

    def _get_tilejson_metadata(
        self,
        dataset: str,
        server_url: str,
        tileset: str | None,
        title: str | None,
        description: str | None,
        keywords: list[str] | None,
    ) -> dict[str, Any]:
        """Return TileJSON metadata response."""
        tileset_name = tileset or self.options["schemes"][0]
        tile_url = url_join(
            server_url,
            f"collections/{dataset}/tiles/{tileset_name}/{{z}}/{{y}}/{{x}}?f={self.format_type}",
        )

        bounds = self.options.get("bounds", [-180.0, -85.05112878, 180.0, 85.05112878])
        minzoom = int(self.options["zoom"]["min"])
        maxzoom = int(self.options["zoom"]["max"])

        return {
            "tilejson": "3.0.0",
            "name": dataset,
            "title": title or dataset,
            "description": description or "TiTiler-backed map tiles",
            "version": "1.0.0",
            "scheme": "xyz",
            "format": self.format_type,
            "bounds": bounds,
            "minzoom": minzoom,
            "maxzoom": maxzoom,
            "tiles": [tile_url],
            "keywords": keywords or [],
        }

    def _get_html_metadata(
        self,
        dataset: str,
        server_url: str,
        tileset: str | None,
        title: str | None,
    ) -> dict[str, Any]:
        """Return metadata payload used by pygeoapi HTML metadata template."""
        tileset_name = tileset or self.options["schemes"][0]
        collections_path = url_join(
            server_url,
            f"collections/{dataset}/tiles/{tileset_name}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?f={self.format_type}",
        )
        metadata_url = url_join(
            server_url,
            f"collections/{dataset}/tiles/{tileset_name}/metadata",
        )

        return {
            "id": dataset,
            "title": title or dataset,
            "tileset": tileset_name,
            "collections_path": collections_path,
            "json_url": f"{metadata_url}?f=json",
            "tilejson_url": f"{metadata_url}?f=tilejson",
            "metadata": {
                "format": self.format_type,
                "scheme": tileset_name,
                "source": self.data,
            },
        }

    def _titiler_query_params(self) -> dict[str, Any]:
        """Return query parameters forwarded to TiTiler tile endpoint."""
        ignored = {"scheme", "schemes", "zoom", "endpoint", "endpoint_base", "timeout"}
        params = {"url": self.data}
        params.update({k: v for k, v in self.options.items() if k not in ignored})
        return params