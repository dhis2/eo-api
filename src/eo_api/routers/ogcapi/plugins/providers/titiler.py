"""TiTiler tile provider plugin for pygeoapi."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast
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

# Mostly generated with copilot agent, might need some cleanup but serves as a good starting point
# for a simple TiTiler provider plugin.


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

    def get_layer(self) -> None:
        """Return provider layer name.

        BaseTileProvider defines this method without a typed return contract,
        and pygeoapi does not rely on this provider's layer value.
        """
        return None

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
        tile_matrix_set_enum = cast(Any, TileMatrixSetEnum)
        tile_matrix_set_links = [enum.value for enum in tile_matrix_set_enum if enum.value.tileMatrixSet in configured]
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
            f"{tileset_endpoint}/{default_scheme}/{{tileMatrix}}/{{tileCol}}/{{tileRow}}.{format_name}?{query}"
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
        if self._is_tile_outside_dataset_bounds(tileset, z, x, y):
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
        if response.status_code >= 500 and (
            self._is_tile_outside_bounds(response) or self._is_tile_outside_dataset_bounds(tileset, z, x, y)
        ):
            raise ProviderTileNotFoundError
        if response.status_code < 500 and not response.ok:
            raise ProviderInvalidQueryError(response.text)
        if response.status_code >= 500:
            raise ProviderGenericError(response.text)

        return cast(bytes, response.content)

    def get_metadata(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Return tile metadata for a collection tileset."""
        dataset = str(kwargs.get("dataset", ""))
        server_url = str(kwargs.get("server_url", ""))
        tileset = cast(str | None, kwargs.get("tileset"))
        metadata_format = cast(str | None, kwargs.get("metadata_format"))
        title = cast(str | None, kwargs.get("title"))
        description = cast(str | None, kwargs.get("description"))
        keywords = cast(list[str] | None, kwargs.get("keywords"))

        if not dataset or not server_url:
            raise ProviderInvalidQueryError("dataset and server_url are required for tile metadata")

        del args

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

    def _is_tile_outside_bounds(self, response: requests.Response) -> bool:
        """Return true when TiTiler reports a tile outside raster bounds."""
        text = (response.text or "").lower()
        if "tileoutsidebounds" in text or "outside bounds" in text:
            return True

        try:
            payload = response.json()
        except ValueError:
            return False

        detail = str(payload.get("detail", "")).lower()
        return "tileoutsidebounds" in detail or "outside bounds" in detail

    def _is_tile_outside_dataset_bounds(self, tileset: str, z: int, x: int, y: int) -> bool:
        """Return true when the requested tile does not intersect local dataset bounds."""
        if is_url(self.data):
            return False

        data_path = Path(self.data)
        if not data_path.exists():
            return False

        try:
            import morecantile
            import rasterio
            from rasterio.crs import CRS
            from rasterio.warp import transform_bounds
        except Exception:
            return False

        try:
            tms_registry = getattr(morecantile, "tms")
            tms = tms_registry.get(tileset)
            tile_cls = getattr(morecantile, "Tile")
            tile = tile_cls(x=int(x), y=int(y), z=int(z))
            tile_bounds = tms.xy_bounds(tile)
            tile_crs = CRS.from_string(str(tms.crs.root))

            with rasterio.open(data_path) as src:
                if src.crs is None:
                    return False

                transformed = transform_bounds(
                    tile_crs,
                    src.crs,
                    tile_bounds.left,
                    tile_bounds.bottom,
                    tile_bounds.right,
                    tile_bounds.top,
                    densify_pts=21,
                )
                src_bounds = src.bounds

            tile_left, tile_bottom, tile_right, tile_top = transformed
            return bool(
                tile_right <= src_bounds.left
                or tile_left >= src_bounds.right
                or tile_top <= src_bounds.bottom
                or tile_bottom >= src_bounds.top
            )
        except Exception:
            return False
