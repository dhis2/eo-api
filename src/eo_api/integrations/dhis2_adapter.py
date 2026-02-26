"""Shared DHIS2 client adapter for reads and writes."""

from __future__ import annotations

import logging
import os
from typing import Any, cast

from dhis2_client.client import DHIS2Client

LOGGER = logging.getLogger(__name__)
DEFAULT_DHIS2_TIMEOUT_SECONDS = float(os.getenv("DHIS2_HTTP_TIMEOUT_SECONDS", "30"))
DEFAULT_DHIS2_RETRIES = int(os.getenv("DHIS2_HTTP_RETRIES", "3"))


def _normalized_base_url(raw_base_url: str) -> str:
    """Normalize DHIS2 base URL for dhis2-python-client."""
    normalized = raw_base_url.rstrip("/")
    if normalized.endswith("/api"):
        normalized = normalized[: -len("/api")]
        LOGGER.warning(
            "DHIS2_BASE_URL ends with /api; normalizing to '%s' for dhis2-python-client",
            normalized,
        )
    return normalized


def create_client(*, timeout_seconds: float | None = None, retries: int | None = None) -> DHIS2Client:
    """Create a configured DHIS2 client from environment variables."""
    base_url = os.environ.get("DHIS2_BASE_URL")
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    if not base_url or not username or not password:
        raise ValueError("DHIS2_BASE_URL, DHIS2_USERNAME and DHIS2_PASSWORD must be set")

    timeout = DEFAULT_DHIS2_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    retry_count = DEFAULT_DHIS2_RETRIES if retries is None else retries

    return DHIS2Client(
        base_url=_normalized_base_url(base_url),
        username=username,
        password=password,
        timeout=timeout,
        retries=retry_count,
    )


def list_organisation_units(client: DHIS2Client, *, fields: str) -> list[dict[str, Any]]:
    """Fetch organisation units using raw endpoint control over fields."""
    response = query_organisation_units(client, fields=fields)
    org_units = response.get("organisationUnits", [])
    return cast(list[dict[str, Any]], org_units)


def query_organisation_units(
    client: DHIS2Client,
    *,
    fields: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch organisation units with optional pass-through query params."""
    query_params: dict[str, Any] = {
        "paging": "false",
        "fields": fields,
    }
    if params:
        query_params.update(params)
    response = client.get(
        "/api/organisationUnits",
        params=query_params,
    )
    return dict(response)


def get_organisation_unit(client: DHIS2Client, *, uid: str, fields: str) -> dict[str, Any]:
    """Fetch one organisation unit with explicit field projection."""
    result = client.get_org_unit(uid, fields=fields)
    return dict(result)


def get_org_units_geojson(
    client: DHIS2Client,
    *,
    level: int | None = None,
    parent: str | None = None,
) -> dict[str, Any]:
    """Fetch organisation units as GeoJSON FeatureCollection."""
    params: dict[str, Any] = {}
    if level is not None:
        params["level"] = level
    if parent is not None:
        params["parent"] = parent
    return cast(dict[str, Any], client.get_org_units_geojson(**params))


def get_org_unit_geojson(client: DHIS2Client, uid: str) -> dict[str, Any]:
    """Fetch one organisation unit as GeoJSON."""
    return cast(dict[str, Any], client.get_org_unit_geojson(uid))


def get_org_unit_subtree_geojson(client: DHIS2Client, uid: str) -> dict[str, Any]:
    """Fetch a subtree of organisation units as GeoJSON."""
    return cast(dict[str, Any], client.get_org_unit_subtree_geojson(uid))
