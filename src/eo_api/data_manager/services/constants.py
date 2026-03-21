"""Module-level constants for downloader defaults.

This module must stay import-safe. DHIS2-backed defaults are best-effort only,
so startup should not fail when DHIS2 is temporarily unavailable.
"""

import json
import logging
import os

import geopandas as gpd

from ...shared.dhis2_adapter import create_client, get_org_units_geojson

LOGGER = logging.getLogger(__name__)
_DEFAULT_BBOX = [-180.0, -90.0, 180.0, 90.0]


def _bbox_from_env() -> list[float] | None:
    raw_bbox = os.getenv("EO_API_DEFAULT_BBOX")
    if not raw_bbox:
        return None
    parts = [part.strip() for part in raw_bbox.split(",")]
    if len(parts) != 4:
        LOGGER.warning("Ignoring EO_API_DEFAULT_BBOX with invalid value: %s", raw_bbox)
        return None
    try:
        return [float(part) for part in parts]
    except ValueError:
        LOGGER.warning("Ignoring EO_API_DEFAULT_BBOX with non-numeric values: %s", raw_bbox)
        return None


def _load_org_unit_defaults() -> tuple[dict[str, object], list[float]]:
    try:
        client = create_client()
        org_units_geojson = get_org_units_geojson(client, level=2)
        bbox = list(map(float, gpd.read_file(json.dumps(org_units_geojson)).total_bounds))
        return org_units_geojson, bbox
    except Exception as exc:
        fallback_bbox = _bbox_from_env() or _DEFAULT_BBOX
        dhis2_base_url = os.getenv("DHIS2_BASE_URL", "<unset>")
        LOGGER.warning(
            (
                "Failed to load DHIS2 org-unit defaults at startup from DHIS2_BASE_URL=%s. "
                "The server will continue using fallback bbox %s and an empty org-unit GeoJSON cache. "
                "This usually means the DHIS2 server is down, unreachable, or the credentials are invalid. "
                "Original error: %s"
            ),
            dhis2_base_url,
            fallback_bbox,
            exc,
        )
        return {"type": "FeatureCollection", "features": []}, fallback_bbox


# Best-effort startup defaults. Runtime flows can still provide explicit bbox.
ORG_UNITS_GEOJSON, BBOX = _load_org_unit_defaults()

# env variables we need from .env
# TODO: should probably centralize to shared config module
COUNTRY_CODE = os.getenv("COUNTRY_CODE")
CACHE_OVERRIDE = os.getenv("CACHE_OVERRIDE")
