"""Module-level constants loaded at import time (DHIS2 org units, bbox, env config)."""

import json
import os

import geopandas as gpd

from ..integrations.dhis2_adapter import create_client, get_org_units_geojson

# load geojson from dhis2 at startup and keep in-memory
# TODO: should probably save to file instead
client = create_client()
ORG_UNITS_GEOJSON = get_org_units_geojson(client, level=2)
BBOX = list(map(float, gpd.read_file(json.dumps(ORG_UNITS_GEOJSON)).total_bounds))

# env variables we need from .env
# TODO: should probably centralize to shared config module
COUNTRY_CODE = os.getenv("COUNTRY_CODE")
CACHE_OVERRIDE = os.getenv("CACHE_OVERRIDE")
