"""Shared DHIS2 models, constants, and helpers for org-unit providers."""

import os
from datetime import datetime
from typing import Any

import httpx
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from pydantic import BaseModel, Field

DHIS2_BASE_URL = os.environ["DHIS2_BASE_URL"]
DHIS2_AUTH = (os.environ["DHIS2_USERNAME"], os.environ["DHIS2_PASSWORD"])
DHIS2_FIELDS = "id,code,name,shortName,level,openingDate,geometry"


class DHIS2OrgUnit(BaseModel):
    """Organisation unit as returned by the DHIS2 API."""

    id: str
    name: str | None = None
    code: str | None = None
    shortName: str | None = None
    level: int | None = None
    openingDate: datetime | None = None
    geometry: Geometry | None = None


class OrgUnitProperties(BaseModel):
    """Feature properties for a DHIS2 org unit."""

    name: str | None = Field(None, title="Name")
    code: str | None = Field(None, title="Code")
    shortName: str | None = Field(None, title="Short name")
    level: int | None = Field(None, title="Level")
    openingDate: str | None = Field(None, title="Opening date")


def schema_to_fields(model: type[BaseModel]) -> dict[str, dict[str, str]]:
    """Convert a Pydantic model's JSON Schema to pygeoapi field definitions."""
    schema = model.model_json_schema()
    fields: dict[str, dict[str, str]] = {}
    for name, prop in schema["properties"].items():
        if "anyOf" in prop:
            types = [t for t in prop["anyOf"] if t.get("type") != "null"]
            field_type = types[0]["type"] if types else "string"
        else:
            field_type = prop.get("type", "string")
        field_def: dict[str, str] = {"type": field_type}
        if "title" in prop:
            field_def["title"] = prop["title"]
        fields[name] = field_def
    return fields


def flatten_coords(coords: list) -> list[list[float]]:
    """Recursively flatten nested coordinate arrays into a list of [x, y] points."""
    if coords and isinstance(coords[0], (int, float)):
        return [coords]
    result: list[list[float]] = []
    for item in coords:
        result.extend(flatten_coords(item))
    return result


def compute_bbox(geometry: Geometry) -> tuple[float, float, float, float]:
    """Compute bounding box from a GeoJSON geometry."""
    coords = flatten_coords(geometry.model_dump()["coordinates"])
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    return (min(xs), min(ys), max(xs), max(ys))


def fetch_bbox() -> list[float] | None:
    """Compute bounding box from level-1 org unit geometries."""
    response = httpx.get(
        f"{DHIS2_BASE_URL}/organisationUnits",
        params={
            "paging": "false",
            "fields": "geometry",
            "filter": "level:eq:1",
        },
        auth=DHIS2_AUTH,
        follow_redirects=True,
    )
    response.raise_for_status()
    all_coords: list[list[float]] = []
    for ou in response.json()["organisationUnits"]:
        geom = ou.get("geometry")
        if geom and geom.get("coordinates"):
            all_coords.extend(flatten_coords(geom["coordinates"]))
    if not all_coords:
        return None
    xs = [c[0] for c in all_coords]
    ys = [c[1] for c in all_coords]
    return [min(xs), min(ys), max(xs), max(ys)]


def fetch_org_units() -> list[DHIS2OrgUnit]:
    """Fetch all organisation units from the DHIS2 API."""
    response = httpx.get(
        f"{DHIS2_BASE_URL}/organisationUnits",
        params={"paging": "false", "fields": DHIS2_FIELDS},
        auth=DHIS2_AUTH,
        follow_redirects=True,
    )
    response.raise_for_status()
    return [DHIS2OrgUnit.model_validate(ou) for ou in response.json()["organisationUnits"]]


def org_unit_to_feature(org_unit: DHIS2OrgUnit) -> Feature:
    """Convert a DHIS2 org unit to a GeoJSON Feature."""
    props = OrgUnitProperties(
        name=org_unit.name,
        code=org_unit.code,
        shortName=org_unit.shortName,
        level=org_unit.level,
        openingDate=org_unit.openingDate.isoformat() if org_unit.openingDate else None,
    )
    bbox = None
    if org_unit.geometry and org_unit.geometry.type in ("Polygon", "MultiPolygon"):
        bbox = compute_bbox(org_unit.geometry)
    return Feature(
        type="Feature",
        id=org_unit.id,
        geometry=org_unit.geometry,
        properties=props.model_dump(),
        bbox=bbox,
    )


def get_single_org_unit(identifier: str) -> dict[str, Any]:
    """Fetch a single org unit by ID and return as a feature dict."""
    response = httpx.get(
        f"{DHIS2_BASE_URL}/organisationUnits/{identifier}",
        params={"fields": DHIS2_FIELDS},
        auth=DHIS2_AUTH,
        follow_redirects=True,
    )
    response.raise_for_status()
    org_unit = DHIS2OrgUnit.model_validate(response.json())
    return org_unit_to_feature(org_unit).model_dump()
