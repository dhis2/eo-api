"""DHIS2 Organization Units feature provider for pygeoapi."""

import os
from datetime import datetime
from typing import Any

import httpx
from geojson_pydantic import Feature, FeatureCollection
from geojson_pydantic.geometries import Geometry
from pydantic import BaseModel, Field
from pygeoapi.provider.base import BaseProvider, SchemaType

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


def _schema_to_fields(model: type[BaseModel]) -> dict[str, dict[str, str]]:
    """Convert a Pydantic model's JSON Schema to pygeoapi field definitions."""
    schema = model.model_json_schema()
    fields = {}
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


def _flatten_coords(coords: list) -> list[list[float]]:
    """Recursively flatten nested coordinate arrays into a list of [x, y] points."""
    if coords and isinstance(coords[0], (int, float)):
        return [coords]
    result = []
    for item in coords:
        result.extend(_flatten_coords(item))
    return result


def _compute_bbox(geometry: Geometry) -> tuple[float, float, float, float]:
    """Compute bounding box from a GeoJSON geometry."""
    coords = _flatten_coords(geometry.model_dump()["coordinates"])
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    return (min(xs), min(ys), max(xs), max(ys))


def _fetch_bbox() -> list[float] | None:
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
            all_coords.extend(_flatten_coords(geom["coordinates"]))
    if not all_coords:
        return None
    xs = [c[0] for c in all_coords]
    ys = [c[1] for c in all_coords]
    return [min(xs), min(ys), max(xs), max(ys)]


def _fetch_org_units() -> list[DHIS2OrgUnit]:
    """Fetch all organisation units from the DHIS2 API."""
    response = httpx.get(
        f"{DHIS2_BASE_URL}/organisationUnits",
        params={"paging": "false", "fields": DHIS2_FIELDS},
        auth=DHIS2_AUTH,
        follow_redirects=True,
    )
    response.raise_for_status()
    return [DHIS2OrgUnit.model_validate(ou) for ou in response.json()["organisationUnits"]]


def _org_unit_to_feature(org_unit: DHIS2OrgUnit) -> Feature:
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
        bbox = _compute_bbox(org_unit.geometry)
    return Feature(
        type="Feature",
        id=org_unit.id,
        geometry=org_unit.geometry,
        properties=props.model_dump(),
        bbox=bbox,
    )


class DHIS2OrgUnitsProvider(BaseProvider):
    """DHIS2 Organization Units Provider."""

    def __init__(self, provider_def: dict[str, Any]) -> None:
        """Inherit from parent class."""
        super().__init__(provider_def)
        self.get_fields()

    def get_fields(self) -> dict[str, dict[str, str]]:
        """Return fields and their datatypes."""
        if not self._fields:
            self._fields = _schema_to_fields(OrgUnitProperties)
        return self._fields

    def get(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Return a single feature by identifier."""
        response = httpx.get(
            f"{DHIS2_BASE_URL}/organisationUnits/{identifier}",
            params={"fields": DHIS2_FIELDS},
            auth=DHIS2_AUTH,
            follow_redirects=True,
        )
        response.raise_for_status()
        org_unit = DHIS2OrgUnit.model_validate(response.json())
        return _org_unit_to_feature(org_unit).model_dump()

    def query(
        self,
        offset: int = 0,
        limit: int = 10,
        resulttype: str = "results",
        bbox: list[float] | None = None,
        datetime_: str | None = None,
        properties: list[str] | None = None,
        sortby: list[str] | None = None,
        select_properties: list[str] | None = None,
        skip_geometry: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return feature collection matching the query parameters."""
        org_units = _fetch_org_units()
        number_matched = len(org_units)
        page = org_units[offset : offset + limit]

        fc = FeatureCollection(
            type="FeatureCollection",
            features=[_org_unit_to_feature(ou) for ou in page],
        )
        result = fc.model_dump()
        result["numberMatched"] = number_matched
        result["numberReturned"] = len(page)
        return result

    def get_schema(self, schema_type: SchemaType = SchemaType.item) -> tuple[str, dict[str, Any]]:
        """Return a JSON schema for the provider."""
        return (
            "application/geo+json",
            {"$ref": "https://geojson.org/schema/Feature.json"},
        )
