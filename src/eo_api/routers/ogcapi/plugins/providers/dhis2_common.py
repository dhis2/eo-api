"""Shared DHIS2 models, constants, and helpers for org-unit providers."""

from datetime import datetime
from typing import Any

from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from pydantic import BaseModel, ConfigDict, Field
from pygeofilter import ast

from eo_api.integrations.dhis2_adapter import (
    create_client,
    get_org_units_geojson,
    get_organisation_unit,
    query_organisation_units,
)

DHIS2_FIELDS = "id,code,name,shortName,level,openingDate,geometry"
DHIS2_QUERY_PARAM_KEYS = {
    "filter",
    "fields",
    "paging",
    "page",
    "pageSize",
    "order",
    "includeDescendants",
    "withinUserHierarchy",
    "withinUserSearchHierarchy",
    "query",
}
OGC_TO_DHIS2_FIELD_MAP = {
    "id": "id",
    "name": "name",
    "code": "code",
    "shortName": "shortName",
    "level": "level",
    "openingDate": "openingDate",
    "geometry": "geometry",
}


class DHIS2OrgUnit(BaseModel):
    """Organisation unit as returned by the DHIS2 API."""

    model_config = ConfigDict(extra="allow")

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


def fetch_bbox(timeout_seconds: float | None = None) -> list[float] | None:
    """Compute bounding box from level-1 org unit geometries."""
    client = create_client(timeout_seconds=timeout_seconds)
    try:
        feature_collection = get_org_units_geojson(client, level=1)
    finally:
        client.close()

    all_coords: list[list[float]] = []
    for feature in feature_collection.get("features", []):
        geom = feature.get("geometry")
        if geom and geom.get("coordinates"):
            all_coords.extend(flatten_coords(geom["coordinates"]))
    if not all_coords:
        return None
    xs = [c[0] for c in all_coords]
    ys = [c[1] for c in all_coords]
    return [min(xs), min(ys), max(xs), max(ys)]


def extract_dhis2_query_options(
    properties: list[Any] | None,
    kwargs: dict[str, Any],
    *,
    default_fields: str = DHIS2_FIELDS,
) -> tuple[str, dict[str, Any], bool]:
    """Extract DHIS2-native query options from provider query kwargs."""
    extra_params: dict[str, Any] = {}
    # Unknown query params are forwarded by pygeoapi as (key, value) tuples in `properties`.
    for item in properties or []:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            key = str(item[0])
            value = item[1]
            extra_params[key] = value

    # Use dhis2_* query params to avoid collision with OGC params like filter/cql.
    raw_fields = str(extra_params.get("dhis2_fields", kwargs.get("dhis2_fields", default_fields)))
    fields = normalize_dhis2_fields(raw_fields)
    params: dict[str, Any] = {}
    for key in DHIS2_QUERY_PARAM_KEYS:
        if key == "fields":
            continue
        raw_value = extra_params.get(f"dhis2_{key}", kwargs.get(f"dhis2_{key}"))
        if raw_value is None:
            continue
        if key == "filter" and isinstance(raw_value, str) and "," in raw_value:
            params[key] = [part.strip() for part in raw_value.split(",") if part.strip()]
        else:
            params[key] = raw_value

    def _is_truthy(value: Any) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes"}

    fetch_all = _is_truthy(extra_params.get("all", kwargs.get("all")))
    if fetch_all:
        params["paging"] = "false"

    return fields, params, fetch_all


def normalize_dhis2_fields(raw_fields: str) -> str:
    """Normalize DHIS2 fields list and enforce required `id`."""
    parsed = _split_top_level_csv(raw_fields)
    if not parsed:
        return "id"
    if "id" not in parsed:
        parsed.insert(0, "id")
    # de-duplicate while preserving order
    unique = list(dict.fromkeys(parsed))
    return ",".join(unique)


def _split_top_level_csv(value: str) -> list[str]:
    """Split comma-separated values while preserving nested bracket groups."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in value:
        if ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth = max(0, depth - 1)
        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def fields_from_select_properties(select_properties: list[str] | None, *, skip_geometry: bool = False) -> str:
    """Build DHIS2 fields projection from OGC select properties."""
    if not select_properties:
        base = {"id", "name", "code", "shortName", "level", "openingDate"}
    else:
        base = {"id"}
        for field in select_properties:
            mapped = OGC_TO_DHIS2_FIELD_MAP.get(field, field)
            if mapped:
                base.add(mapped)
    if not skip_geometry:
        base.add("geometry")
    return ",".join(sorted(base))


def _attribute_name(node: Any) -> str | None:
    if isinstance(node, ast.Attribute):
        return str(node.name)
    return None


def _escape_filter_value(value: Any) -> str:
    return str(value).replace(",", "\\,")


def cql_to_dhis2_filters(filterq: Any) -> list[str] | None:
    """Translate a supported CQL AST subset into DHIS2 filter expressions."""
    if filterq is None:
        return []

    if isinstance(filterq, ast.And):
        lhs = cql_to_dhis2_filters(filterq.lhs)
        rhs = cql_to_dhis2_filters(filterq.rhs)
        if lhs is None or rhs is None:
            return None
        return lhs + rhs

    if isinstance(filterq, ast.Equal):
        field = _attribute_name(filterq.lhs)
        if field is None:
            return None
        return [f"{field}:eq:{_escape_filter_value(filterq.rhs)}"]

    if isinstance(filterq, ast.In):
        field = _attribute_name(filterq.lhs)
        if field is None:
            return None
        values = ",".join(_escape_filter_value(v) for v in filterq.sub_nodes)
        return [f"{field}:in:[{values}]"]

    if isinstance(filterq, ast.Like):
        field = _attribute_name(filterq.lhs)
        if field is None:
            return None
        op = "ilike" if filterq.nocase else "like"
        return [f"{field}:{op}:{_escape_filter_value(filterq.pattern)}"]

    return None


def merge_dhis2_filters(params: dict[str, Any], extra_filters: list[str]) -> dict[str, Any]:
    """Merge additional filters into DHIS2 query params."""
    merged = dict(params)
    current = merged.get("filter")
    if current is None:
        merged["filter"] = extra_filters
        return merged
    if isinstance(current, list):
        merged["filter"] = current + extra_filters
    else:
        merged["filter"] = [str(current), *extra_filters]
    return merged


def fetch_org_units(*, fields: str = DHIS2_FIELDS, dhis2_params: dict[str, Any] | None = None) -> list[DHIS2OrgUnit]:
    """Fetch organisation units from DHIS2, optionally with pass-through params."""
    client = create_client()
    try:
        response = query_organisation_units(client, fields=fields, params=dhis2_params)
        organisation_units = response.get("organisationUnits", [])
    finally:
        client.close()
    return [DHIS2OrgUnit.model_validate(ou) for ou in organisation_units]


def org_unit_to_feature(org_unit: DHIS2OrgUnit) -> Feature:
    """Convert a DHIS2 org unit to a GeoJSON Feature."""
    props = OrgUnitProperties(
        name=org_unit.name,
        code=org_unit.code,
        shortName=org_unit.shortName,
        level=org_unit.level,
        openingDate=org_unit.openingDate.isoformat() if org_unit.openingDate else None,
    ).model_dump(exclude_none=True)
    # Preserve requested DHIS2-native fields not modeled explicitly.
    for key, value in (org_unit.model_extra or {}).items():
        if value is not None and key not in props:
            props[key] = value
    bbox = None
    if org_unit.geometry and org_unit.geometry.type in ("Polygon", "MultiPolygon"):
        bbox = compute_bbox(org_unit.geometry)
    return Feature(
        type="Feature",
        id=org_unit.id,
        geometry=org_unit.geometry,
        properties=props,
        bbox=bbox,
    )


def get_single_org_unit(identifier: str) -> dict[str, Any]:
    """Fetch a single org unit by ID and return as a feature dict."""
    client = create_client()
    try:
        org_unit_data = get_organisation_unit(client, uid=identifier, fields=DHIS2_FIELDS)
    finally:
        client.close()
    org_unit = DHIS2OrgUnit.model_validate(org_unit_data)
    return org_unit_to_feature(org_unit).model_dump()
