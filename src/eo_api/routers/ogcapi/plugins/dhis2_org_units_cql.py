"""DHIS2 Organization Units feature provider with CQL filter support."""

from typing import Any

from pygeoapi.provider.base import BaseProvider, SchemaType
from pygeofilter.backends.native.evaluate import NativeEvaluator

from eo_api.routers.ogcapi.plugins.dhis2_common import (
    OrgUnitProperties,
    fetch_org_units,
    get_single_org_unit,
    org_unit_to_feature,
    schema_to_fields,
)


class DHIS2OrgUnitsCqlProvider(BaseProvider):
    """DHIS2 Organization Units Provider with CQL filter support."""

    def __init__(self, provider_def: dict[str, Any]) -> None:
        """Inherit from parent class."""
        super().__init__(provider_def)
        self.get_fields()

    def get_fields(self) -> dict[str, dict[str, str]]:
        """Return fields and their datatypes."""
        if not self._fields:
            self._fields = schema_to_fields(OrgUnitProperties)
        return self._fields

    def get(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Return a single feature by identifier."""
        return get_single_org_unit(identifier)

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
        filterq: list | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return feature collection matching the query parameters."""
        org_units = fetch_org_units()
        features = [org_unit_to_feature(ou).model_dump() for ou in org_units]

        if filterq:
            evaluator = NativeEvaluator(use_getattr=False)
            match = evaluator.evaluate(filterq)
            features = [f for f in features if match(f["properties"])]

        number_matched = len(features)
        page = features[offset : offset + limit]

        return {
            "type": "FeatureCollection",
            "features": page,
            "numberMatched": number_matched,
            "numberReturned": len(page),
        }

    def get_schema(self, schema_type: SchemaType = SchemaType.item) -> tuple[str, dict[str, Any]]:
        """Return a JSON schema for the provider."""
        return (
            "application/geo+json",
            {"$ref": "https://geojson.org/schema/Feature.json"},
        )
