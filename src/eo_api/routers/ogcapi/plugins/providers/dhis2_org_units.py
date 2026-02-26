"""DHIS2 Organization Units feature provider for pygeoapi."""

from typing import Any

from pygeoapi.provider.base import BaseProvider, SchemaType
from pygeofilter.backends.native.evaluate import NativeEvaluator

from eo_api.routers.ogcapi.plugins.providers.dhis2_common import (
    OrgUnitProperties,
    cql_to_dhis2_filters,
    extract_dhis2_query_options,
    fetch_org_units,
    fields_from_select_properties,
    get_single_org_unit,
    merge_dhis2_filters,
    org_unit_to_feature,
    schema_to_fields,
)


class DHIS2OrgUnitsProvider(BaseProvider):
    """DHIS2 Organization Units Provider."""

    _fields: dict[str, dict[str, str]]

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
        filterq: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return feature collection matching the query parameters."""
        default_fields = fields_from_select_properties(select_properties, skip_geometry=skip_geometry)
        fields, dhis2_params, fetch_all = extract_dhis2_query_options(properties, kwargs, default_fields=default_fields)

        cql_filters = cql_to_dhis2_filters(filterq)
        use_local_filter = False
        if cql_filters is None and filterq is not None:
            use_local_filter = True
        elif cql_filters:
            dhis2_params = merge_dhis2_filters(dhis2_params, cql_filters)

        use_server_paging = False
        if not fetch_all and limit > 0 and "page" not in dhis2_params and "pageSize" not in dhis2_params:
            if offset % limit == 0:
                dhis2_params = dict(dhis2_params)
                dhis2_params["paging"] = "true"
                dhis2_params["pageSize"] = str(limit)
                dhis2_params["page"] = str((offset // limit) + 1)
                use_server_paging = True

        org_units = fetch_org_units(fields=fields, dhis2_params=dhis2_params)

        features = [org_unit_to_feature(ou).model_dump() for ou in org_units]
        if use_local_filter and filterq is not None:
            evaluator = NativeEvaluator(use_getattr=False)
            match = evaluator.evaluate(filterq)
            features = [feature for feature in features if match(feature["properties"])]

        number_matched = len(features)
        if fetch_all or use_server_paging:
            page = features
        else:
            page = features[offset : offset + limit]

        result: dict[str, Any] = {
            "type": "FeatureCollection",
            "features": page,
        }
        result["numberMatched"] = number_matched
        result["numberReturned"] = len(page)
        return result

    def get_schema(self, schema_type: SchemaType = SchemaType.item) -> tuple[str, dict[str, Any]]:
        """Return a JSON schema for the provider."""
        return (
            "application/geo+json",
            {"$ref": "https://geojson.org/schema/Feature.json"},
        )
