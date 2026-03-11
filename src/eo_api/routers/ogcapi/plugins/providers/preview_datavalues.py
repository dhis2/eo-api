"""Feature provider for generic DHIS2 dataValue preview collection."""

from __future__ import annotations

from typing import Any

from pygeoapi.provider.base import BaseProvider, ProviderItemNotFoundError, SchemaType
from pygeofilter.backends.native.evaluate import NativeEvaluator

from eo_api.integrations.orchestration import preview_store


class PreviewDataValuesProvider(BaseProvider):
    """Serve preview rows with job-level filtering."""

    _fields: dict[str, dict[str, str]]

    def __init__(self, provider_def: dict[str, Any]) -> None:
        """Initialize provider from pygeoapi definition."""
        super().__init__(provider_def)
        self._fields = {}
        self.get_fields()

    def _load_features(self) -> list[dict[str, Any]]:
        return preview_store.load_preview_features()

    def _normalize_properties_filter(self, properties: list[Any] | None) -> dict[str, str]:
        normalized: dict[str, str] = {}
        if not properties:
            return normalized
        for item in properties:
            if isinstance(item, (tuple, list)) and len(item) == 2:
                key, value = item
                normalized[str(key)] = str(value)
                continue
            if isinstance(item, str) and "=" in item:
                key, value = item.split("=", 1)
                normalized[key] = value
        return normalized

    def get_fields(self) -> dict[str, dict[str, str]]:
        """Return a best-effort schema from feature properties."""
        if self._fields:
            return self._fields

        self._fields = preview_store.infer_preview_fields()
        return self._fields

    def get(self, identifier: str, **kwargs: Any) -> dict[str, Any]:
        """Return one feature by id."""
        feature = preview_store.get_preview_feature(identifier)
        if feature is None:
            raise ProviderItemNotFoundError(f"Feature {identifier} not found")
        return feature

    def query(
        self,
        offset: int = 0,
        limit: int = 10,
        resulttype: str = "results",
        bbox: list[float] | None = None,
        datetime_: str | None = None,
        properties: list[Any] | None = None,
        sortby: list[str] | None = None,
        select_properties: list[str] | None = None,
        skip_geometry: bool = False,
        filterq: list | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return preview feature collection matching query parameters."""
        del resulttype, bbox, datetime_, sortby, select_properties, skip_geometry
        job_id = kwargs.get("job_id")
        features = preview_store.load_preview_features(job_id=job_id if isinstance(job_id, str) and job_id else None)

        if isinstance(job_id, str) and job_id:
            features = [f for f in features if str((f.get("properties") or {}).get("job_id")) == job_id]

        property_filters = self._normalize_properties_filter(properties)
        if property_filters:
            filtered = []
            for feature in features:
                props = feature.get("properties") or {}
                if not isinstance(props, dict):
                    continue
                if all(str(props.get(key)) == value for key, value in property_filters.items()):
                    filtered.append(feature)
            features = filtered

        if filterq:
            evaluator = NativeEvaluator(use_getattr=False)
            match = evaluator.evaluate(filterq)
            features = [f for f in features if match((f.get("properties") or {}))]

        number_matched = len(features)
        page = features[offset : offset + limit]
        return {
            "type": "FeatureCollection",
            "features": page,
            "numberMatched": number_matched,
            "numberReturned": len(page),
        }

    def get_schema(self, schema_type: SchemaType = SchemaType.item) -> tuple[str, dict[str, Any]]:
        """Return GeoJSON schema."""
        return (
            "application/geo+json",
            {"$ref": "https://geojson.org/schema/Feature.json"},
        )
