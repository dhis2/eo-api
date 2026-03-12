from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator


class FeatureSourceType(StrEnum):
    """Supported feature source backends."""

    GEOJSON_FILE = "geojson_file"
    DHIS2_LEVEL = "dhis2_level"
    DHIS2_IDS = "dhis2_ids"


class FeatureSourceConfig(BaseModel):
    """How to fetch features for spatial aggregation."""

    source_type: FeatureSourceType
    geojson_path: str | None = None
    dhis2_level: int | None = None
    dhis2_ids: list[str] | None = None
    dhis2_parent: str | None = None
    feature_id_property: str = "id"

    @model_validator(mode="after")
    def validate_by_source(self) -> "FeatureSourceConfig":
        """Enforce required fields per source backend."""
        if self.source_type == FeatureSourceType.GEOJSON_FILE and not self.geojson_path:
            raise ValueError("geojson_path is required when source_type='geojson_file'")
        if self.source_type == FeatureSourceType.DHIS2_LEVEL and self.dhis2_level is None:
            raise ValueError("dhis2_level is required when source_type='dhis2_level'")
        if self.source_type == FeatureSourceType.DHIS2_IDS and not self.dhis2_ids:
            raise ValueError("dhis2_ids is required when source_type='dhis2_ids'")
        return self


class _FeatureSourceStepConfig(BaseModel):
    # from workflows folder
    model_config = ConfigDict(extra="forbid")
