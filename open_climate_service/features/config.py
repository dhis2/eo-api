"""Validated instance configuration for declared feature sets (CLIM-926).

A declaration binds an id to a provider and its parameters:

    features:
      - id: districts
        provider: dhis2
        params: { level: 2 }

Workflows and triggers then reference ``districts`` rather than carrying its geometry, which is
what lets a schedule stay small and stay correct as the hierarchy grows.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from open_climate_service import config as api_config

DEFAULT_TTL_SECONDS = 24 * 60 * 60
"""How long a resolved collection is reused before the provider is asked again.

A day, because org-unit hierarchies change on the order of months and the cost of being one day
stale is far below the cost of refetching a hierarchy on every scheduled run — or of the pipeline
failing whenever the upstream server is briefly unreachable.
"""


class FeatureDeclaration(BaseModel):
    """One named feature set an instance offers."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    ttl_seconds: int | None = Field(default=None, ge=0)
    id_property: str | None = None
    """Column or property whose value becomes each feature's id.

    The id becomes the geometry-dimension label an export writes against, so for a DHIS2-destined
    set this must name the org-unit UID column. Providers that already return correct ids — the
    DHIS2 one does — leave it unset.
    """

    @property
    def effective_ttl(self) -> int:
        return DEFAULT_TTL_SECONDS if self.ttl_seconds is None else self.ttl_seconds


class FeaturesConfig(BaseModel):
    """The feature sets declared by this instance."""

    model_config = ConfigDict(extra="forbid")

    features: list[FeatureDeclaration] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_ids(self) -> "FeaturesConfig":
        ids = [declaration.id for declaration in self.features]
        duplicates = sorted({value for value in ids if ids.count(value) > 1})
        if duplicates:
            raise ValueError(f"feature ids must be unique: {duplicates}")
        return self

    def get(self, feature_id: str) -> FeatureDeclaration:
        """The declaration for ``feature_id``, or a ValueError naming what is declared."""
        for declaration in self.features:
            if declaration.id == feature_id:
                return declaration
        available = ", ".join(sorted(d.id for d in self.features)) or "none"
        raise ValueError(f"Unknown feature id {feature_id!r}. Declared: {available}")


def get_features_config() -> FeaturesConfig:
    """Load the ``features:`` block from the instance configuration."""
    raw = api_config.get_config().get("features", [])
    if not isinstance(raw, list):
        raise ValueError("features in CLIMATE_SERVICE_CONFIG must be a list")
    return FeaturesConfig.model_validate({"features": raw})
