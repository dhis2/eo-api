"""Schemas for generic components."""

from __future__ import annotations

from typing import Any
from enum import StrEnum

from pydantic import BaseModel, Field


class ComponentDefinition(BaseModel):
    """Component metadata for discovery."""

    name: str
    version: str = "v1"
    description: str
    inputs: list[str]
    outputs: list[str]
    input_schema: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)
    error_codes: list[str] = Field(default_factory=list)


class ComponentCatalogResponse(BaseModel):
    """List of discoverable components."""

    components: list[ComponentDefinition]


# below is shared across components, maybe move elsewhere...

class PeriodType(StrEnum):
    """Supported temporal period types."""

    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"
