"""Native OGC API - Processes schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OGCOutputFormatInfo(BaseModel):
    """Format descriptor for one OGC process output."""

    media_type: str = Field(description="IANA media type for the output payload")
    schema_url: str | None = Field(default=None, description="Optional schema or specification URL")
    encoding: str | None = Field(default="UTF-8", description="Character encoding when applicable")


class OGCOutputValue(BaseModel):
    """Inline OGC process output."""

    id: str = Field(description="Output identifier")
    value: Any = Field(description="Inline output value")
    format: OGCOutputFormatInfo = Field(description="Format metadata")
    title: str | None = Field(default=None)
    description: str | None = Field(default=None)


class OGCOutputReference(BaseModel):
    """Referenced OGC process output."""

    id: str = Field(description="Output identifier")
    href: str = Field(description="Absolute URL to the referenced output")
    format: OGCOutputFormatInfo = Field(description="Format metadata")
    title: str | None = Field(default=None)
    description: str | None = Field(default=None)
    rel: str = Field(default="related", description="Relationship type")


class OGCJobResultsResponse(BaseModel):
    """Strict OGC API - Processes results envelope."""

    outputs: list[OGCOutputValue | OGCOutputReference] = Field(default_factory=list)


class OGCJobResultsExtended(OGCJobResultsResponse):
    """Extended OGC results with native metadata."""

    metadata: dict[str, Any] | None = Field(default=None)
