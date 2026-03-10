"""Schemas for zonal statistics processes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class ZonalStatisticsInput(BaseModel):
    """Inputs for raster zonal statistics process."""

    geojson: dict[str, Any] | str = Field(
        ...,
        description="GeoJSON FeatureCollection object or URI/path to GeoJSON file",
    )
    raster: str = Field(..., description="Raster path or URI")
    band: int = Field(default=1, ge=1, description="1-based raster band index")
    stats: list[str] = Field(
        default_factory=lambda: ["mean"],
        min_length=1,
        description="Statistics to compute",
    )
    feature_id_property: str = Field(default="id", description="Fallback property key for feature ID")
    output_property: str = Field(default="zonal_statistics", description="Property key where zonal stats are attached")
    all_touched: bool = Field(default=False)
    include_nodata: bool = Field(default=False)
    nodata: float | None = Field(default=None, description="Optional nodata override")

    @model_validator(mode="after")
    def validate_stats(self) -> "ZonalStatisticsInput":
        """Ensure all requested statistics are supported."""
        supported = {"count", "sum", "mean", "min", "max", "median", "std"}
        invalid = [stat for stat in self.stats if stat not in supported]
        if invalid:
            raise ValueError(f"Unsupported stats requested: {invalid}. Allowed stats: {sorted(supported)}")
        return self
