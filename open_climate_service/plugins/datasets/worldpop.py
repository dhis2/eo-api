"""WorldPop plugin for per-period streaming ingest."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import xarray as xr

from open_climate_service.streaming.protocol import GridSpec


@dataclass(frozen=True)
class _WorldPopVariant:
    product: str
    source_variable: str
    output_variable: str
    fetch_module: str


class WorldPopYearlyPlugin:
    """Streaming plugin for yearly WorldPop country population rasters.

    Args:
        version: WorldPop release variant, e.g. ``global2``.
        product: WorldPop product selector. Currently only ``total`` is
            supported, but the resolver is structured so future
            disaggregations can be added without redesigning the plugin.
        variable: Output variable name written into the managed dataset.
    """

    max_concurrency = 1
    commit_batch_size = 1

    def __init__(self, version: str = "global2", product: str = "total", variable: str = "pop_total") -> None:
        self.version = version
        self.variant = _resolve_variant(product=product, variable=variable)

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        country_code = _required_country_code(params)
        dataset = self._normalize_dataset(await asyncio.to_thread(self._fetch_year, 2015, country_code))
        try:
            return GridSpec(
                shape=(int(dataset.sizes["y"]), int(dataset.sizes["x"])),
                crs=4326,
                dtype=np.dtype(dataset[self.variant.output_variable].dtype),
                nodata=None,
            )
        finally:
            dataset.close()

    async def periods(self, start: str, end: str) -> list[str]:
        start_year = int(str(start)[:4])
        end_year = int(str(end)[:4])
        if start_year > end_year:
            return []
        return [str(year) for year in range(start_year, end_year + 1)]

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = bbox
        country_code = _required_country_code(params)
        dataset = await asyncio.to_thread(self._fetch_year, int(period_id), country_code)
        return self._normalize_dataset(dataset)

    def _fetch_year(self, year: int, country_code: str) -> xr.Dataset:
        from importlib import import_module

        module = import_module(self.variant.fetch_module)
        fetch_country_year = cast(Callable[[int, str, str], xr.Dataset], getattr(module, "fetch_country_year"))
        return fetch_country_year(year, country_code, self.version)

    def _normalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        rename_map: dict[str, str] = {}
        if {"lon", "lat"} <= set(dataset.dims):
            rename_map.update({"lon": "x", "lat": "y"})
        for t_name in ("time", "valid_time"):
            if t_name in dataset.dims:
                rename_map[t_name] = "t"
                break
        if self.variant.source_variable != self.variant.output_variable:
            rename_map[self.variant.source_variable] = self.variant.output_variable
        if rename_map:
            dataset = dataset.rename(rename_map)
        # Mask the WorldPop sentinel nodata value (-99999) to NaN so the Zarr
        # store uses its native NaN fill_value for unpopulated / ocean cells.
        var = self.variant.output_variable
        if var in dataset:
            dataset[var] = dataset[var].where(dataset[var] != -99999)
        return dataset


def _resolve_variant(*, product: str, variable: str) -> _WorldPopVariant:
    if product == "total":
        return _WorldPopVariant(
            product="total",
            source_variable="pop_total",
            output_variable=variable,
            fetch_module="dhis2eo.data.worldpop.pop_total.yearly",
        )
    raise ValueError(f"Unsupported WorldPop product '{product}'")


def _required_country_code(params: dict[str, Any]) -> str:
    country_code = params.get("country_code")
    if not isinstance(country_code, str) or not country_code:
        raise ValueError("WorldPop streaming ingest requires country_code in plugin params")
    return country_code
