"""WorldPop plugin for per-period streaming ingest."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

import xarray as xr

from open_climate_service.streaming import BaseDatasetPlugin, normalize_period

_WORLDPOP_NODATA = -99999.0


@dataclass(frozen=True)
class _WorldPopVariant:
    product: str
    source_variable: str
    output_variable: str
    fetch_module: str


class WorldPopYearlyPlugin(BaseDatasetPlugin):
    """Streaming plugin for yearly WorldPop country population rasters.

    Args:
        version: WorldPop release variant, e.g. ``global2``.
        product: WorldPop product selector. Currently only ``total`` is
            supported, but the resolver is structured so future
            disaggregations can be added without redesigning the plugin.
        variable: Output variable name written into the managed dataset.

    The default concurrency (1) and commit cadence (1) suit the one-request-per-year
    fetch, so no overrides are needed. The grid is inferred from the first fetched
    year, so no explicit ``probe`` is required.
    """

    def __init__(
        self, version: str = "global2", product: str = "total", variable: str = "pop_total", **_: object
    ) -> None:
        self.version = version
        self.variant = _resolve_variant(product=product, variable=variable)

    async def periods(self, start: str, end: str) -> list[str]:
        start_year = int(str(start)[:4])
        end_year = int(str(end)[:4])
        if start_year > end_year:
            return []
        return [str(year) for year in range(start_year, end_year + 1)]

    def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = bbox
        country_code = _required_country_code(params)
        dataset = self._fetch_year(int(period_id), country_code)
        # Mask the WorldPop sentinel (-99999) so the Zarr store uses NaN as the fill
        # value for unpopulated / ocean cells.
        return normalize_period(
            dataset,
            variable=self.variant.output_variable,
            source_variable=self.variant.source_variable,
            nodata=_WORLDPOP_NODATA,
        )

    def _fetch_year(self, year: int, country_code: str) -> xr.Dataset:
        from importlib import import_module

        module = import_module(self.variant.fetch_module)
        fetch_country_year = cast(Callable[[int, str, str], xr.Dataset], getattr(module, "fetch_country_year"))
        return fetch_country_year(year, country_code, self.version)


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
