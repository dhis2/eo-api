"""WorldPop plugin for per-period streaming ingest.

Fetches WorldPop's yearly population GeoTIFFs directly from the public
``data.worldpop.org`` hub (no auth) and streams them through ``normalize_period`` —
the same read-remote-raster pattern as the CHIRPS3 plugin. The grid is inferred
from the first fetched year.

Two plugins, both Global2 with a per-revision id (the hub reissues yearly, currently
``R2025A``):

- ``WorldPopYearlyPlugin`` — population counts (hub category id=3). The URL builder
  also knows the 1 km (``1km_ua``) and unconstrained layouts, but only the 100 m
  constrained template ships today.
- ``WorldPopAgeSexYearlyPlugin`` — age/sex structures (hub category id=8): ~40
  per-(sex, age) rasters per country-year, combined lazily into one cube
  (``population_female`` / ``population_male`` over an ``age_group`` dimension), 100 m
  per-country only.

Global 1 km mosaics are not yet supported (they are not COGs — see
https://github.com/dhis2/open-climate-service/issues/269).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import xarray as xr

from open_climate_service.streaming import BaseDatasetPlugin, normalize_period

_WORLDPOP_NODATA = -99999.0
_HUB = "https://data.worldpop.org/GIS"
# Global2 covers 2015–2030; clamp requested periods so we never build URLs for
# years the hub doesn't publish (out-of-range requests would 404 at ingest).
_GLOBAL2_FIRST_YEAR, _GLOBAL2_LAST_YEAR = 2015, 2030


def _global2_years(start: str, end: str) -> list[str]:
    start_year = max(int(str(start)[:4]), _GLOBAL2_FIRST_YEAR)
    end_year = min(int(str(end)[:4]), _GLOBAL2_LAST_YEAR)
    if start_year > end_year:
        return []
    return [str(year) for year in range(start_year, end_year + 1)]


# WorldPop age/sex 5-year bands (lower bound of each band), as the filename tokens.
_AGE_BANDS = (
    "00",
    "01",
    "05",
    "10",
    "15",
    "20",
    "25",
    "30",
    "35",
    "40",
    "45",
    "50",
    "55",
    "60",
    "65",
    "70",
    "75",
    "80",
    "85",
    "90",
)
# Output variable -> WorldPop sex filename token. The t_* / T_F / T_M totals are
# redundant sums of these, so we skip them.
_AGESEX_VARS = {"population_female": "f", "population_male": "m"}


@dataclass(frozen=True)
class _WorldPopVariant:
    product: str
    output_variable: str


class WorldPopYearlyPlugin(BaseDatasetPlugin):
    """Streaming plugin for yearly WorldPop country population rasters.

    WorldPop reissues the Global2 hub yearly under a new revision (currently
    ``R2025A``). We track one revision at a time; it, the resolution, and the
    constrained/unconstrained flavour are plugin parameters and belong in the
    dataset id, e.g. ``worldpop_population_global2_R2025A_100m``, so a later
    revision is a new dataset rather than a silent overwrite.

    Args:
        version: WorldPop release variant. Only ``global2`` (2015–2030) is supported.
        revision: Hub revision tag, e.g. ``R2025A``.
        resolution: ``100m`` (per-country constrained) or ``1km`` (per-country
            ``1km_ua``).
        constrained: Use the constrained (settlement-masked) product. Defaults to
            ``True``; ``False`` selects the unconstrained product.
        product: WorldPop product selector. Currently only ``total`` (population
            counts, id=3) is supported; the age/sex structures product (id=8) is
            handled separately.
        variable: Output variable name written into the managed dataset.

    The default concurrency (1) and commit cadence (1) suit the one-request-per-year
    fetch, so no overrides are needed. The grid is inferred from the first fetched year.
    """

    def __init__(
        self,
        version: str = "global2",
        revision: str = "R2025A",
        resolution: str = "100m",
        constrained: bool = True,
        product: str = "total",
        variable: str = "pop_total",
        **_: object,
    ) -> None:
        if version != "global2":
            raise ValueError(f"Unsupported WorldPop version '{version}'; only 'global2' is supported")
        self.version = version
        self.revision = revision
        self.resolution = resolution
        self.constrained = constrained
        self.variant = _resolve_variant(product=product, variable=variable)

    async def periods(self, start: str, end: str) -> list[str]:
        return _global2_years(start, end)

    def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        """Read one year's country GeoTIFF straight from the WorldPop hub.

        A regular (blocking) method — the framework runs it in a worker thread.
        """
        import rioxarray

        url = _population_url(
            int(period_id), _required_country_code(params), self.revision, self.resolution, self.constrained
        )
        da = rioxarray.open_rasterio(url, masked=True)
        if not isinstance(da, xr.DataArray):
            raise TypeError(f"Expected DataArray from WorldPop raster read, got {type(da).__name__}")
        # Mask the WorldPop sentinel (-99999) so the Zarr store uses NaN as the fill
        # value for unpopulated / ocean cells. No bbox clip: WorldPop publishes one
        # GeoTIFF per country, so we ingest the whole country rather than crop it to
        # the instance extent.
        return normalize_period(da, variable=self.variant.output_variable, period=period_id, nodata=_WORLDPOP_NODATA)


class WorldPopAgeSexYearlyPlugin(BaseDatasetPlugin):
    """Streaming plugin for WorldPop Global2 age/sex population structures (hub id=8).

    For each country-year WorldPop publishes one GeoTIFF per (sex, 5-year age band).
    This plugin combines them into a single store with two variables —
    ``population_female`` / ``population_male`` — each carrying an ordinal
    ``age_group`` dimension (the lower bound of the 5-year band: 0, 1, 5, 10, … 90).
    Only 100 m per-country is supported.

    Memory: each band is opened **lazily** (dask-chunked), so the ~40 rasters never
    all sit in memory — the orchestrator streams the combined cube to Zarr one year
    at a time. No bbox clip: the per-country rasters are ingested whole.
    """

    def __init__(
        self, revision: str = "R2025A", resolution: str = "100m", constrained: bool = True, **_: object
    ) -> None:
        if resolution != "100m":
            raise ValueError(f"WorldPop age/sex: only resolution '100m' is supported, got {resolution!r}")
        self.revision = revision
        self.resolution = resolution
        self.constrained = constrained

    async def periods(self, start: str, end: str) -> list[str]:
        return _global2_years(start, end)

    def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        """Combine the per-(sex, age) GeoTIFFs into one lazy ``(t, age_group, y, x)`` cube.

        A regular (blocking) method — the framework runs it in a worker thread.
        """
        import rioxarray  # noqa: F401  # registers the .rio accessor

        country_code = _required_country_code(params)
        year = int(period_id)
        ages = [int(a) for a in _AGE_BANDS]

        data_vars: dict[str, xr.DataArray] = {}
        for var_name, sex_token in _AGESEX_VARS.items():
            bands: list[xr.DataArray] = []
            for age in _AGE_BANDS:
                url = _agesex_url(year, country_code, self.revision, sex_token, age, self.constrained)
                da = rioxarray.open_rasterio(url, masked=True, chunks={})  # lazy — no data read yet
                if not isinstance(da, xr.DataArray):
                    raise TypeError(f"Expected DataArray from WorldPop raster read, got {type(da).__name__}")
                bands.append(da.squeeze("band", drop=True))
            data_vars[var_name] = xr.concat(bands, dim="age_group").assign_coords(age_group=ages)

        ds = xr.Dataset(data_vars).rio.write_crs("EPSG:4326")
        ds = ds.where(ds != _WORLDPOP_NODATA)  # mask the sentinel (lazy)
        return ds.expand_dims(t=[np.datetime64(str(year))])  # type: ignore[no-any-return]


def _resolve_variant(*, product: str, variable: str) -> _WorldPopVariant:
    if product == "total":
        return _WorldPopVariant(product="total", output_variable=variable)
    raise ValueError(f"Unsupported WorldPop product '{product}'; only 'total' (population counts) is supported")


def _required_country_code(params: dict[str, Any]) -> str:
    country_code = params.get("country_code")
    if not isinstance(country_code, str) or not country_code:
        raise ValueError("WorldPop streaming ingest requires country_code in plugin params")
    return country_code


def _population_url(year: int, country_code: str, revision: str, resolution: str, constrained: bool) -> str:
    """Build the public WorldPop hub population-counts GeoTIFF URL for one country-year.

    Verified Global2 layout (``data.worldpop.org/GIS/Population/Global_2015_2030``):
    - 100 m: ``{rev}/{year}/{CC}/v1/100m/{constrained}/{cc}_pop_{year}_{CN}_100m_{rev}_v1.tif``
    - 1 km:  ``{rev}/{year}/{CC}/v1/1km_ua/{constrained}/{cc}_pop_{year}_{CN}_1km_{rev}_UA_v1.tif``
    """
    cc_lower, cc_upper = country_code.lower(), country_code.upper()
    sub = "constrained" if constrained else "unconstrained"
    cn = "CN" if constrained else "UC"
    base = f"{_HUB}/Population/Global_2015_2030/{revision}/{year}/{cc_upper}/v1"
    if resolution == "100m":
        return f"{base}/100m/{sub}/{cc_lower}_pop_{year}_{cn}_100m_{revision}_v1.tif"
    if resolution == "1km":
        return f"{base}/1km_ua/{sub}/{cc_lower}_pop_{year}_{cn}_1km_{revision}_UA_v1.tif"
    raise ValueError(f"Unsupported WorldPop resolution '{resolution}'; expected '100m' or '1km'")


def _agesex_url(year: int, country_code: str, revision: str, sex: str, age: str, constrained: bool) -> str:
    """Build the WorldPop age/sex 100 m GeoTIFF URL for one country-year-sex-age band.

    Verified Global2 layout (``data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030``):
    ``{rev}/{year}/{CC}/v1/100m/{constrained}/{cc}_{sex}_{age}_{year}_{CN}_100m_{rev}_v1.tif``
    """
    cc_lower, cc_upper = country_code.lower(), country_code.upper()
    sub = "constrained" if constrained else "unconstrained"
    cn = "CN" if constrained else "UC"
    return (
        f"{_HUB}/AgeSex_structures/Global_2015_2030/{revision}/{year}/{cc_upper}/v1/100m/{sub}/"
        f"{cc_lower}_{sex}_{age}_{year}_{cn}_100m_{revision}_v1.tif"
    )
