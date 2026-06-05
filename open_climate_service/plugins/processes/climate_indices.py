"""Curated overrides for xclim climate index processes.

The base layer of xclim processes is auto-registered by xclim_processes.scan().
This file only needs to override indicators where the auto-generated metadata
is insufficient.
"""

from typing import Any, cast

import xarray as xr
import xclim.indicators.atmos as xclim_atmos
import xclim.indices
from xclim.core.indicator import InputKind

from open_climate_service.process import process

_VARIABLE_SCHEMA: dict[str, Any] = {"type": "object", "subtype": "datacube"}


def _xclim_params(indicator: Any, *names: str) -> dict[str, dict[str, Any]]:
    """Read parameter descriptions and schemas from an xclim indicator."""
    result: dict[str, dict[str, Any]] = {}
    for name in names:
        if name not in indicator.parameters:
            continue
        p = indicator.parameters[name]
        entry: dict[str, Any] = {}
        if p.description:
            entry["description"] = p.description
        if p.kind in (InputKind.VARIABLE, InputKind.OPTIONAL_VARIABLE):
            entry["schema"] = _VARIABLE_SCHEMA
        result[name] = entry
    return result


_spi_ind = xclim_atmos.standardized_precipitation_index


@process(
    summary="Standardized Precipitation Index (SPI)",
    parameters={
        **_xclim_params(_spi_ind, "pr", "cal_start", "cal_end", "freq"),
        "window": {"description": "Accumulation window in months (1 = SPI-1, 3 = SPI-3, 6 = SPI-6)."},
    },
)
def spi(
    pr: xr.DataArray,
    window: int = 1,
    cal_start: str | None = None,
    cal_end: str | None = None,
    freq: str | None = "MS",
) -> xr.DataArray:
    """Standardized Precipitation Index.

    Computes SPI at a given accumulation timescale using a gamma distribution
    fitted to the calibration period. Values below -1 indicate drought conditions;
    values above +1 indicate wet conditions.
    """
    # DateStr is NewType(str) in xclim — cast to satisfy pyright without importing private types
    return xclim.indices.standardized_precipitation_index(  # type: ignore[no-any-return]
        pr,
        freq=freq,
        window=window,
        cal_start=cast(Any, cal_start),
        cal_end=cast(Any, cal_end),
    )
