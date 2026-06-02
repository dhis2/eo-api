"""Climate extreme indices via xclim."""

from __future__ import annotations

import xarray as xr
import xclim.indices

from open_climate_service.process import process


@process(
    summary="Standardized Precipitation Index (SPI)",
    parameters={
        "pr": {"description": "Daily precipitation (kg m-2 s-1 or mm/day)."},
        "window": {"description": "Accumulation window in months (1 = SPI-1, 3 = SPI-3, 6 = SPI-6)."},
        "cal_start": {"description": "Calibration period start date (YYYY-MM-DD). Defaults to start of record."},
        "cal_end": {"description": "Calibration period end date (YYYY-MM-DD). Defaults to end of record."},
        "freq": {"description": "Output frequency. 'MS' = monthly (default). Use None if input is pre-aggregated."},
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
    return xclim.indices.standardized_precipitation_index(  # type: ignore[no-any-return]
        pr,
        freq=freq,
        window=window,
        cal_start=cal_start,
        cal_end=cal_end,
    )
