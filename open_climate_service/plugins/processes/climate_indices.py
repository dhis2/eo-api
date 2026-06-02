"""Climate extreme indices via xclim."""

import xarray as xr
import xclim.indicators.atmos as xclim_atmos
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


@process(
    summary="Maximum consecutive dry days (CDD)",
    parameters={
        "pr": {"description": "Daily precipitation (kg m-2 s-1 or mm/day)."},
        "thresh": {"description": "Precipitation threshold below which a day is considered dry (e.g. '1 mm/day')."},
        "freq": {"description": "Resampling frequency. 'YS' = annual (default), 'MS' = monthly."},
    },
)
def cdd(
    pr: xr.DataArray,
    thresh: str = "1 mm/day",
    freq: str = "YS",
) -> xr.DataArray:
    """Maximum number of consecutive dry days per period.

    A dry day is one where precipitation is below the given threshold.
    CDD is one of the ETCCDI core climate extreme indices.
    """
    return xclim_atmos.maximum_consecutive_dry_days(  # type: ignore[no-any-return]
        pr, thresh=thresh, freq=freq
    )


@process(
    summary="Maximum consecutive wet days (CWD)",
    parameters={
        "pr": {"description": "Daily precipitation (kg m-2 s-1 or mm/day)."},
        "thresh": {"description": "Precipitation threshold for a wet day (e.g. '1 mm/day')."},
        "freq": {"description": "Resampling frequency. 'YS' = annual (default), 'MS' = monthly."},
    },
)
def cwd(
    pr: xr.DataArray,
    thresh: str = "1 mm/day",
    freq: str = "YS",
) -> xr.DataArray:
    """Maximum number of consecutive wet days per period.

    A wet day is one where precipitation meets or exceeds the given threshold.
    CWD is one of the ETCCDI core climate extreme indices.
    """
    return xclim_atmos.maximum_consecutive_wet_days(  # type: ignore[no-any-return]
        pr, thresh=thresh, freq=freq
    )


@process(
    summary="Number of days with maximum temperature above threshold (TX days above)",
    parameters={
        "tasmax": {"description": "Daily maximum temperature (degC or K)."},
        "thresh": {"description": "Temperature threshold (e.g. '25 degC', '30 degC')."},
        "freq": {"description": "Resampling frequency. 'YS' = annual (default), 'MS' = monthly."},
    },
)
def tx_days_above(
    tasmax: xr.DataArray,
    thresh: str = "25 degC",
    freq: str = "YS",
) -> xr.DataArray:
    """Number of days per period where daily maximum temperature exceeds a threshold.

    Commonly used for heat stress assessment. The threshold is a parameter,
    covering TX28°C through TX40°C variants with a single process.
    """
    return xclim_atmos.tx_days_above(  # type: ignore[no-any-return]
        tasmax, thresh=thresh, freq=freq
    )
