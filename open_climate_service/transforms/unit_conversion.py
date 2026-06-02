"""Unit conversion transforms: named functions for common unit conversions."""

import logging
from typing import Any

import xarray as xr

logger = logging.getLogger(__name__)


def _apply(ds: xr.Dataset, dataset: dict[str, Any], *, scale: float, offset: float, units: str) -> xr.Dataset:
    varname = dataset["variable"]
    da = ds[varname]
    converted = da * scale + offset if scale != 1.0 else da + offset
    return ds.assign({varname: converted.assign_attrs({**da.attrs, "units": units})})


def kelvin_to_celsius(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    """Convert the dataset variable from Kelvin to degrees Celsius."""
    logger.info("Converting '%s' from K to °C", dataset["variable"])
    return _apply(ds, dataset, scale=1.0, offset=-273.15, units="degC")


def metres_to_mm(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    """Convert the dataset variable from metres to millimetres."""
    logger.info("Converting '%s' from m to mm", dataset["variable"])
    return _apply(ds, dataset, scale=1000.0, offset=0.0, units="mm")
