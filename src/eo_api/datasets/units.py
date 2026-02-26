"""Unit conversion helpers for pandas DataFrames and xarray Datasets."""

import logging
from typing import Any

import xarray as xr
from metpy.units import units

logger = logging.getLogger(__name__)


def convert_pandas_units(ds: Any, dataset: dict[str, Any]) -> None:
    """Convert values in a pandas DataFrame column from source to target units."""
    varname = dataset["variable"]
    from_units = dataset["units"]
    to_units = dataset.get("convertUnits")

    if to_units and to_units != from_units:
        logger.info(f"Applying unit conversion from {from_units} to {to_units}...")
        values_with_units = ds[varname].values * units(from_units)
        converted = values_with_units.to(to_units).magnitude
        ds[varname] = converted
    else:
        logger.info("No unit conversion needed")


def convert_xarray_units(ds: xr.Dataset, dataset: dict[str, Any]) -> None:
    """Convert values in an xarray Dataset variable from source to target units."""
    varname = dataset["variable"]
    from_units = dataset["units"]
    to_units = dataset.get("convertUnits")

    if to_units and to_units != from_units:
        logger.info(f"Applying unit conversion from {from_units} to {to_units}...")
        values_with_units = ds[varname].values * units(from_units)
        converted = values_with_units.to(to_units).magnitude
        ds[varname].values = converted
    else:
        logger.info("No unit conversion needed")
