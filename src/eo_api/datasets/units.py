import logging

from metpy.units import units

# logger
logger = logging.getLogger(__name__)

def convert_pandas_units(ds, dataset):
    varname = dataset['variable']
    from_units = dataset['units']
    to_units = dataset.get('convertUnits')

    if to_units and to_units != from_units:
        logger.info(f"Applying unit conversion from {from_units} to {to_units}...")
        # values with source units
        values_with_units = ds[varname].values * units(from_units)
        # convert to target units
        converted = values_with_units.to(to_units).magnitude
        # update the dataframe
        ds[varname] = converted

    else:
        logger.info("No unit conversion needed")

def convert_xarray_units(ds, dataset):
    varname = dataset['variable']
    from_units = dataset['units']
    to_units = dataset.get('convertUnits')

    if to_units and to_units != from_units:
        logger.info(f"Applying unit conversion from {from_units} to {to_units}...")
        # values with source units
        values_with_units = ds[varname].values * units(from_units)
        # convert to target units
        converted = values_with_units.to(to_units).magnitude
        # update the ds
        ds[varname].values = converted

    else:
        logger.info("No unit conversion needed")
