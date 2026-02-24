
from metpy.units import units

def convert_units(ds, dataset):
    varname = dataset['variable']
    from_units = dataset['units']
    to_units = dataset.get('convertUnits')
    
    if to_units and to_units != from_units:
        print(f"Applying unit conversion from {from_units} to {to_units}...")
        # values with source units
        values_with_units = ds.values * units(from_units)
        # convert to target units
        converted = values_with_units.to(to_units).magnitude
        # update the dataframe
        ds.values = converted

    else:
        print("No unit conversion needed")
