
from metpy.units import units

def convert_units(df, dataset):
    varname = dataset['variable']
    from_units = dataset['units']
    to_units = dataset.get('convertUnits')
    
    if to_units and to_units != from_units:
        print(f"Applying unit conversion from {from_units} to {to_units}...")
        # values with source units
        values_with_units = df[varname].values * units(from_units)
        # convert to target units
        converted = values_with_units.to(to_units).magnitude
        # update the dataframe
        df[varname] = converted

    else:
        print("No unit conversion needed")
