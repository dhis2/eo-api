
import numpy as np

def get_time_dim(ds):
    # get first available time dim
    time_dim = None
    for time_name in ['valid_time', 'time']:
        if hasattr(ds, time_name):
            time_dim = time_name
            break
    if time_dim is None:
        raise Exception(f'Unable to find time dimension: {ds.coordinates}')
    
    return time_dim
    
def get_lon_lat_dims(ds):
    # get first available spatial dim
    lat_dim = None
    lon_dim = None
    for lon_name,lat_name in [('lon','lat'), ('longitude','latitude'), ('x','y')]:
        if hasattr(ds, lat_name):
            lat_dim = lat_name
            lon_dim = lon_name
            break
    if lat_dim is None:
        raise Exception(f'Unable to find space dimension: {ds.coordinates}')

    return lon_dim, lat_dim

def numpy_period_string(t: np.datetime64, period_type: str) -> str:
    # convert numpy dateime to period string
    s = np.datetime_as_string(t, unit="s")

    if period_type == "hourly":
        return s[:13]        # YYYY-MM-DDTHH

    if period_type == "daily":
        return s[:10]        # YYYY-MM-DD

    if period_type == "monthly":
        return s[:7]         # YYYY-MM

    if period_type == "yearly":
        return s[:4]         # YYYY

    raise ValueError(f"Unknown periodType: {period_type}")

def numpy_period_array(t_array: np.ndarray, period_type: str) -> np.ndarray:
    # TODO: this and numpy_period_string should be merged
    # ...
    
    # Convert the whole array to strings at once
    s = np.datetime_as_string(t_array, unit="s")
    
    # Map periods to string lengths: YYYY-MM-DDTHH (13), YYYY-MM-DD (10), etc.
    lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
    return s.astype(f"U{lengths[period_type]}")

def pandas_period_string(column, period_type):
    if period_type == "hourly":
        return column.dt.strftime('%Y-%m-%dT%H')

    if period_type == "daily":
        return column.dt.strftime('%Y-%m-%d')

    if period_type == "monthly":
        return column.dt.strftime('%Y-%m')
    
    if period_type == "yearly":
        return column.dt.strftime('%Y')
    
    raise ValueError(f"Unknown periodType: {period_type}")
