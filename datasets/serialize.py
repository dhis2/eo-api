import os
import tempfile

from dhis2eo.integrations.pandas import dataframe_to_dhis2_json

from .utils import get_time_dim

def dataframe_to_json_data(df, dataset, period_type):
    time_dim = get_time_dim(df)
    varname = dataset['variable']

    # create smaller dataframe with known columns
    temp_df = df[[time_dim, "id", varname]].rename(columns={time_dim:'period', 'id':'orgunit', varname:'value'})
    
    # convert period string depending on period type
    def convert_to_period_string(column, period_type):
        if period_type == "hourly":
            return column.dt.strftime('%Y-%m-%dT%H')

        if period_type == "daily":
            return column.dt.strftime('%Y-%m-%d')

        if period_type == "monthly":
            return column.dt.strftime('%Y-%m')
        
        if period_type == "yearly":
            return column.dt.strftime('%Y')
    temp_df['period'] = convert_to_period_string(temp_df['period'], period_type)

    # convert to list of json dicts
    data = temp_df.to_dict(orient="records")

    # return
    return data

def xarray_to_temporary_netcdf(ds):
    # temporary file path
    path = tempfile.mktemp()

    # save to path
    ds.to_netcdf(path)

    # return
    return path

def cleanup_file(path: str):
    os.remove(path)
