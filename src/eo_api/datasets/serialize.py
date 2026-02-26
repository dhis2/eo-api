import io
import json
import logging
import os
import tempfile

import geopandas as gpd
from matplotlib.figure import Figure

from . import constants
from .utils import get_time_dim, numpy_period_array, pandas_period_string

logger = logging.getLogger(__name__)

def dataframe_to_json_data(df, dataset, period_type):
    time_dim = get_time_dim(df)
    varname = dataset['variable']

    # create smaller dataframe with known columns
    temp_df = df[[time_dim, "id", varname]].rename(columns={time_dim:'period', 'id':'orgunit', varname:'value'})

    # convert period string depending on period type
    temp_df['period'] = pandas_period_string(temp_df['period'], period_type)

    # convert to list of json dicts
    data = temp_df.to_dict(orient="records")

    # return
    return data

def dataframe_to_preview(df, dataset, period_type):
    logger.info('Generating dataframe map preview')
    time_dim = get_time_dim(df)
    varname = dataset['variable']

    # create smaller dataframe with known columns
    temp_df = df[[time_dim, "id", varname]]

    # convert period string depending on period type
    temp_df[time_dim] = pandas_period_string(temp_df[time_dim], period_type)

    # validate only one period
    assert len(temp_df[time_dim].unique()) == 1

    # merge with org units geojson
    org_units = gpd.read_file(json.dumps(constants.ORG_UNITS_GEOJSON))
    org_units_with_temp = org_units.merge(temp_df, on='id', how='left')

    # plot to map
    fig = Figure()
    ax = fig.subplots()
    period = temp_df[time_dim].values[0]
    org_units_with_temp.plot(ax=ax, column=varname, legend=True, legend_kwds={'label': varname})
    ax.set_title(f'{period}')

    # save to in-memory image
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)

    # return as image
    image_data = buf.getvalue()
    buf.close()
    return image_data

def xarray_to_preview(ds, dataset, period_type):
    logger.info('Generating xarray map preview')
    time_dim = get_time_dim(ds)
    varname = dataset['variable']

    # create smaller dataframe with known columns
    temp_ds = ds[[time_dim, varname]]

    # convert period string depending on period type
    temp_ds = temp_ds.assign_coords({
        time_dim: lambda x: numpy_period_array(x[time_dim].values, period_type)
    })

    # validate only one period
    assert len(temp_ds[time_dim].values) == 1

    # plot to map
    fig = Figure()
    ax = fig.subplots()
    period = temp_ds[time_dim].values[0]
    temp_ds[varname].plot(ax=ax)
    ax.set_title(f'{period}')

    # save to in-memory image
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)

    # return as image
    image_data = buf.getvalue()
    buf.close()
    return image_data

def xarray_to_temporary_netcdf(ds):
    # temporary file path
    path = tempfile.mktemp()

    # save to path
    ds.to_netcdf(path)

    # return
    return path

def cleanup_file(path: str):
    os.remove(path)
