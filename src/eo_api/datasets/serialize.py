"""Serialization of xarray/pandas data to JSON, PNG previews, and NetCDF files."""

import io
import json
import logging
import os
import tempfile
from typing import Any

import geopandas as gpd
import pandas as pd
import xarray as xr
from matplotlib.figure import Figure

from . import constants
from .utils import get_time_dim, numpy_period_array, pandas_period_string

logger = logging.getLogger(__name__)


def dataframe_to_json_data(df: pd.DataFrame, dataset: dict[str, Any], period_type: str) -> list[dict[str, Any]]:
    """Convert a DataFrame to a list of ``{period, orgunit, value}`` dicts."""
    time_dim = get_time_dim(df)
    varname = dataset["variable"]

    temp_df = df[[time_dim, "id", varname]].rename(columns={time_dim: "period", "id": "orgunit", varname: "value"})
    temp_df["period"] = pandas_period_string(temp_df["period"], period_type)

    return temp_df.to_dict(orient="records")  # type: ignore[return-value]


def dataframe_to_preview(df: pd.DataFrame, dataset: dict[str, Any], period_type: str) -> bytes:
    """Render a DataFrame as a choropleth PNG map image."""
    logger.info("Generating dataframe map preview")
    time_dim = get_time_dim(df)
    varname = dataset["variable"]

    temp_df = df[[time_dim, "id", varname]]
    temp_df[time_dim] = pandas_period_string(temp_df[time_dim], period_type)

    if len(temp_df[time_dim].unique()) != 1:
        raise ValueError("dataframe_to_preview expects exactly one timestep")

    org_units = gpd.read_file(json.dumps(constants.ORG_UNITS_GEOJSON))
    org_units_with_temp = org_units.merge(temp_df, on="id", how="left")

    fig = Figure()
    ax = fig.subplots()
    period = temp_df[time_dim].values[0]
    org_units_with_temp.plot(ax=ax, column=varname, legend=True, legend_kwds={"label": varname})
    ax.set_title(f"{period}")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)

    image_data = buf.getvalue()
    buf.close()
    return image_data


def xarray_to_preview(ds: xr.Dataset, dataset: dict[str, Any], period_type: str) -> bytes:
    """Render an xarray Dataset as a PNG map image."""
    logger.info("Generating xarray map preview")
    time_dim = get_time_dim(ds)
    varname = dataset["variable"]

    temp_ds = ds[[time_dim, varname]]
    temp_ds = temp_ds.assign_coords({time_dim: lambda x: numpy_period_array(x[time_dim].values, period_type)})

    if len(temp_ds[time_dim].values) != 1:
        raise ValueError("xarray_to_preview expects exactly one timestep")

    fig = Figure()
    ax = fig.subplots()
    period = temp_ds[time_dim].values[0]
    temp_ds[varname].plot(ax=ax)
    ax.set_title(f"{period}")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)

    image_data = buf.getvalue()
    buf.close()
    return image_data


def xarray_to_temporary_netcdf(ds: xr.Dataset) -> str:
    """Write a dataset to a temporary NetCDF file and return the path."""
    fd = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
    path = fd.name
    fd.close()
    try:
        ds.to_netcdf(path)
    except Exception:
        os.remove(path)
        raise
    return path


def cleanup_file(path: str) -> None:
    """Remove a file from disk."""
    os.remove(path)
