"""Utility helpers for time and spatial dimension discovery and formatting."""

from typing import Any

import numpy as np
import pandas as pd


def get_time_dim(ds: Any) -> str:
    """Return the name of the time dimension in a dataset or dataframe."""
    for time_name in ["valid_time", "time"]:
        if hasattr(ds, time_name):
            return time_name
    raise ValueError(f"Unable to find time dimension: {ds.coordinates}")


def get_lon_lat_dims(ds: Any) -> tuple[str, str]:
    """Return ``(lon, lat)`` dimension names from a dataset."""
    for lon_name, lat_name in [("lon", "lat"), ("longitude", "latitude"), ("x", "y")]:
        if hasattr(ds, lat_name):
            return lon_name, lat_name
    raise ValueError(f"Unable to find space dimension: {ds.coordinates}")


def numpy_period_string(t: np.datetime64, period_type: str) -> str:
    """Convert a single numpy datetime to a period string."""
    s = np.datetime_as_string(t, unit="s")

    if period_type == "hourly":
        return s[:13]  # YYYY-MM-DDTHH

    if period_type == "daily":
        return s[:10]  # YYYY-MM-DD

    if period_type == "monthly":
        return s[:7]  # YYYY-MM

    if period_type == "yearly":
        return s[:4]  # YYYY

    raise ValueError(f"Unknown periodType: {period_type}")


def numpy_period_array(t_array: np.ndarray[Any, Any], period_type: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    # TODO: this and numpy_period_string should be merged
    s = np.datetime_as_string(t_array, unit="s")

    # Map periods to string lengths: YYYY-MM-DDTHH (13), YYYY-MM-DD (10), etc.
    lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
    return s.astype(f"U{lengths[period_type]}")


def pandas_period_string(column: 'pd.Series[Any]', period_type: str) -> 'pd.Series[Any]':
    """Format a pandas datetime column as period strings."""
    if period_type == "hourly":
        return column.dt.strftime("%Y-%m-%dT%H")  # type: ignore[no-any-return]

    if period_type == "daily":
        return column.dt.strftime("%Y-%m-%d")  # type: ignore[no-any-return]

    if period_type == "monthly":
        return column.dt.strftime("%Y-%m")  # type: ignore[no-any-return]

    if period_type == "yearly":
        return column.dt.strftime("%Y")  # type: ignore[no-any-return]

    raise ValueError(f"Unknown periodType: {period_type}")
