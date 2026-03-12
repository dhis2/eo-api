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
