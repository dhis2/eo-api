"""Xarray-backed TiTiler router configuration for EO-API tile endpoints."""

from titiler.xarray.factory import TilerFactory  # pyright: ignore[reportMissingImports]
from titiler.xarray.io import Reader  # pyright: ignore[reportMissingImports]
from titiler.xarray.extensions import VariablesExtension  # pyright: ignore[reportMissingImports]

# Xarray-backed TiTiler endpoints using zarr reader
tiles_router = TilerFactory(
    reader=Reader,
    extensions=[VariablesExtension()]
).router
