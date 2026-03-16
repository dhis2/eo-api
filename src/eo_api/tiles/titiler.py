from titiler.xarray.factory import TilerFactory
from titiler.xarray.io import Reader 
from titiler.xarray.extensions import VariablesExtension

# Xarray-backed TiTiler endpoints using zarr reader
tiles_router = TilerFactory(
    reader=Reader,
    extensions=[VariablesExtension()]
).router
