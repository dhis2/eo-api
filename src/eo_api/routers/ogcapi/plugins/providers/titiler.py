"""TiTiler tile provider plugin for pygeoapi."""

from pygeoapi.provider.tile import BaseTileProvider

# https://github.com/geopython/pygeoapi/blob/master/pygeoapi/provider/wmts_facade.py
# http://127.0.0.1:8000/cog/tiles/WebMercatorQuad/4/5/5.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&colormap_name=delta


class TiTilerProvider(BaseTileProvider):
    """TiTiler Provider."""
