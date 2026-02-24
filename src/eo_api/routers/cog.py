"""Cloud Optimized GeoTIFF (COG) endpoints powered by titiler."""

from titiler.core.factory import TilerFactory

cog = TilerFactory(
    # router_prefix should match the prefix used in app.include_router()
    router_prefix="/cog",
    # Endpoints to register (all True by default except add_ogc_maps)
    add_preview=True,
    add_part=True,
    add_viewer=True,
    # GDAL environment variables applied to every request
    # environment_dependency=lambda: {
    #     "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    #     "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    # },
)

router = cog.router
