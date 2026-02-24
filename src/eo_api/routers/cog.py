"""Cloud Optimized GeoTIFF (COG) endpoints."""

from titiler.core.factory import TilerFactory

cog = TilerFactory()
router = cog.router
