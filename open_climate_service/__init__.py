# The lightweight client is import-safe with only the base deps (httpx, pystac),
# so it can be re-exported at the top level without pulling in the server stack.
from open_climate_service.client import ClimateService

try:
    from importlib.metadata import version as _get_version

    __version__ = _get_version("open-climate-service")
except Exception:
    __version__ = "unknown"

__all__ = ["ClimateService", "__version__"]
