"""OGC API endpoints (pygeoapi).

Unlike titiler (configured in Python), pygeoapi is almost entirely
YAML-config-driven.  The config file is located via the ``PYGEOAPI_CONFIG``
environment variable and controls:

- **resources** -- datasets exposed as OGC API collections.
  Each resource declares a type (feature, coverage, map, process) and a
  provider that handles the backend I/O (e.g. Elasticsearch, PostGIS,
  rasterio, xarray).
- **server settings** -- gzip compression, CORS headers, response limits,
  language negotiation, and the optional admin API.
- **metadata** -- service identification, contact info, and license.
- **API rules** -- URL path encoding, property inclusion/exclusion, and
  custom API behaviour overrides.

Adding or changing datasets therefore means editing the YAML file, not
this module.

References:
----------
- Configuration guide: https://docs.pygeoapi.io/en/latest/configuration.html
- Data publishing:     https://docs.pygeoapi.io/en/latest/data-publishing/
"""

import logging

from pygeoapi.starlette_app import APP as pygeoapi_app
from pygeoapi.starlette_app import CONFIG

from eo_api.routers.ogcapi.plugins.dhis2_common import fetch_bbox

logger = logging.getLogger(__name__)

try:
    bbox = fetch_bbox()
    if bbox is not None:
        CONFIG["resources"]["dhis2-org-units"]["extents"]["spatial"]["bbox"] = [bbox]
        CONFIG["resources"]["dhis2-org-units-cql"]["extents"]["spatial"]["bbox"] = [bbox]
        logger.info("DHIS2 org-units bbox set to %s", bbox)
    else:
        logger.info("No level-1 org unit geometry found, skipping bbox")
except Exception:
    logger.warning("Failed to fetch DHIS2 bbox, using config default", exc_info=True)

# pygeoapi exposes a ready-made Starlette app; we re-export it so the
# main application can mount it with app.mount().
app = pygeoapi_app
