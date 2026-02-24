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

from pygeoapi.starlette_app import APP as pygeoapi_app

# pygeoapi exposes a ready-made Starlette app; we re-export it so the
# main application can mount it with app.mount().
app = pygeoapi_app
