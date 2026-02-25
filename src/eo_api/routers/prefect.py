"""Embedded Prefect server UI and API.

Mounts the Prefect server as a sub-application so the dashboard is
available at ``/prefect/`` alongside the main API.

The ``PREFECT_UI_SERVE_BASE`` and ``PREFECT_UI_API_URL`` env vars
must be set before Prefect is first imported (done in ``main.py``)
because Prefect caches settings on initial import.
"""

from prefect.server.api.server import create_app

app = create_app()
