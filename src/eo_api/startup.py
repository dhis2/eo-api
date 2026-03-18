"""Early-boot side effects.

This module is imported before any other eo_api modules so that
environment variables and logging are configured before other imports.
"""

import logging
import os

from dotenv import load_dotenv  # noqa: E402

# -- Load .env (must happen before pygeoapi reads PYGEOAPI_CONFIG) ------------
load_dotenv()

# -- eo_api / third-party logging setup ---------------------------------------
eo_logger = logging.getLogger("eo_api")
eo_logger.setLevel(logging.INFO)
if not eo_logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    eo_logger.addHandler(handler)
eo_logger.propagate = False


def _configure_generated_pygeoapi() -> None:
    """Materialize publication-driven pygeoapi documents before pygeoapi import."""
    from eo_api.publications.pygeoapi import write_generated_pygeoapi_documents

    server_url = os.environ.get("PYGEOAPI_SERVER_URL", "http://127.0.0.1:8000/ogcapi")
    config_path, openapi_path = write_generated_pygeoapi_documents(server_url=server_url)
    os.environ["PYGEOAPI_CONFIG"] = str(config_path)
    os.environ["PYGEOAPI_OPENAPI"] = str(openapi_path)
    eo_logger.info("Configured generated pygeoapi documents: %s %s", config_path, openapi_path)


_configure_generated_pygeoapi()
