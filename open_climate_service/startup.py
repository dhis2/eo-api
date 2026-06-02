"""Early-boot side effects.

This module is imported before any other open_climate_service modules so that
environment variables and logging are configured before other imports.
"""

import logging
import os

from dotenv import load_dotenv  # noqa: E402

# -- Load .env (must happen before pygeoapi reads PYGEOAPI_CONFIG) ------------
load_dotenv()

from open_climate_service.publications.services import PYGEOAPI_CONFIG_PATH, PYGEOAPI_OPENAPI_PATH  # noqa: E402

os.environ.setdefault("PYGEOAPI_CONFIG", str(PYGEOAPI_CONFIG_PATH))
os.environ.setdefault("PYGEOAPI_OPENAPI", str(PYGEOAPI_OPENAPI_PATH))

# -- open_climate_service / third-party logging setup ---------------------------------------
eo_logger = logging.getLogger("open_climate_service")
eo_logger.setLevel(logging.INFO)
if not eo_logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    eo_logger.addHandler(handler)
eo_logger.propagate = False
