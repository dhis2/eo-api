"""Early-boot side effects.

This module is imported before any other eo_api modules so that
environment variables and logging are configured before other imports.
"""

import logging

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
