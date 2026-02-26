"""Early-boot side effects: env vars, PROJ config, logging, dotenv, OpenAPI.

This module is imported before any other eo_api modules so that
environment variables and logging are configured before Prefect/pygeoapi
read them at import time.
"""

import logging
import os
import warnings
from importlib.util import find_spec
from pathlib import Path
from typing import Any, cast

# -- Prefect env-var defaults (must be set before Prefect is imported) --------
os.environ.setdefault("PREFECT_UI_SERVE_BASE", "/prefect/")
os.environ.setdefault("PREFECT_UI_API_URL", "/prefect/api")
os.environ.setdefault("PREFECT_SERVER_API_BASE_PATH", "/prefect/api")
os.environ.setdefault("PREFECT_API_URL", "http://localhost:8000/prefect/api")
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT", "false")


# -- PROJ data configuration --------------------------------------------------
def _configure_proj_data() -> None:
    """Point PROJ to rasterio bundled data to avoid mixed-install conflicts."""
    spec = find_spec("rasterio")
    if spec is None or spec.origin is None:
        return

    proj_data = Path(spec.origin).parent / "proj_data"
    if not proj_data.is_dir():
        return

    proj_data_path = str(proj_data)
    os.environ["PROJ_DATA"] = proj_data_path
    os.environ["PROJ_LIB"] = proj_data_path


_configure_proj_data()

# -- Warning filters ---------------------------------------------------------
warnings.filterwarnings("ignore", message="ecCodes .* or higher is recommended")
warnings.filterwarnings("ignore", message=r"Engine 'cfgrib' loading failed:[\s\S]*", category=RuntimeWarning)

# -- Silence noisy third-party loggers early ----------------------------------
logging.getLogger("pygeoapi.api.processes").setLevel(logging.ERROR)
logging.getLogger("pygeoapi.l10n").setLevel(logging.ERROR)

# -- Load .env (must happen before pygeoapi reads PYGEOAPI_CONFIG) ------------
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

# -- Generate missing OpenAPI document ----------------------------------------
openapi_path = os.getenv("PYGEOAPI_OPENAPI")
config_path = os.getenv("PYGEOAPI_CONFIG")
if openapi_path and config_path and not Path(openapi_path).exists():
    from pygeoapi.openapi import generate_openapi_document  # noqa: E402

    with Path(config_path).open(encoding="utf-8") as config_file:
        openapi_doc = generate_openapi_document(
            config_file,
            output_format=cast(Any, "yaml"),
            fail_on_invalid_collection=False,
        )
    Path(openapi_path).write_text(openapi_doc, encoding="utf-8")
    warnings.warn(f"Generated missing OpenAPI document at '{openapi_path}'.", RuntimeWarning)

# -- eo_api / third-party logging setup ---------------------------------------
eo_logger = logging.getLogger("eo_api")
eo_logger.setLevel(logging.INFO)
if not eo_logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    eo_logger.addHandler(handler)
eo_logger.propagate = False

logging.getLogger("dhis2eo").setLevel(logging.WARNING)
logging.getLogger("xarray").setLevel(logging.WARNING)
