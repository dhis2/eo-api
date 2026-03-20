"""Early-boot side effects.

This module is imported before any other eo_api modules so that
environment variables and logging are configured before other imports.
"""

import logging
import os
import sys
from pathlib import Path

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


def _configure_proj_data() -> None:
    """Point PROJ at the active environment's data files."""
    candidates: list[Path] = []
    for sys_path in sys.path:
        if not sys_path:
            continue
        candidates.append(Path(sys_path) / "rasterio" / "proj_data")

    try:
        from pyproj import datadir

        pyproj_data_dir = datadir.get_data_dir()
        if pyproj_data_dir:
            candidates.append(Path(pyproj_data_dir))
    except Exception:
        pass

    for candidate in candidates:
        if candidate.exists():
            proj_path = str(candidate)
            os.environ["PROJ_LIB"] = proj_path
            os.environ["PROJ_DATA"] = proj_path
            eo_logger.info("Configured PROJ data directory: %s", proj_path)
            return

    eo_logger.warning("Could not locate a compatible PROJ data directory in the active environment")


def _configure_generated_pygeoapi() -> None:
    """Materialize publication-driven pygeoapi documents before pygeoapi import."""
    from eo_api.publications.pygeoapi import write_generated_pygeoapi_documents

    server_url = os.environ.get("PYGEOAPI_SERVER_URL", "http://127.0.0.1:8000/pygeoapi")
    config_path, openapi_path = write_generated_pygeoapi_documents(server_url=server_url)
    os.environ["PYGEOAPI_CONFIG"] = str(config_path)
    os.environ["PYGEOAPI_OPENAPI"] = str(openapi_path)
    eo_logger.info("Configured generated pygeoapi documents: %s %s", config_path, openapi_path)


_configure_proj_data()
_configure_generated_pygeoapi()
