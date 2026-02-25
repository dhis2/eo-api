"""DHIS2 EO API - Earth observation data API for DHIS2.

load_dotenv() is called before pygeoapi import because pygeoapi
reads PYGEOAPI_CONFIG and PYGEOAPI_OPENAPI at import time.
"""

import logging
import warnings

warnings.filterwarnings("ignore", message="ecCodes .* or higher is recommended")

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402

from eo_api.pipelines.router import router as pipelines_router  # noqa: E402
from eo_api.routers import cog, ogcapi, root  # noqa: E402

logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(root.router)
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])
app.include_router(pipelines_router, prefix="/pipelines", tags=["Pipelines"])

try:
    from prefect.server.api.server import create_app as create_prefect_app

    prefect_app = create_prefect_app()
    prefect_app.root_path = "/prefect"
    app.mount(path="/prefect", app=prefect_app)
except Exception:
    logger.warning("Failed to mount Prefect server", exc_info=True)

app.mount(path="/ogcapi", app=ogcapi.app)
