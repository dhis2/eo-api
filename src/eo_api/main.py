"""DHIS2 EO API - Earth observation data API for DHIS2.

load_dotenv() is called before pygeoapi import because pygeoapi
reads PYGEOAPI_CONFIG and PYGEOAPI_OPENAPI at import time.
"""

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from pygeoapi.starlette_app import APP as pygeoapi_app  # noqa: E402

from eo_api.routers import cog, root  # noqa: E402

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
app.mount(path="/ogcapi", app=pygeoapi_app)
