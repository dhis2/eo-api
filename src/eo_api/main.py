"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from eo_api import components, data_accessor, data_manager, data_registry, system, workflows
from eo_api.ogc import routes as ogc_routes
from eo_api.ogc_api import ogc_api_app
from eo_api.publications import generated_routes as publication_generated_routes
from eo_api.publications import routes as publication_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.routes.router, tags=["System"])
app.include_router(data_registry.routes.router, prefix="/registry", tags=["Data registry"])
app.include_router(data_manager.routes.router, prefix="/manage", tags=["Data manager"])
app.include_router(data_accessor.routes.router, prefix="/retrieve", tags=["Data retrieval"])
app.include_router(workflows.routes.router, prefix="/workflows", tags=["Workflows"])
app.include_router(publication_routes.router, prefix="/publications", tags=["Publications"])
app.include_router(publication_generated_routes.router, prefix="/publications", tags=["Publications"])
app.include_router(components.routes.router, tags=["Components"])
app.include_router(ogc_routes.router, prefix="/ogcapi", tags=["OGC API"])
app.mount("/data", StaticFiles(directory="data/downloads"), name="Data")
app.mount("/ogcapi", ogc_api_app)
