"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from eo_api.artifacts import routes as artifact_routes
from eo_api.collections import routes as collection_routes
from eo_api.data_accessor import routes as data_accessor_routes
from eo_api.data_manager import routes as data_manager_routes
from eo_api.data_registry import routes as data_registry_routes
from eo_api.pygeoapi_app import mount_pygeoapi
from eo_api.system import routes as system_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system_routes.router, tags=["System"])
app.include_router(data_registry_routes.router, prefix="/registry", tags=["Data registry"])
app.include_router(data_manager_routes.router, prefix="/manage", tags=["Data manager"])
app.include_router(data_accessor_routes.router, prefix="/retrieve", tags=["Data retrieval"])
app.include_router(artifact_routes.ingestions_router, prefix="/ingestions", tags=["Ingestions"])
app.include_router(artifact_routes.router, prefix="/artifacts", tags=["Artifacts"])
app.include_router(collection_routes.router, prefix="/collections", tags=["Collections"])

mount_pygeoapi(app)
