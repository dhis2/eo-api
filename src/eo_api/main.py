"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from . import system, workflows, components

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# main routes
app.include_router(system.routes.router, tags=["System"])
app.include_router(workflows.routes.router, prefix="/workflows", tags=["Workflows"])
app.include_router(components.routes.router, prefix="/components", tags=["Components"])

# component routes
app.include_router(components.data_registry.routes.router, prefix="/components/registry", tags=["Data registry"])
app.include_router(components.download.routes.router, prefix="/components/download", tags=["Data download"])
app.include_router(components.data_retrieval.routes.router, prefix="/components/retrieve", tags=["Data retrieval"])
app.include_router(components.features.routes.router, prefix="/components/features", tags=["Features"])
app.include_router(components.temporal_aggregation.routes.router, prefix="/components/temporal_aggregation", tags=["Temporal aggregation"])
app.include_router(components.spatial_aggregation.routes.router, prefix="/components/spatial_aggregation", tags=["Spatial aggregation"])
app.include_router(components.dhis2_datavalueset.routes.router, prefix="/components/datavalueset", tags=["DHIS2 DataValueSet"])
