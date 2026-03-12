"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from . import system, components

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
#app.include_router(workflows.routes.router, prefix="/workflows", tags=["Workflows"])
app.include_router(components.routes.router, prefix="/components", tags=["Components"])

# component routes
app.include_router(components.data_registry.routes.router, prefix="/components/dataset-registry", tags=["Component: Data registry"])
app.include_router(components.download.routes.router, prefix="/components/download-dataset", tags=["Component: Data download"])
app.include_router(components.data_retrieval.routes.router, prefix="/components/query-data", tags=["Component: Data retrieval"])
app.include_router(components.features.routes.router, prefix="/components/feature-source", tags=["Component: Features"])
app.include_router(components.temporal_aggregation.routes.router, prefix="/components/temporal-aggregation", tags=["Component: Temporal aggregation"])
app.include_router(components.spatial_aggregation.routes.router, prefix="/components/spatial-aggregation", tags=["Component: Spatial aggregation"])
app.include_router(components.dhis2_datavalueset.routes.router, prefix="/components/build-datavalueset", tags=["Component: DHIS2 DataValueSet"])
