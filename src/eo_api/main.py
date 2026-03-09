"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from eo_api import data_manager, system

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.routes.router, tags=['System'])
app.include_router(data_manager.routes.router, prefix='/datasets', tags=['Data manager'])
