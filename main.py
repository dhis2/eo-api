from fastapi import FastAPI
from titiler.core.factory import (TilerFactory)
from rio_tiler.io import STACReader
from eoapi.endpoints.collections import router as collections_router
from eoapi.endpoints.conformance import router as conformance_router
from eoapi.endpoints.root import router as root_router

from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

# Bsed on: 
# https://developmentseed.org/titiler/user_guide/getting_started/#4-create-your-titiler-application
# https://github.com/developmentseed/titiler/blob/main/src/titiler/application/titiler/application/main.py

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (for development - be more specific in production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create a TilerFactory for Cloud-Optimized GeoTIFFs
cog = TilerFactory()

app.include_router(root_router)
app.include_router(conformance_router)
app.include_router(collections_router)

# Register all the COG endpoints automatically
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])
