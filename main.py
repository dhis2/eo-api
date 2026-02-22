from fastapi import FastAPI
from titiler.core.factory import (TilerFactory,  MultiBaseTilerFactory)
from rio_tiler.io import STACReader

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

# Register all the COG endpoints automatically
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])

stac = MultiBaseTilerFactory(
    reader=STACReader,
    router_prefix="/stac",
    add_ogc_maps=True,
    # extensions=[stacViewerExtension(), stacRenderExtension(), wmtsExtension()],
    # enable_telemetry=api_settings.telemetry_enabled,
    # templates=titiler_templates,
)

app.include_router(
    stac.router,
    prefix="/stac",
    tags=["SpatioTemporal Asset Catalog"],
)

# Optional: Add a welcome message for the root endpoint
@app.get("/")
def read_index():
    return {"message": "Welcome to DHIS2 EO API"}