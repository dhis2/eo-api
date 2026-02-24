"""DHIS2 EO API - Earth observation data API for DHIS2."""

from dotenv import load_dotenv
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from titiler.core.factory import TilerFactory

load_dotenv()

from pygeoapi.starlette_app import APP as pygeoapi_app  # noqa: E402

app = FastAPI()

# Bsed on:
# https://docs.pygeoapi.io/en/stable/administration.html
# https://dive.pygeoapi.io/advanced/downstream-applications/#starlette-and-fastapi
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

# mount all pygeoapi endpoints to /ogcapi
app.mount(path="/ogcapi", app=pygeoapi_app)

# Create a TilerFactory for Cloud-Optimized GeoTIFFs
cog = TilerFactory()

# Register all the COG endpoints automatically
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])


# Optional: Add a welcome message for the root endpoint
@app.get("/")
def read_index() -> dict[str, str]:
    """Return a welcome message for the root endpoint."""
    return {"message": "Welcome to DHIS2 EO API"}
