"""DHIS2 EO API - Earth observation data API for DHIS2.

load_dotenv() is called before pygeoapi import because pygeoapi
reads PYGEOAPI_CONFIG and PYGEOAPI_OPENAPI at import time.

Prefect UI env vars are set before any imports because Prefect
caches its settings on first import.
"""

import os
import warnings

os.environ.setdefault("PREFECT_UI_SERVE_BASE", "/prefect/")
os.environ.setdefault("PREFECT_UI_API_URL", "/prefect/api")
os.environ.setdefault("PREFECT_SERVER_API_BASE_PATH", "/prefect/api")
os.environ.setdefault("PREFECT_API_URL", "http://localhost:8000/prefect/api")
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT", "false")

warnings.filterwarnings("ignore", message="ecCodes .* or higher is recommended")

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from collections.abc import AsyncIterator  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402

from eo_api.routers import cog, ogcapi, pipelines, prefect, root  # noqa: E402


async def _serve_flows() -> None:
    """Register Prefect deployments and start a runner to execute them."""
    from prefect.runner import Runner

    from eo_api.prefect_flows.flows import ALL_FLOWS

    runner = Runner()
    for fl in ALL_FLOWS:
        await runner.aadd_flow(fl, name=fl.name)
    await runner.start()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start Prefect server, then register and serve pipeline deployments."""
    import asyncio

    # Mounted sub-apps don't get their lifespans called automatically,
    # so we trigger the Prefect server's lifespan here to initialize
    # the database, docket, and background workers.
    prefect_app = prefect.app
    async with prefect_app.router.lifespan_context(prefect_app):
        task = asyncio.create_task(_serve_flows())
        yield
        task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(root.router)
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])
app.include_router(pipelines.router, prefix="/pipelines", tags=["Pipelines"])
app.mount(path="/ogcapi", app=ogcapi.app)
app.mount(path="/", app=prefect.app)
