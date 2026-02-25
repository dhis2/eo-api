"""DHIS2 EO API - Earth observation data API for DHIS2.

load_dotenv() is called before pygeoapi import because pygeoapi
reads PYGEOAPI_CONFIG and PYGEOAPI_OPENAPI at import time.

Prefect UI env vars are set before any imports because Prefect
caches its settings on first import.
"""

import logging
import os
import warnings
from importlib.util import find_spec
from pathlib import Path

os.environ.setdefault("PREFECT_UI_SERVE_BASE", "/prefect/")
os.environ.setdefault("PREFECT_UI_API_URL", "/prefect/api")
os.environ.setdefault("PREFECT_SERVER_API_BASE_PATH", "/prefect/api")
os.environ.setdefault("PREFECT_API_URL", "http://localhost:8000/prefect/api")
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT", "false")


def _configure_proj_data() -> None:
    """Point PROJ to rasterio bundled data to avoid mixed-install conflicts."""
    spec = find_spec("rasterio")
    if spec is None or spec.origin is None:
        return

    proj_data = Path(spec.origin).parent / "proj_data"
    if not proj_data.is_dir():
        return

    proj_data_path = str(proj_data)
    os.environ["PROJ_DATA"] = proj_data_path
    os.environ["PROJ_LIB"] = proj_data_path


_configure_proj_data()

warnings.filterwarnings("ignore", message="ecCodes .* or higher is recommended")

logging.getLogger("pygeoapi.api.processes").setLevel(logging.ERROR)
logging.getLogger("pygeoapi.l10n").setLevel(logging.ERROR)

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from collections.abc import AsyncIterator  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import RedirectResponse  # noqa: E402

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


@app.get("/ogcapi", include_in_schema=False)
async def ogcapi_redirect() -> RedirectResponse:
    """Redirect /ogcapi to /ogcapi/ for trailing-slash consistency."""
    return RedirectResponse(url="/ogcapi/")


app.mount(path="/ogcapi", app=ogcapi.app)
app.mount(path="/", app=prefect.app)
