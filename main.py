import os
import logging
from urllib.parse import urlparse
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from titiler.core.factory import (TilerFactory)
from rio_tiler.io import STACReader
from eoapi.endpoints.collections import router as collections_router
from eoapi.endpoints.conformance import router as conformance_router
from eoapi.endpoints.features import router as features_router
from eoapi.endpoints.processes import router as processes_router
from eoapi.endpoints.root import router as root_router
from eoapi.endpoints.schedules import router as schedules_router
from eoapi.endpoints.workflows import router as workflows_router
from eoapi.dhis2_integration import dhis2_configured
from eoapi.scheduler_runtime import start_internal_scheduler, stop_internal_scheduler
from eoapi.state_store import STATE_DIR_ENV, STATE_PERSIST_ENV

from starlette.middleware.cors import CORSMiddleware

app = FastAPI()
logger = logging.getLogger(__name__)


def _cors_origins() -> list[str]:
    raw = os.getenv("EOAPI_CORS_ORIGINS", "*").strip()
    if not raw:
        return ["*"]
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _api_key_required() -> str | None:
    token = os.getenv("EOAPI_API_KEY", "").strip()
    return token or None


def _internal_scheduler_enabled() -> bool:
    raw = os.getenv("EOAPI_INTERNAL_SCHEDULER_ENABLED", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _internal_scheduler_poll_seconds() -> float:
    raw = os.getenv("EOAPI_INTERNAL_SCHEDULER_POLL_SECONDS", "30").strip()
    try:
        value = float(raw)
    except ValueError:
        value = 30.0
    return value if value > 0 else 30.0


def _state_persistence_enabled() -> bool:
    raw = os.getenv(STATE_PERSIST_ENV, "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _state_directory() -> str:
    return os.getenv(STATE_DIR_ENV, ".cache/state").strip() or ".cache/state"


def _dhis2_auth_mode() -> str:
    if os.getenv("EOAPI_DHIS2_TOKEN", "").strip():
        return "token"

    if os.getenv("EOAPI_DHIS2_USERNAME", "").strip() and os.getenv("EOAPI_DHIS2_PASSWORD", "").strip():
        return "basic"

    return "none"


def _dhis2_host() -> str:
    base = os.getenv("EOAPI_DHIS2_BASE_URL", "").strip()
    if not base:
        return "unset"

    parsed = urlparse(base)
    return parsed.netloc or parsed.path or "configured"


def _log_startup_configuration() -> None:
    cors = _cors_origins()
    logger.info(
        "Startup config: cors=%s apiKeyRequired=%s",
        "*" if cors == ["*"] else f"{len(cors)} origins",
        _api_key_required() is not None,
    )
    logger.info(
        "Startup config: dhis2 configured=%s host=%s authMode=%s",
        dhis2_configured(),
        _dhis2_host(),
        _dhis2_auth_mode(),
    )
    logger.info(
        "Startup config: statePersistence=%s stateDir=%s",
        _state_persistence_enabled(),
        _state_directory(),
    )
    logger.info(
        "Startup config: internalScheduler=%s pollSeconds=%s",
        _internal_scheduler_enabled(),
        _internal_scheduler_poll_seconds(),
    )

# Bsed on: 
# https://developmentseed.org/titiler/user_guide/getting_started/#4-create-your-titiler-application
# https://github.com/developmentseed/titiler/blob/main/src/titiler/application/titiler/application/main.py

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _optional_api_key_guard(request: Request, call_next):
    expected = _api_key_required()
    if expected is None:
        return await call_next(request)

    if request.method in {"POST", "PATCH", "DELETE", "PUT"}:
        provided = request.headers.get("X-API-Key", "")
        if provided != expected:
            return JSONResponse(
                status_code=403,
                content={
                    "detail": {
                        "code": "Forbidden",
                        "description": "Invalid or missing API key",
                    }
                },
            )

    return await call_next(request)

# Create a TilerFactory for Cloud-Optimized GeoTIFFs
cog = TilerFactory()

app.include_router(root_router)
app.include_router(conformance_router)
app.include_router(collections_router)
app.include_router(features_router)
app.include_router(processes_router)
app.include_router(workflows_router)
app.include_router(schedules_router)

# Register all the COG endpoints automatically
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])

webapp_dir = Path(__file__).resolve().parent / "webapp"
app.mount("/example-app", StaticFiles(directory=str(webapp_dir), html=True), name="example-app")


@app.on_event("startup")
def _on_startup() -> None:
    _log_startup_configuration()
    start_internal_scheduler()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    stop_internal_scheduler()
