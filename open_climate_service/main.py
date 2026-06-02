"""Open Climate Service -- Climate and earth observation data API for DHIS2."""

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

import open_climate_service.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from open_climate_service.data_registry import routes as dataset_template_routes
from open_climate_service.extents import routes as extent_routes
from open_climate_service.ingestions import routes as ingestion_routes
from open_climate_service.jobs import routes as job_routes
from open_climate_service.jobs.service import get_job_service
from open_climate_service.openeo import routes as openeo_routes
from open_climate_service.openeo.jobs import get_openeo_job_service
from open_climate_service.processing import routes as processing_routes
from open_climate_service.pygeoapi_app import mount_pygeoapi
from open_climate_service.stac import routes as stac_routes
from open_climate_service.system import routes as system_routes


def _zarr_browser_access_origins() -> set[str]:
    """Return allowlisted remote origins permitted to inspect local Zarr endpoints."""
    raw = os.getenv("CLIMATE_SERVICE_ZARR_BROWSER_ORIGINS", "https://inspect.geozarr.org")
    return {origin.strip() for origin in raw.split(",") if origin.strip()}


def _pna_trusted_origins() -> set[str]:
    """Return origins allowed to make Private Network Access requests.

    Defaults to the openEO editor and the Zarr inspector.  Override with the
    CLIMATE_SERVICE_PNA_ORIGINS environment variable (comma-separated).
    """
    default = "https://editor.openeo.org," + os.getenv(
        "CLIMATE_SERVICE_ZARR_BROWSER_ORIGINS", "https://inspect.geozarr.org"
    )
    raw = os.getenv("CLIMATE_SERVICE_PNA_ORIGINS", default)
    return {o.strip() for o in raw.split(",") if o.strip()}


def _append_vary_value(response: Response, value: str) -> None:
    """Append one token to the Vary header without clobbering existing values."""
    existing = response.headers.get("Vary")
    if existing is None:
        response.headers["Vary"] = value
        return

    values = [item.strip() for item in existing.split(",") if item.strip()]
    if value not in values:
        response.headers["Vary"] = ", ".join([*values, value])


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Run lightweight startup recovery hooks for the application lifecycle."""
    job_service = get_job_service()
    job_service.recover_pending_jobs()
    openeo_service = get_openeo_job_service()
    openeo_service.recover_pending_jobs()
    try:
        yield
    finally:
        job_service.shutdown()
        openeo_service.shutdown()


def create_app() -> FastAPI:
    """Create and configure the Open Climate Service FastAPI application.

    This is the public entry point for embedding the API in a larger application:

        from open_climate_service.main import create_app
        app = create_app()
    """
    _app = FastAPI(lifespan=_lifespan)

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @_app.middleware("http")
    async def add_browser_access_headers(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Handle Private Network Access preflights and Zarr browser CORS headers.

        Chrome's Private Network Access policy blocks public-origin pages (e.g.
        editor.openeo.org or inspect.geozarr.org) from fetching localhost resources
        unless the server explicitly opts in via Access-Control-Allow-Private-Network.
        We grant this for any origin that is already allowed by the CORS wildcard,
        which covers both the openEO editor and remote Zarr inspectors.
        """
        origin = request.headers.get("origin")
        is_pna_preflight = (
            request.method == "OPTIONS"
            and request.headers.get("access-control-request-private-network") == "true"
            and origin is not None
        )

        # Short-circuit PNA preflight for trusted origins (openEO editor, Zarr inspectors).
        if is_pna_preflight and origin in _pna_trusted_origins():
            response = Response(status_code=200)
            response.headers["Access-Control-Allow-Origin"] = str(origin)
            response.headers["Access-Control-Allow-Private-Network"] = "true"
            response.headers["Access-Control-Allow-Methods"] = request.headers.get(
                "access-control-request-method", "GET, POST, OPTIONS"
            )
            response.headers["Access-Control-Allow-Headers"] = request.headers.get(
                "access-control-request-headers", "*"
            )
            response.headers["Vary"] = "Origin"
            return response

        response = await call_next(request)

        # Extra CORS + PNA headers for Zarr inspector origins on /zarr paths.
        allowed_zarr_origin = origin if origin in _zarr_browser_access_origins() else None
        if allowed_zarr_origin and (request.url.path == "/zarr" or request.url.path.startswith("/zarr/")):
            response.headers["Access-Control-Allow-Origin"] = allowed_zarr_origin
            _append_vary_value(response, "Origin")
            response.headers.setdefault("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
            response.headers.setdefault(
                "Access-Control-Allow-Headers",
                request.headers.get("access-control-request-headers", "*"),
            )
            response.headers["Access-Control-Allow-Private-Network"] = "true"

        return response

    _app.include_router(system_routes.router, tags=["System"])
    _app.include_router(openeo_routes.capabilities_router, tags=["openEO"])
    _app.include_router(stac_routes.router, prefix="/stac", tags=["STAC"])
    _app.include_router(openeo_routes.collections_router, prefix="/collections", tags=["openEO"])
    _app.include_router(openeo_routes.jobs_router, prefix="/jobs", tags=["openEO"])
    _app.include_router(openeo_routes.udp_router, prefix="/process_graphs", tags=["openEO"])
    _app.include_router(openeo_routes.result_router, prefix="/result", tags=["openEO"])
    _app.include_router(extent_routes.router, prefix="/extent", tags=["Extent"])
    _app.include_router(dataset_template_routes.router, prefix="/dataset-templates", tags=["Dataset templates"])
    _app.include_router(ingestion_routes.datasets_router, prefix="/datasets", tags=["Datasets"])
    _app.include_router(ingestion_routes.ingestions_router, prefix="/ingestions", tags=["Ingestions"])
    _app.include_router(ingestion_routes.zarr_router, prefix="/zarr", tags=["Zarr"])
    _app.include_router(ingestion_routes.sync_router, prefix="/sync", tags=["Sync"])
    _app.include_router(openeo_routes.processes_router, prefix="/processes", tags=["openEO"])
    _app.include_router(processing_routes.router, prefix="/processes", tags=["Processes"])
    # Internal job tracker for native /processes execution (separate from the openEO jobs API at /jobs).
    _app.include_router(job_routes.router, prefix="/internal/jobs", tags=["Internal"])

    mount_pygeoapi(_app)

    return _app


app = create_app()
