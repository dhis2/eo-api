"""Root API endpoints."""

import asyncio
import json
import os
import sys
import urllib.parse
from collections.abc import AsyncIterator
from importlib.metadata import version as _pkg_version
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from starlette.responses import RedirectResponse, StreamingResponse

from open_climate_service import config as api_config

from .schemas import AppInfo, HealthStatus, Status
from .templates import (
    ROOT_RESPONSES,
    app_version,
    render_landing,
    render_manage,
    render_maps,
    wants_json,
)

router = APIRouter()


async def _sse_events(queue: asyncio.Queue[dict[str, Any] | None]) -> AsyncIterator[str]:
    while True:
        item = await queue.get()
        if item is None:
            break
        yield f"data: {json.dumps(item)}\n\n"


@router.get("/", response_class=Response, responses=ROOT_RESPONSES)
def read_index(request: Request) -> Response:
    """Return openEO capabilities (JSON) or the landing page (HTML)."""
    import os

    # Use CLIMATE_SERVICE_BASE_URL when set so links are correct behind a reverse proxy.
    base = os.getenv("CLIMATE_SERVICE_BASE_URL", "").rstrip("/") or str(request.base_url).rstrip("/")
    if wants_json(request):
        from open_climate_service.openeo.capabilities import build_capabilities

        caps = build_capabilities(base)
        return JSONResponse(caps.model_dump())
    return HTMLResponse(render_landing(app_version, base))


@router.get("/map", response_class=HTMLResponse, include_in_schema=False)
def maps(request: Request) -> HTMLResponse:
    """Return the interactive map viewer."""
    base = str(request.base_url).rstrip("/")
    return HTMLResponse(render_maps(base))


@router.get("/openeo", response_class=HTMLResponse, include_in_schema=False)
def openeo_editor(request: Request) -> RedirectResponse:
    """Redirect to the openEO Web Editor pre-connected to this backend."""
    base = os.getenv("CLIMATE_SERVICE_BASE_URL", "").rstrip("/") or str(request.base_url).rstrip("/")
    params = urllib.parse.urlencode({"server": base, "server-title": api_config.get_name()})
    return RedirectResponse(f"https://editor.openeo.org/?{params}", status_code=302)


@router.get("/manage", response_class=HTMLResponse, include_in_schema=False)
def manage(
    request: Request,
    message: str | None = None,
    error: str | None = None,
) -> HTMLResponse:
    """Return the management interface for ingestion and sync operations."""
    base = str(request.base_url).rstrip("/")
    return HTMLResponse(render_manage(app_version, base, message=message, error=error))


@router.post("/manage/ingest", include_in_schema=False)
async def manage_ingest(request: Request) -> Response:
    """Handle ingest form submission and stream progress via SSE."""
    from fastapi import HTTPException

    from open_climate_service.data_registry.services.datasets import get_dataset
    from open_climate_service.extents.services import get_extent_or_404
    from open_climate_service.ingestions.services import create_artifact

    base = str(request.base_url).rstrip("/")
    try:
        form = await request.form()
        dataset_id = str(form.get("dataset_id", ""))
        start = str(form.get("start", ""))
        end = str(form.get("end", "")) or None
        publish = "publish" in form
        overwrite = "overwrite" in form

        template = get_dataset(dataset_id)
        if template is None:
            msg = urllib.parse.quote(f"Dataset template '{dataset_id}' not found")
            return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)

        extent = get_extent_or_404()
        resolved_bbox = list(extent["bbox"])
        country_code = extent.get("country_code")
    except HTTPException as exc:
        msg = urllib.parse.quote(str(exc.detail))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)
    except Exception as exc:
        msg = urllib.parse.quote(str(exc))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)

    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def on_progress(done: int | None, total: int | None, message: str | None) -> None:
        loop.call_soon_threadsafe(
            queue.put_nowait,
            {"done": done, "total": total, "message": message},
        )

    async def run() -> None:
        try:
            await asyncio.to_thread(
                lambda: create_artifact(
                    dataset=template,
                    start=start,
                    end=end,
                    bbox=resolved_bbox,
                    country_code=country_code,
                    overwrite=overwrite,
                    publish=publish,
                    on_progress=on_progress,
                )
            )
            name = urllib.parse.quote(str(template.get("name", dataset_id)))
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"redirect": f"{base}/manage?message=Ingested+{name}"},
            )
        except HTTPException as exc:
            msg = urllib.parse.quote(str(exc.detail))
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"error": str(exc.detail), "redirect": f"{base}/manage?error={msg}"},
            )
        except Exception as exc:
            msg = urllib.parse.quote(str(exc))
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"error": str(exc), "redirect": f"{base}/manage?error={msg}"},
            )
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, None)

    asyncio.create_task(run())
    return StreamingResponse(_sse_events(queue), media_type="text/event-stream")


@router.post("/manage/sync", include_in_schema=False)
async def manage_sync(request: Request) -> Response:
    """Handle sync form submission and stream progress via SSE."""
    from fastapi import HTTPException

    from open_climate_service.ingestions.services import sync_dataset

    base = str(request.base_url).rstrip("/")
    try:
        form = await request.form()
        dataset_id = str(form.get("dataset_id", ""))
        publish = "publish" in form
    except HTTPException as exc:
        msg = urllib.parse.quote(str(exc.detail))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)
    except Exception as exc:
        msg = urllib.parse.quote(str(exc))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)

    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def on_progress(done: int | None, total: int | None, message: str | None) -> None:
        loop.call_soon_threadsafe(
            queue.put_nowait,
            {"done": done, "total": total, "message": message},
        )

    async def run() -> None:
        try:
            await asyncio.to_thread(
                lambda: sync_dataset(dataset_id=dataset_id, end=None, publish=publish, on_progress=on_progress)
            )
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"redirect": f"{base}/manage?message=Sync+completed"},
            )
        except HTTPException as exc:
            msg = urllib.parse.quote(str(exc.detail))
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"error": str(exc.detail), "redirect": f"{base}/manage?error={msg}"},
            )
        except Exception as exc:
            msg = urllib.parse.quote(str(exc))
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"error": str(exc), "redirect": f"{base}/manage?error={msg}"},
            )
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, None)

    asyncio.create_task(run())
    return StreamingResponse(_sse_events(queue), media_type="text/event-stream")


@router.get("/health")
def health() -> HealthStatus:
    """Return health status for container health checks."""
    return HealthStatus(status=Status.HEALTHY)


@router.get("/info")
def info() -> AppInfo:
    """Return application version and environment info."""
    return AppInfo(
        app_version=_pkg_version("open-climate-service"),
        python_version=sys.version,
        pygeoapi_version=_pkg_version("pygeoapi"),
        uvicorn_version=_pkg_version("uvicorn"),
    )
