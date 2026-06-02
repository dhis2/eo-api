"""pygeoapi mounting helpers."""

from __future__ import annotations

import importlib
import logging
import threading

from fastapi import FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send

from open_climate_service.publications.services import ensure_pygeoapi_base_config

logger = logging.getLogger(__name__)


class ReloadablePygeoapiApp:
    """ASGI wrapper that can reload the mounted pygeoapi app in-process."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._app: ASGIApp | None = None
        self.reload()

    def reload(self) -> None:
        """Rebuild the underlying pygeoapi Starlette app from current config."""
        ensure_pygeoapi_base_config()
        module = importlib.import_module("pygeoapi.starlette_app")
        module = importlib.reload(module)
        with self._lock:
            self._app = module.APP

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Delegate requests to the current pygeoapi app instance."""
        with self._lock:
            app = self._app
        if app is None:
            raise RuntimeError("pygeoapi app is not initialized")
        await app(scope, receive, send)


_pygeoapi_wrapper: ReloadablePygeoapiApp | None = None


def _get_wrapper() -> ReloadablePygeoapiApp:
    global _pygeoapi_wrapper
    if _pygeoapi_wrapper is None:
        _pygeoapi_wrapper = ReloadablePygeoapiApp()
    return _pygeoapi_wrapper


def mount_pygeoapi(app: FastAPI) -> None:
    """Mount pygeoapi if the dependency is available."""
    try:
        pygeoapi_app = _get_wrapper()
    except Exception as exc:
        logger.warning("pygeoapi mount skipped: %s", exc)
        return

    app.mount("/ogcapi", pygeoapi_app)


def refresh_pygeoapi() -> None:
    """Reload the mounted pygeoapi app after config changes."""
    if _pygeoapi_wrapper is None:
        return
    _pygeoapi_wrapper.reload()
