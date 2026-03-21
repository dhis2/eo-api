"""Mounted pygeoapi application with publication-aware runtime refresh."""

from __future__ import annotations

import asyncio
import importlib
import os
from types import ModuleType
from typing import Any

from starlette.types import Receive, Scope, Send

from ..publications.pygeoapi import write_generated_pygeoapi_documents

_STARLETTE_APP_MODULE = "pygeoapi.starlette_app"


class DynamicPygeoapiApp:
    """Refresh pygeoapi runtime documents before serving mounted requests.

    This keeps the mounted publication surface aligned with live publication
    truth without requiring an application restart after each publication
    change.
    """

    def __init__(self) -> None:
        self._module: ModuleType | None = None
        # pygeoapi keeps request handlers and config as module globals.
        # Serialize mounted requests so reloads cannot race with in-flight
        # requests and produce mixed old/new publication state.
        self._lock = asyncio.Lock()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async with self._lock:
            config_path, openapi_path = write_generated_pygeoapi_documents()
            os.environ["PYGEOAPI_CONFIG"] = str(config_path)
            os.environ["PYGEOAPI_OPENAPI"] = str(openapi_path)

            if self._module is None:
                self._module = importlib.import_module(_STARLETTE_APP_MODULE)
            else:
                self._module = importlib.reload(self._module)

            app = getattr(self._module, "APP")
            await app(scope, receive, send)


ogc_api_app: Any = DynamicPygeoapiApp()

__all__ = ["ogc_api_app"]
