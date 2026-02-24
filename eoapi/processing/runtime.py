"""Shared runtime primitives for process metadata and execution dispatch."""

from dataclasses import dataclass
from typing import Any, Callable

try:
    from pygeoapi.api import FORMAT_TYPES, F_JSON
    from pygeoapi.util import url_join
except ImportError:
    F_JSON = "json"
    FORMAT_TYPES = {F_JSON: "application/json"}

    def url_join(base_url: str, *parts: str) -> str:
        segments = [base_url.rstrip("/"), *(part.strip("/") for part in parts if part)]
        return "/".join(segment for segment in segments if segment)

from eoapi.endpoints.errors import not_found

DefinitionBuilder = Callable[[str], dict[str, Any]]
Executor = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class ProcessHandler:
    """Binds one process ID to its definition builder and executor."""

    process_id: str
    definition: DefinitionBuilder
    execute: Executor


class ProcessRuntime:
    """In-memory dispatcher used by `/processes*` endpoints."""

    def __init__(self, handlers: list[ProcessHandler]) -> None:
        self._handlers = handlers
        self._handlers_by_id = {handler.process_id: handler for handler in handlers}

    def list_summaries(self, base_url: str) -> list[dict[str, Any]]:
        """Return OGC-like process summaries for all registered handlers."""

        summaries: list[dict[str, Any]] = []
        for handler in self._handlers:
            definition = handler.definition(base_url)
            summaries.append(
                {
                    "id": handler.process_id,
                    "title": definition["title"],
                    "description": definition["description"],
                    "links": [
                        {
                            "rel": "process",
                            "type": FORMAT_TYPES[F_JSON],
                            "href": url_join(base_url, "processes", handler.process_id),
                        }
                    ],
                }
            )
        return summaries

    def get_definition(self, process_id: str, base_url: str) -> dict[str, Any]:
        """Return full process definition or raise a standard not-found error."""

        handler = self._handlers_by_id.get(process_id)
        if handler is None:
            raise not_found("Process", process_id)
        return handler.definition(base_url)

    def execute(self, process_id: str, inputs: dict[str, Any]) -> dict[str, Any]:
        """Execute the mapped process handler and return a job payload."""

        handler = self._handlers_by_id.get(process_id)
        if handler is None:
            raise not_found("Process", process_id)
        return handler.execute(inputs)
