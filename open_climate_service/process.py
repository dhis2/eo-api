"""@process decorator for registering Python functions as named openEO processes."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar, overload

F = TypeVar("F", bound=Callable[..., Any])

_OCS_PROCESS_ATTR = "__ocs_process__"

_PYTHON_TYPE_MAP: dict[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
}


@overload
def process(func: F) -> F: ...


@overload
def process(
    func: None = None,
    *,
    summary: str | None = None,
    description: str | None = None,
    parameters: dict[str, dict[str, Any]] | None = None,
) -> Callable[[F], F]: ...


def process(
    func: F | None = None,
    *,
    summary: str | None = None,
    description: str | None = None,
    parameters: dict[str, dict[str, Any]] | None = None,
) -> F | Callable[[F], F]:
    """Register a function as a named openEO process plugin.

    Decorated functions are discovered automatically from ``plugins_dir/processes/``
    and appear in ``GET /processes``, callable directly by ``process_id`` in any
    openEO process graph.

    Usage — minimal (summary and parameter descriptions from docstring)::

        @process
        def consecutive_dry_days(pr: xr.DataArray, thresh: str = "1mm/day") -> xr.DataArray:
            '''Maximum consecutive dry days per period.'''
            return xclim.atmos.maximum_consecutive_dry_days(pr, thresh=thresh)

    Usage — explicit metadata::

        @process(
            summary="Maximum consecutive dry days",
            parameters={"thresh": {"description": "Precipitation threshold"}},
        )
        def consecutive_dry_days(pr: xr.DataArray, thresh: str = "1mm/day") -> xr.DataArray: ...
    """

    def decorator(fn: F) -> F:
        doc = inspect.getdoc(fn) or ""
        sig = inspect.signature(fn)

        params: list[dict[str, Any]] = []
        for name, param in sig.parameters.items():
            if name in ("self", "cls"):
                continue
            p: dict[str, Any] = {"name": name, "schema": {}}
            ann = param.annotation
            if ann is not inspect.Parameter.empty:
                type_str = _PYTHON_TYPE_MAP.get(ann)
                if type_str:
                    p["schema"] = {"type": type_str}
            if param.default is not inspect.Parameter.empty:
                p["optional"] = True
                p["default"] = param.default
            if parameters and name in parameters:
                p.update(parameters[name])
            params.append(p)

        meta: dict[str, Any] = {
            "id": fn.__name__,
            "summary": summary or (doc.splitlines()[0] if doc else fn.__name__),
            "description": description or doc,
            "parameters": params,
            "returns": {"schema": {}},
        }

        setattr(fn, _OCS_PROCESS_ATTR, meta)
        return fn

    if func is not None:
        return decorator(func)
    return decorator


def get_process_metadata(obj: Any) -> dict[str, Any] | None:
    """Return the process metadata set by ``@process``, or None."""
    return getattr(obj, _OCS_PROCESS_ATTR, None)
