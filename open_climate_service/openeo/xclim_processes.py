"""Auto-registration of xclim indicators as openEO process callables."""

from __future__ import annotations

import inspect
import logging
from typing import Any

from open_climate_service.process import _OCS_PROCESS_ATTR

logger = logging.getLogger(__name__)

_cache: list[Any] | None = None


def scan() -> list[Any]:
    """Return all xclim indicators as process callables (cached after first call)."""
    global _cache
    if _cache is not None:
        return _cache
    try:
        _cache = _collect()
        return _cache
    except Exception:
        logger.warning("Failed to load xclim indicators", exc_info=True)
        return []


def _collect() -> list[Any]:
    import xclim.indicators.atmos as atmos
    import xclim.indicators.land as land
    import xclim.indicators.seaIce as seaice
    from xclim.core.indicator import InputKind

    _SKIP_KINDS = {InputKind.DATASET, InputKind.KWARGS}
    _VARIABLE_KINDS = {InputKind.VARIABLE, InputKind.OPTIONAL_VARIABLE}
    kind_schema: dict[InputKind, dict[str, str]] = {
        InputKind.VARIABLE: {"type": "object", "subtype": "datacube"},
        InputKind.OPTIONAL_VARIABLE: {"type": "object", "subtype": "datacube"},
        InputKind.QUANTIFIED: {"type": "string"},
        InputKind.FREQ_STR: {"type": "string"},
        InputKind.NUMBER: {"type": "number"},
        InputKind.STRING: {"type": "string"},
        InputKind.DAY_OF_YEAR: {"type": "string"},
        InputKind.DATE: {"type": "string"},
        InputKind.NUMBER_SEQUENCE: {"type": "array"},
        InputKind.BOOL: {"type": "boolean"},
    }

    funcs: list[Any] = []
    seen: set[str] = set()

    for mod in (atmos, land, seaice):
        for obj in vars(mod).values():
            if not (hasattr(obj, "identifier") and hasattr(obj, "parameters") and hasattr(obj, "title")):
                continue
            indicator_id: str = obj.identifier
            if indicator_id in seen:
                continue
            seen.add(indicator_id)

            params: list[dict[str, Any]] = []
            for name, p in obj.parameters.items():
                if p.kind in _SKIP_KINDS:
                    continue
                param_meta: dict[str, Any] = {"name": name, "schema": kind_schema.get(p.kind, {})}
                if p.description:
                    param_meta["description"] = p.description
                # Only expose JSON-serializable scalar defaults.
                # VARIABLE/OPTIONAL_VARIABLE defaults are dataset-mode variable name
                # strings (e.g. "pr") — not meaningful as API defaults.
                if p.kind not in _VARIABLE_KINDS:
                    default = p.default
                    if default is not inspect.Parameter.empty and isinstance(
                        default, (str, int, float, bool, type(None))
                    ):
                        param_meta["optional"] = True
                        param_meta["default"] = default
                elif p.kind is InputKind.OPTIONAL_VARIABLE:
                    param_meta["optional"] = True
                params.append(param_meta)

            meta: dict[str, Any] = {
                "id": indicator_id,
                "summary": obj.title,
                "description": obj.abstract,
                "parameters": params,
                "returns": {"schema": {}},
            }
            funcs.append(_make_callable(obj, meta))

    return funcs


def _make_callable(indicator: Any, meta: dict[str, Any]) -> Any:
    """Wrap an xclim indicator as a bare callable with process metadata attached."""

    def _call(**kwargs: Any) -> Any:
        return indicator(**kwargs)

    setattr(_call, _OCS_PROCESS_ATTR, meta)
    return _call
