"""Discovery and loading of @process-decorated plugin functions."""

from __future__ import annotations

import importlib
import importlib.resources
import logging
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from open_climate_service import config as api_config
from open_climate_service.process import _OCS_PROCESS_ATTR, get_process_metadata

logger = logging.getLogger(__name__)


def load_plugin_processes() -> list[tuple[str, Any]]:
    """Return (process_id, callable) for all @process-decorated functions.

    Resolution order (last wins):
    1. xclim indicators — auto-registered from all indicator modules
    2. Built-in file plugins — ``open_climate_service/plugins/processes/``
    3. Instance plugins — ``plugins_dir/processes/`` (override everything)
    """
    found: dict[str, Any] = {}
    for func in _scan_xclim_indicators():
        meta = get_process_metadata(func)
        if meta:
            found[meta["id"]] = func
    for func in _scan_builtin_processes():
        meta = get_process_metadata(func)
        if meta:
            found[meta["id"]] = func
    for func in _scan_instance_processes():
        meta = get_process_metadata(func)
        if meta:
            found[meta["id"]] = func
    return list(found.items())


def to_openeo_descriptor(func: Any) -> dict[str, Any]:
    """Convert a @process-decorated function to an openEO process descriptor."""
    meta = get_process_metadata(func)
    if meta is None:
        raise ValueError(f"{func!r} is not decorated with @process")
    return dict(meta)


def _scan_builtin_processes() -> list[Any]:
    pkg = importlib.resources.files("open_climate_service") / "plugins" / "processes"
    funcs: list[Any] = []
    try:
        for resource in pkg.iterdir():
            if not resource.name.endswith(".py") or resource.name.startswith("_"):
                continue
            module_name = f"open_climate_service.plugins.processes.{resource.name[:-3]}"
            funcs.extend(_load_from_module(module_name))
    except (FileNotFoundError, NotADirectoryError):
        pass
    return funcs


def _scan_instance_processes() -> list[Any]:
    config = api_config.get_config()
    plugins_dir_raw = config.get("plugins_dir") if config else None
    if not plugins_dir_raw:
        return []
    config_path = api_config.get_config_path()
    base = config_path.parent if config_path else Path()
    processes_dir = (base / plugins_dir_raw).resolve() / "processes"
    if not processes_dir.is_dir():
        return []
    parent = str(processes_dir.parent)
    if parent not in sys.path:
        sys.path.append(parent)
    funcs: list[Any] = []
    for path in sorted(processes_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        funcs.extend(_load_from_module(f"processes.{path.stem}"))
    return funcs


def _load_from_module(module_name: str) -> list[Any]:
    try:
        module: ModuleType = importlib.import_module(module_name)
        return [obj for obj in vars(module).values() if callable(obj) and get_process_metadata(obj) is not None]
    except Exception:
        logger.warning("Failed to load plugin processes from %s", module_name, exc_info=True)
        return []


def _scan_xclim_indicators() -> list[Any]:
    """Auto-register all xclim indicators as process callables."""
    try:
        return _collect_xclim_indicators()
    except Exception:
        logger.warning("Failed to load xclim indicators", exc_info=True)
        return []


def _collect_xclim_indicators() -> list[Any]:
    import xclim.indicators.atmos as atmos
    import xclim.indicators.land as land
    import xclim.indicators.seaIce as seaice
    from xclim.core.indicator import InputKind

    _SKIP_KINDS = {InputKind.DATASET, InputKind.KWARGS}
    _KIND_SCHEMA: dict[InputKind, dict[str, str]] = {
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
                param_meta: dict[str, Any] = {"name": name, "schema": _KIND_SCHEMA.get(p.kind, {})}
                if p.description:
                    param_meta["description"] = p.description
                # VARIABLE defaults are dataset-mode variable name strings, not real defaults
                if p.kind not in (InputKind.VARIABLE,):
                    param_meta["optional"] = True
                    param_meta["default"] = p.default
                params.append(param_meta)

            meta: dict[str, Any] = {
                "id": indicator_id,
                "summary": obj.title,
                "description": obj.abstract,
                "parameters": params,
                "returns": {"schema": {}},
            }
            funcs.append(_make_indicator_callable(obj, meta))

    return funcs


def _make_indicator_callable(indicator: Any, meta: dict[str, Any]) -> Any:
    """Wrap an xclim indicator as a bare callable with process metadata attached."""

    def _call(**kwargs: Any) -> Any:
        return indicator(**kwargs)

    setattr(_call, _OCS_PROCESS_ATTR, meta)
    return _call
