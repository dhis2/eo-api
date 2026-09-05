"""@feature_provider — register a callable that resolves params to a FeatureCollection (CLIM-926).

A provider answers one question: given some parameters, what geometries? Where they come from is
the provider's business — a DHIS2 hierarchy, a stored GeoParquet file, a national registry, a WFS.
The instance config names them and binds parameters; nothing else in OCS needs to know.

Discovery mirrors `@process` exactly: a ``features/`` folder, read built-in first, then from each
installed plugin package, then from ``plugins_dir`` which overrides both. A country therefore adds
a provider the same way it already adds a dataset or a process.
"""

from __future__ import annotations

import importlib
import importlib.resources
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, TypeVar

from open_climate_service import config as api_config

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

_OCS_FEATURE_PROVIDER_ATTR = "__ocs_feature_provider__"


def feature_provider(name: str) -> Callable[[F], F]:
    """Register ``func`` as the provider named ``name``.

    The function takes the declaration's ``params`` as keyword arguments and returns a GeoJSON
    FeatureCollection. It may also return ``(collection, version)``, where ``version`` is an etag
    or release id the cache can compare against — a provider that can answer "has this changed?"
    more cheaply than refetching should say so that way.
    """

    def decorate(func: F) -> F:
        setattr(func, _OCS_FEATURE_PROVIDER_ATTR, name)
        return func

    return decorate


def get_provider_name(func: Any) -> str | None:
    """The registered name of a provider function, or None if it is not one."""
    return getattr(func, _OCS_FEATURE_PROVIDER_ATTR, None)


def _load_from_module(module_name: str) -> list[Any]:
    try:
        module: ModuleType = importlib.import_module(module_name)
        return [obj for obj in vars(module).values() if callable(obj) and get_provider_name(obj) is not None]
    except Exception:
        logger.warning("Failed to load feature providers from %s", module_name, exc_info=True)
        return []


def _scan_builtin() -> list[Any]:
    pkg = importlib.resources.files("open_climate_service") / "plugins" / "features"
    funcs: list[Any] = []
    try:
        for resource in pkg.iterdir():
            if not resource.name.endswith(".py") or resource.name.startswith("_"):
                continue
            funcs.extend(_load_from_module(f"open_climate_service.plugins.features.{resource.name[:-3]}"))
    except (FileNotFoundError, NotADirectoryError):
        pass
    return funcs


def _scan_plugin_packages() -> list[Any]:
    """Scan ``features/`` in each installed plugin package."""
    from open_climate_service.plugin_discovery import iter_plugin_subdirs

    funcs: list[Any] = []
    for _name, package, features_res in iter_plugin_subdirs("features"):
        for resource in features_res.iterdir():
            if not resource.name.endswith(".py") or resource.name.startswith("_"):
                continue
            funcs.extend(_load_from_module(f"{package}.features.{resource.name[:-3]}"))
    return funcs


def _scan_instance() -> list[Any]:
    config = api_config.get_config()
    plugins_dir_raw = config.get("plugins_dir") if config else None
    if not plugins_dir_raw:
        return []
    config_path = api_config.get_config_path()
    base = config_path.parent if config_path else Path()
    features_dir = (base / plugins_dir_raw).resolve() / "features"
    if not features_dir.is_dir():
        return []
    parent = str(features_dir.parent)
    if parent not in sys.path:
        sys.path.append(parent)
    funcs: list[Any] = []
    for path in sorted(features_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        funcs.extend(_load_from_module(f"features.{path.stem}"))
    return funcs


def registry() -> dict[str, Any]:
    """Every discovered provider by name, later tiers overriding earlier ones.

    Built-in, then installed plugin packages, then ``plugins_dir`` — the same precedence the other
    three plugin folders use, so an instance can replace the shipped `dhis2` provider with one that
    talks to its own infrastructure without forking.
    """
    found: dict[str, Any] = {}
    for func in [*_scan_builtin(), *_scan_plugin_packages(), *_scan_instance()]:
        name = get_provider_name(func)
        if name is not None:
            found[name] = func
    return found


def resolve_provider(name: str) -> Any:
    """The provider registered as ``name``, or a ValueError naming what is available."""
    providers = registry()
    provider = providers.get(name)
    if provider is None:
        available = ", ".join(sorted(providers)) or "none"
        raise ValueError(f"Unknown feature provider {name!r}. Available: {available}")
    return provider
