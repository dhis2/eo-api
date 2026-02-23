from functools import lru_cache
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

from eoapi.datasets.base import AreaResolver, CoverageResolver, PositionResolver
from eoapi.datasets.catalog import DATASETS_DIR


def _load_module(module_name: str, module_path: Path) -> ModuleType:
    spec = spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load resolver module from: {module_path}")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def _resolver_modules() -> dict[str, ModuleType]:
    modules: dict[str, ModuleType] = {}

    for dataset_dir in sorted(path for path in DATASETS_DIR.iterdir() if path.is_dir()):
        resolver_path = dataset_dir / "resolver.py"
        if not resolver_path.exists():
            continue

        module_name = f"eoapi.datasets._resolver_{dataset_dir.name.replace('-', '_')}"
        modules[dataset_dir.name] = _load_module(module_name, resolver_path)

    return modules


@lru_cache(maxsize=1)
def coverage_resolvers() -> dict[str, CoverageResolver]:
    resolvers: dict[str, CoverageResolver] = {}
    for dataset_id, module in _resolver_modules().items():
        resolver = getattr(module, "coverage_source", None)
        if callable(resolver):
            resolvers[dataset_id] = resolver
    return resolvers


@lru_cache(maxsize=1)
def position_resolvers() -> dict[str, PositionResolver]:
    resolvers: dict[str, PositionResolver] = {}
    for dataset_id, module in _resolver_modules().items():
        resolver = getattr(module, "position_source", None)
        if callable(resolver):
            resolvers[dataset_id] = resolver
    return resolvers


@lru_cache(maxsize=1)
def area_resolvers() -> dict[str, AreaResolver]:
    resolvers: dict[str, AreaResolver] = {}
    for dataset_id, module in _resolver_modules().items():
        resolver = getattr(module, "area_source", None)
        if callable(resolver):
            resolvers[dataset_id] = resolver
    return resolvers
