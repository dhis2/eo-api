"""Process graph execution for openEO jobs.

Uses openeo-pg-parser-networkx for process graph parsing and
openeo-processes-dask for the standard openEO process implementations.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from typing import Any

import xarray as xr
from fastapi import HTTPException, Request

from open_climate_service.data_accessor.services.accessor import open_icechunk_dataset, open_zarr_dataset
from open_climate_service.data_manager.services.utils import get_time_dim
from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.ingestions.schemas import ArtifactFormat

logger = logging.getLogger(__name__)

_registry: Any = None  # lazy singleton


def _build_process_registry() -> Any:
    """Build the base process registry (lazy-initialised singleton).

    Contains: all openeo-processes-dask standard processes, backend load_collection /
    save_result, and any exposed native YAML-configured processing plugins.
    UDPs are NOT included here — they are added per-execution by _augment_with_udps.
    """
    global _registry
    if _registry is not None:
        return _registry

    from openeo_pg_parser_networkx.process_registry import Process, ProcessRegistry
    from openeo_processes_dask.process_implementations.core import process as wrap_fn

    impls_module = importlib.import_module("openeo_processes_dask.process_implementations")
    specs_module = importlib.import_module("openeo_processes_dask.specs")

    registry = ProcessRegistry(wrap_funcs=[wrap_fn])

    for name, func in inspect.getmembers(impls_module, inspect.isfunction):
        spec: dict[str, Any] = getattr(specs_module, name, None) or {}
        registry[name] = Process(spec=spec, implementation=func)

    # Backend-specific processes override any stub from the standard library.
    registry["load_collection"] = Process(spec={}, implementation=_load_collection_impl)
    registry["save_result"] = Process(spec={}, implementation=_save_result_impl)

    # Native YAML-configured processing plugins.
    # Plugins with the same id as a standard process shadow the built-in.
    _register_native_plugins(registry)

    _registry = registry
    return registry


def _register_native_plugins(registry: Any) -> None:
    """Register all native processing plugins into the openEO process registry.

    All plugins with an execution.function are registered regardless of the
    expose flag — expose only controls visibility in GET /processes/{id}
    (OGC-style endpoint), not callability from openEO process graphs.
    """
    from openeo_pg_parser_networkx.process_registry import Process

    from open_climate_service.data_registry.services import processes as process_registry_svc

    for p in process_registry_svc.list_processes():
        execution = p.get("execution") or {}
        fn_path = execution.get("function") if isinstance(execution, dict) else None
        if not fn_path:
            continue
        try:
            func = process_registry_svc.get_process_function(p["id"])
            registry[p["id"]] = Process(spec={}, implementation=func)
        except ImportError:
            logger.debug("Native plugin '%s' skipped: module not found", p["id"])
        except Exception:
            logger.warning("Could not register native plugin '%s' in process registry", p["id"])


class _RegistryOverlay:
    """Thin read-only overlay that resolves UDPs before falling back to the base registry.

    pg-parser only calls __getitem__ on the registry, so we only need to implement that.
    """

    def __init__(self, base: Any, udp_map: dict[str, Any]) -> None:
        self._base = base
        self._udps = udp_map

    def __getitem__(self, key: Any) -> Any:
        name = key[1] if isinstance(key, tuple) else key
        if name in self._udps:
            return self._udps[name]
        return self._base[key]


def _augment_with_udps(base_registry: Any) -> Any:
    """Return a registry overlay that adds currently stored UDPs to the base registry.

    UDPs are loaded fresh on every call so that PUT /process_graphs changes take effect
    without restarting the server.  Returns the base registry unchanged when no UDPs exist.
    """
    from openeo_pg_parser_networkx.process_registry import Process

    from open_climate_service.openeo import udps as udp_store

    udp_records = udp_store.list_udps().processes
    if not udp_records:
        return base_registry

    udp_map: dict[str, Any] = {}
    overlay = _RegistryOverlay(base_registry, udp_map)

    for udp in udp_records:
        if not udp.process_graph:
            continue
        pg_dict: dict[str, Any] = dict(udp.process_graph)

        def _make_udp_impl(pg: dict[str, Any], udp_id: str) -> Any:
            _executing: set[str] = set()

            def _udp_impl(**kwargs: Any) -> Any:
                from openeo_pg_parser_networkx import OpenEOProcessGraph

                if udp_id in _executing:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Recursive UDP call detected: '{udp_id}' called itself",
                    )
                _executing.add(udp_id)
                try:
                    return OpenEOProcessGraph(pg).to_callable(overlay, parameters=kwargs)()  # pyright: ignore[reportArgumentType]
                finally:
                    _executing.discard(udp_id)

            return _udp_impl

        udp_map[udp.id] = Process(spec={}, implementation=_make_udp_impl(pg_dict, udp.id))

    return overlay


# ---------------------------------------------------------------------------
# load_collection — returns DataArray (openEO data cube)
# ---------------------------------------------------------------------------


def _bbox_to_dict(extent: Any) -> dict[str, float] | None:
    """Normalise a BoundingBox object or plain dict to a lowercase-keyed dict."""
    if extent is None:
        return None
    if isinstance(extent, dict):
        _coord_keys = {"west", "south", "east", "north"}
        return {k.lower(): float(v) for k, v in extent.items() if k.lower() in _coord_keys and v is not None}
    # openeo_pg_parser_networkx BoundingBox pydantic model
    return {
        "west": float(extent.west),
        "south": float(extent.south),
        "east": float(extent.east),
        "north": float(extent.north),
    }


def _temporal_to_list(extent: Any) -> list[str | None] | None:
    """Normalise a TemporalInterval object or plain list to [start, end] strings."""
    if extent is None:
        return None
    items: list[Any]
    if isinstance(extent, list):
        items = extent
    else:
        # TemporalInterval is a pydantic RootModel wrapping a list of Date|None
        items = list(getattr(extent, "root", None) or [])

    result: list[str | None] = []
    for v in items:
        if v is None:
            result.append(None)
        elif hasattr(v, "root"):
            # Date/DateTime wrapper — inner value is a pendulum DateTime
            result.append(_strip_tz(v.root.isoformat()))
        elif isinstance(v, str):
            result.append(_strip_tz(v))
        else:
            result.append(_strip_tz(str(v)))
    return result


def _load_collection_impl(
    id: str,
    spatial_extent: Any = None,
    temporal_extent: Any = None,
    bands: Any = None,
) -> xr.DataArray:
    """Load a published dataset as an openEO data cube (xr.DataArray)."""
    artifact = _get_published_artifact(id)
    ds = _open_artifact(artifact)

    bbox = _bbox_to_dict(spatial_extent)
    t_extent = _temporal_to_list(temporal_extent)

    if t_extent is not None:
        start = t_extent[0] if len(t_extent) > 0 else None
        end = t_extent[1] if len(t_extent) > 1 else None
        try:
            t_dim = get_time_dim(ds)
            ds = ds.sel({t_dim: slice(start, end)})
        except (ValueError, KeyError):
            # ValueError: no recognisable time dimension
            # KeyError: coordinate exists but is not an indexable dimension
            pass

    if bbox is not None:
        x_dim = next((d for d in ("x", "longitude") if d in ds.dims), None)
        y_dim = next((d for d in ("y", "latitude") if d in ds.dims), None)
        west, east = bbox.get("west"), bbox.get("east")
        south, north = bbox.get("south"), bbox.get("north")
        if x_dim and west is not None and east is not None:
            ds = ds.sel({x_dim: slice(west, east)})
        if y_dim and south is not None and north is not None:
            coord = ds[y_dim].values
            # Descending latitude axis (e.g. ERA5, WorldPop)
            if len(coord) > 1 and coord[0] > coord[-1]:
                ds = ds.sel({y_dim: slice(north, south)})
            else:
                ds = ds.sel({y_dim: slice(south, north)})

    available_vars = list(ds.data_vars)
    if isinstance(bands, list) and bands:
        available_vars = [b for b in bands if b in ds]
        if not available_vars:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"None of the requested bands {bands!r} exist in collection '{id}'. Available: {list(ds.data_vars)}"
                ),
            )

    if len(available_vars) == 1:
        return ds[available_vars[0]]

    # Multi-band: stack variables on a new "bands" dimension
    import pandas as pd

    return xr.concat(
        [ds[b] for b in available_vars],
        dim=pd.Index(available_vars, name="bands"),
    )


class SaveResultEnvelope:
    """Wraps the process graph result with the requested output format."""

    def __init__(self, data: Any, format: str) -> None:
        self.data = data
        self.format = format.upper()


def _save_result_impl(data: Any, format: str = "Zarr", options: dict[str, Any] | None = None) -> Any:
    return SaveResultEnvelope(data, format)


# ---------------------------------------------------------------------------
# Internal helpers shared with _load_collection_impl
# ---------------------------------------------------------------------------


def _get_published_artifact(collection_id: str) -> Any:
    eligible = _eligible_artifacts()
    artifact = eligible.get(collection_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")
    return artifact


def _eligible_artifacts() -> dict[str, Any]:
    return ingestion_services.latest_published_zarr_artifacts_by_dataset()


def _open_artifact(artifact: Any) -> xr.Dataset:
    path = _artifact_store_path(artifact)
    if artifact.format == ArtifactFormat.ICECHUNK:
        return open_icechunk_dataset(path)
    return open_zarr_dataset(path)


def _artifact_store_path(artifact: Any) -> str:
    if artifact.path:
        return str(artifact.path)
    if artifact.asset_paths:
        return str(artifact.asset_paths[0])
    raise HTTPException(
        status_code=500,
        detail=f"Artifact '{artifact.artifact_id}' has no readable storage path",
    )


def _strip_tz(value: str | None) -> str | None:
    """Strip timezone suffix from an ISO datetime string for naive xarray coords."""
    if value is None:
        return None
    for suffix in ("Z", "+00:00"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_process_graph(
    process: dict[str, Any],
    request: Request | None = None,
    result_dir: str | None = None,
) -> Any:
    """Execute an openEO process graph and return the result value."""
    from openeo_pg_parser_networkx import OpenEOProcessGraph

    process_graph = process.get("process_graph")
    if not isinstance(process_graph, dict):
        raise HTTPException(status_code=422, detail="process.process_graph must be an object")

    registry = _augment_with_udps(_build_process_registry())
    try:
        graph = OpenEOProcessGraph(process_graph)
        return graph.to_callable(registry)()
    except HTTPException:
        raise
    except (TypeError, ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid process graph: {exc}") from exc
    except Exception as e:
        logger.exception("Process graph execution failed")
        raise HTTPException(status_code=500, detail=f"Process graph execution failed: {e}") from e
