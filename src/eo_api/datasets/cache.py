"""Dataset cache: download, store, and optimize raster data as local files."""

import datetime
import importlib
import inspect
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import xarray as xr
from fastapi import BackgroundTasks

from .constants import BBOX, CACHE_OVERRIDE, COUNTRY_CODE
from .utils import get_lon_lat_dims, get_time_dim, numpy_period_string

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
_cache_dir = SCRIPT_DIR / "cache"
if CACHE_OVERRIDE:
    _cache_dir = Path(CACHE_OVERRIDE)
CACHE_DIR: Path = _cache_dir


def build_dataset_cache(
    dataset: dict[str, Any],
    start: str,
    end: str | None,
    overwrite: bool,
    background_tasks: BackgroundTasks | None,
) -> None:
    """Download dataset from source and store as local NetCDF cache files."""
    cache_info = dataset["cacheInfo"]
    eo_download_func_path = cache_info["eoFunction"]
    eo_download_func = _get_dynamic_function(eo_download_func_path)

    params: dict[str, Any] = dict(cache_info["defaultParams"])
    params.update(
        {
            "start": start,
            "end": end or datetime.date.today().isoformat(),
            "dirname": CACHE_DIR,
            "prefix": _get_cache_prefix(dataset),
            "overwrite": overwrite,
        }
    )

    sig = inspect.signature(eo_download_func)
    if "bbox" in sig.parameters:
        params["bbox"] = BBOX
    elif "country_code" in sig.parameters:
        params["country_code"] = COUNTRY_CODE

    if background_tasks is not None:
        background_tasks.add_task(eo_download_func, **params)


def optimize_dataset_cache(dataset: dict[str, Any]) -> None:
    """Collect all cache files into a single optimised zarr archive."""
    logger.info(f"Optimizing cache for dataset {dataset['id']}")

    files = get_cache_files(dataset)
    logger.info(f"Opening {len(files)} files from cache")
    ds = xr.open_mfdataset(files)

    # trim to only minimal vars and coords
    logger.info("Trimming unnecessary variables and coordinates")
    varname = dataset["variable"]
    ds = ds[[varname]]
    keep_coords = [get_time_dim(ds)] + list(get_lon_lat_dims(ds))
    drop_coords = [c for c in ds.coords if c not in keep_coords]
    ds = ds.drop_vars(drop_coords)

    # determine optimal chunk sizes
    logger.info("Determining optimal chunk size for zarr archive")
    ds_autochunk = ds.chunk("auto").unify_chunks()
    uniform_chunks: dict[str, Any] = {str(dim): ds_autochunk.chunks[dim][0] for dim in ds_autochunk.dims}
    time_space_chunks = _compute_time_space_chunks(ds, dataset)
    uniform_chunks.update(time_space_chunks)
    logging.info(f"--> {uniform_chunks}")

    # save as zarr
    logger.info("Saving to optimized zarr file")
    zarr_path = CACHE_DIR / f"{_get_cache_prefix(dataset)}.zarr"
    ds_chunked = ds.chunk(uniform_chunks)
    ds_chunked.to_zarr(zarr_path, mode="w")
    ds_chunked.close()

    logger.info("Finished cache optimization")


def _compute_time_space_chunks(
    ds: xr.Dataset,
    dataset: dict[str, Any],
    max_spatial_chunk: int = 256,
) -> dict[str, int]:
    """Compute chunk sizes tuned for common temporal access patterns."""
    chunks: dict[str, int] = {}

    dim = get_time_dim(ds)
    period_type = dataset["periodType"]
    if period_type == "hourly":
        chunks[dim] = 24 * 7
    elif period_type == "daily":
        chunks[dim] = 30
    elif period_type == "monthly":
        chunks[dim] = 12
    elif period_type == "yearly":
        chunks[dim] = 1

    lon_dim, lat_dim = get_lon_lat_dims(ds)
    chunks[lon_dim] = min(ds.sizes[lon_dim], max_spatial_chunk)
    chunks[lat_dim] = min(ds.sizes[lat_dim], max_spatial_chunk)

    return chunks


def get_cache_info(dataset: dict[str, Any]) -> dict[str, Any]:
    """Return temporal and spatial coverage metadata for the cached dataset."""
    files = get_cache_files(dataset)
    if not files:
        return {"temporal_coverage": None, "spatial_coverage": None}

    ds = xr.open_dataset(sorted(files)[0])

    time_dim = get_time_dim(ds)
    lon_dim, lat_dim = get_lon_lat_dims(ds)

    start = numpy_period_string(ds[time_dim].min().values, dataset["periodType"])  # type: ignore[arg-type]

    xmin, xmax = ds[lon_dim].min().item(), ds[lon_dim].max().item()
    ymin, ymax = ds[lat_dim].min().item(), ds[lat_dim].max().item()

    ds = xr.open_dataset(sorted(files)[-1])
    end = numpy_period_string(ds[time_dim].max().values, dataset["periodType"])  # type: ignore[arg-type]

    return {
        "coverage": {
            "temporal": {"start": start, "end": end},
            "spatial": {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax},
        }
    }


def _get_cache_prefix(dataset: dict[str, Any]) -> str:
    return str(dataset["id"])


def get_cache_files(dataset: dict[str, Any]) -> list[Path]:
    """Return all NetCDF cache files matching this dataset's prefix."""
    # TODO: not bulletproof -- e.g. 2m_temperature matches 2m_temperature_modified
    prefix = _get_cache_prefix(dataset)
    return list(CACHE_DIR.glob(f"{prefix}*.nc"))


def get_zarr_path(dataset: dict[str, Any]) -> Path | None:
    """Return the optimised zarr archive path if it exists."""
    prefix = _get_cache_prefix(dataset)
    optimized = CACHE_DIR / f"{prefix}.zarr"
    if optimized.exists():
        return optimized
    return None


def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    """Import and return a function given its dotted module path."""
    parts = full_path.split(".")
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]
    module = importlib.import_module(module_path)
    return getattr(module, function_name)  # type: ignore[no-any-return]
