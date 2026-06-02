"""Dataset cache: download, store, and optimize raster data as local files."""

import datetime
import importlib
import inspect
import logging
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import xarray as xr
import xproj  # noqa: F401  # type: ignore[import-untyped]  # pyright: ignore[reportUnusedImport]
from fastapi import BackgroundTasks, HTTPException
from geozarr_toolkit import MultiscalesConventionMetadata, create_geozarr_attrs
from topozarr.coarsen import create_pyramid

from open_climate_service import config as api_config
from open_climate_service.shared.time import resolve_iso_period_step, time_chunk_for_iso_step
from open_climate_service.transforms.pipeline import run_dataset_transforms
from open_climate_service.transforms.reproject import reproject_to_instance_crs

from .utils import get_time_dim, get_x_y_dims

logger = logging.getLogger(__name__)


def _resolve_download_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "downloads"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-service" / "downloads"


DOWNLOAD_DIR = _resolve_download_dir()


def download_dataset(
    dataset: dict[str, Any],
    start: str,
    end: str | None,
    bbox: list[float] | None,
    country_code: str | None,
    overwrite: bool,
    background_tasks: BackgroundTasks | None,
) -> list[Path]:
    """Download dataset files and return the NetCDF paths created or modified by this run.

    The download still happens primarily through side effects in the provider function.
    This return value is used to identify the concrete files created for this invocation.
    When running in the background-task path, the download is deferred and this function
    returns an empty list because no files have been created yet.
    """
    _validate_spatial_coverage(dataset, bbox if bbox is not None else _bbox_from_env())
    ingestion = dataset["ingestion"]
    eo_download_func_path = ingestion.get("function")
    if not isinstance(eo_download_func_path, str) or not eo_download_func_path:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Dataset '{dataset['id']}' does not expose the legacy download path. "
                "Use the plugin-backed ingestion process instead."
            ),
        )
    eo_download_func = _get_dynamic_function(eo_download_func_path)
    before_files = {path.resolve(): path.stat().st_mtime_ns for path in get_cache_files(dataset)}

    params = dict(ingestion.get("default_params", {}))
    params.update(
        {
            "start": start,
            "end": end or datetime.date.today().isoformat(),
            "dirname": DOWNLOAD_DIR,
            "prefix": _get_cache_prefix(dataset),
            "overwrite": overwrite,
        }
    )

    sig = inspect.signature(eo_download_func)
    try:
        if "bbox" in sig.parameters:
            params["bbox"] = _resolve_bbox(bbox=bbox)
        if "country_code" in sig.parameters:
            resolved_country_code = country_code or os.getenv("COUNTRY_CODE")
            if resolved_country_code:
                params["country_code"] = resolved_country_code
            else:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Downloading this dataset requires a country code. "
                        "Provide it through the resolved extent configuration or set COUNTRY_CODE in the environment."
                    ),
                )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if background_tasks is not None:
        background_tasks.add_task(eo_download_func, **params)
        return []

    try:
        eo_download_func(**params)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        message = str(exc).strip() or "Unexpected error from upstream data provider"
        raise HTTPException(status_code=502, detail=f"Upstream dataset download failed: {message}") from exc

    after_files = [path.resolve() for path in get_cache_files(dataset)]
    changed_files = [
        path for path in after_files if path not in before_files or path.stat().st_mtime_ns != before_files[path]
    ]
    return changed_files


def build_dataset_zarr(dataset: dict[str, Any], *, start: str | None = None, end: str | None = None) -> None:
    """Collect dataset cache files into one optimised Zarr archive, clipped to request scope."""
    logger.info(f"Optimizing cache for dataset {dataset['id']}")

    files = get_cache_files(dataset)
    logger.info(f"Opening {len(files)} files from cache")
    ds = xr.open_mfdataset(files, parallel=False)

    x_dim, y_dim = get_x_y_dims(ds)
    dims = [x_dim, y_dim]

    # trim to only minimal vars and coords before loading into memory
    logger.info("Trimming unnecessary variables and coordinates")
    varname = dataset["variable"]
    ds = ds[[varname]]
    keep_coords = [get_time_dim(ds)] + dims
    drop_coords = [c for c in ds.coords if c not in keep_coords]
    ds = ds.drop_vars(drop_coords)

    # Normalise to canonical names so all stored Zarr files are consistent.
    crs = api_config.get_crs()
    time_dim = get_time_dim(ds)
    rename_map = {k: v for k, v in [(time_dim, "t"), (x_dim, "x"), (y_dim, "y")] if k != v}
    if rename_map:
        ds = ds.rename(rename_map)
    x_dim, y_dim = "x", "y"
    dims = [x_dim, y_dim]

    ds = _select_time_range(ds, dataset=dataset, start=start, end=end)
    ds = _run_transforms(ds, dataset)

    source_crs: str = dataset.get("source_crs", "EPSG:4326")
    ds = reproject_to_instance_crs(ds, dataset, source_crs=source_crs)

    xmin = ds[x_dim].min().item()
    xmax = ds[x_dim].max().item()
    ymin = ds[y_dim].min().item()
    ymax = ds[y_dim].max().item()
    bbox = [xmin, ymin, xmax, ymax]
    shape = (ds.sizes[x_dim], ds.sizes[y_dim])

    # https://github.com/zarr-developers/geozarr-toolkit/issues/15
    geozarr_attrs = create_geozarr_attrs(
        dimensions=dims,
        crs=crs,
        bbox=bbox,
        shape=shape,
    )

    # save as zarr
    logger.info("Saving to optimized zarr file")
    zarr_path = DOWNLOAD_DIR / f"{_get_cache_prefix(dataset)}.zarr"

    if _needs_pyramid(ds, x_dim, y_dim):
        levels = _pyramid_levels(ds, x_dim, y_dim)
        logger.info("Building %d-level pyramid (max dim %d pixels)", levels, max(ds.sizes[x_dim], ds.sizes[y_dim]))

        # Add multiscales convention metadata to the zarr attributes
        zarr_conventions = geozarr_attrs.get("zarr_conventions", [])
        zarr_conventions.append(MultiscalesConventionMetadata().model_dump())
        geozarr_attrs["zarr_conventions"] = zarr_conventions

        # Load into memory then close to deterministically release netCDF file handles
        # before create_pyramid spawns multiprocessing workers. After load() the data
        # lives in numpy arrays and no longer needs the underlying file objects.
        ds.load()
        ds.close()

        ds = ds.proj.assign_crs(spatial_ref=crs)

        # https://github.com/carbonplan/topozarr/issues/13
        pyramid = create_pyramid(ds, levels=levels, x_dim=x_dim, y_dim=y_dim, method="mean")

        pyramid.dt.attrs.update(geozarr_attrs)
        pyramid.dt.to_zarr(zarr_path, mode="w", encoding=pyramid.encoding, zarr_format=3)

        # zarr-layer looks for the time coordinate at the root of the store, not inside each level.
        # Copy it from level 0 so browser clients can discover it without knowing the level structure.
        time_dim = get_time_dim(ds)
        time_src = zarr_path / "0" / time_dim
        time_dst = zarr_path / time_dim
        if time_src.exists():
            if time_dst.exists():
                shutil.rmtree(time_dst)
            shutil.copytree(time_src, time_dst)

        pyramid.dt.close()

    else:
        logger.info("Building flat zarr (max dim %d pixels)", max(ds.sizes[x_dim], ds.sizes[y_dim]))
        uniform_chunks = _compute_time_space_chunks(ds, dataset)
        logger.info(f"--> {uniform_chunks}")

        ds.attrs.update(geozarr_attrs)
        ds_chunked = ds.chunk(uniform_chunks)
        # Remove _FillValue from each variable's encoding so that in-memory NaN values
        # are stored as IEEE NaN in zarr rather than re-encoded as a sentinel (e.g.
        # -999.99). ZarrLayer uses the zarr fill_value attribute (nan for floats) to
        # render missing pixels as transparent — not a separately specified fillValue.
        for var in ds_chunked.data_vars:
            ds_chunked[var].encoding.pop("_FillValue", None)
        ds_chunked.to_zarr(zarr_path, mode="w", zarr_format=3, consolidated=True)
        ds_chunked.close()

    ds.close()
    logger.info("Finished cache optimization")


_PYRAMID_PIXEL_THRESHOLD = 2048 * 2048
_PYRAMID_MAX_LEVELS = 8
_PYRAMID_TARGET_TILE_SIZE = 512


def _needs_pyramid(ds: xr.Dataset, x_dim: str, y_dim: str) -> bool:
    """Return True when the spatial extent is large enough to benefit from a pyramid."""
    return ds.sizes[x_dim] * ds.sizes[y_dim] > _PYRAMID_PIXEL_THRESHOLD


def _pyramid_levels(ds: xr.Dataset, x_dim: str, y_dim: str) -> int:
    """Compute the number of pyramid levels needed to reach a manageable tile size."""
    import math

    max_dim = max(ds.sizes[x_dim], ds.sizes[y_dim])
    levels = math.ceil(math.log2(max_dim / _PYRAMID_TARGET_TILE_SIZE))
    return max(2, min(levels, _PYRAMID_MAX_LEVELS))


def _select_time_range(
    ds: xr.Dataset,
    *,
    dataset: dict[str, Any],
    start: str | None,
    end: str | None,
) -> xr.Dataset:
    """Clip a cached dataset to the managed artifact's requested temporal scope."""
    if start is None and end is None:
        return ds

    time_dim = get_time_dim(ds)
    selected = ds.sel({time_dim: slice(start, end)})
    if selected.sizes.get(time_dim, 0) == 0:
        raise ValueError(f"No cached data for dataset '{dataset['id']}' intersects requested time range {start}..{end}")
    logger.info(
        "Clipped dataset '%s' to requested time range %s..%s (%d steps)",
        dataset["id"],
        start,
        end,
        selected.sizes[time_dim],
    )
    return selected


def _run_transforms(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    return run_dataset_transforms(ds, dataset)


def _compute_time_space_chunks(
    ds: xr.Dataset,
    dataset: dict[str, Any],
    max_spatial_chunk: int = 512,
) -> dict[str, int]:
    """Compute chunk sizes tuned for common temporal access patterns."""
    chunks: dict[str, int] = {}

    iso_step = resolve_iso_period_step(dataset)
    dim = get_time_dim(ds)
    if iso_step is not None:
        try:
            chunks[dim] = time_chunk_for_iso_step(iso_step)
        except ValueError:
            logger.warning(
                "Invalid ISO 8601 step %r for dataset '%s'; defaulting time chunk to 12.",
                iso_step,
                dataset.get("id", "?"),
            )
            chunks[dim] = 12
    else:
        logger.warning(
            "No ISO 8601 step for dataset '%s'; defaulting time chunk to 12. "
            "Declare 'extents.temporal.resolution' in the template to silence this warning.",
            dataset.get("id", "?"),
        )
        chunks[dim] = 12

    x_dim, y_dim = get_x_y_dims(ds)
    chunks[x_dim] = min(ds.sizes[x_dim], max_spatial_chunk)
    chunks[y_dim] = min(ds.sizes[y_dim], max_spatial_chunk)

    return chunks


def _get_cache_prefix(dataset: dict[str, Any]) -> str:
    return str(dataset["id"])


def get_cache_files(dataset: dict[str, Any]) -> list[Path]:
    """Return all NetCDF cache files matching this dataset's prefix."""
    # TODO: not bulletproof -- e.g. 2m_temperature matches 2m_temperature_modified
    prefix = _get_cache_prefix(dataset)
    return list(DOWNLOAD_DIR.glob(f"{prefix}*.nc"))


def get_zarr_path(dataset: dict[str, Any]) -> Path | None:
    """Return the optimised zarr archive path if it exists."""
    prefix = _get_cache_prefix(dataset)
    optimized = DOWNLOAD_DIR / f"{prefix}.zarr"
    if optimized.exists():
        return optimized
    return None


def get_icechunk_path(dataset: dict[str, Any]) -> Path:
    """Return the Icechunk store path for a dataset."""
    prefix = _get_cache_prefix(dataset)
    return DOWNLOAD_DIR / f"{prefix}.icechunk"


def _validate_spatial_coverage(dataset: dict[str, Any], bbox: list[float] | None) -> None:
    """Raise HTTP 400 if the request bbox falls outside the dataset's declared extents."""
    extents = dataset.get("extents")
    if not extents or bbox is None:
        return
    spatial = extents.get("spatial")
    if not spatial:
        return
    cov_bbox = spatial.get("bbox")
    if not isinstance(cov_bbox, (list, tuple)) or len(cov_bbox) != 4:
        return
    cov_xmin, cov_ymin, cov_xmax, cov_ymax = cov_bbox
    xmin, ymin, xmax, ymax = bbox
    if ymin > cov_ymax or ymax < cov_ymin:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Dataset '{dataset['id']}' does not cover this extent. "
                f"Latitude coverage: {cov_ymin}°–{cov_ymax}°, "
                f"requested: {ymin}°–{ymax}°."
            ),
        )
    if xmin > cov_xmax or xmax < cov_xmin:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Dataset '{dataset['id']}' does not cover this extent. "
                f"Longitude coverage: {cov_xmin}°–{cov_xmax}°, "
                f"requested: {xmin}°–{xmax}°."
            ),
        )


def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    """Import and return a function given its dotted module path."""
    parts = full_path.split(".")
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]
    module = importlib.import_module(module_path)
    return getattr(module, function_name)  # type: ignore[no-any-return]


def _resolve_bbox(*, bbox: list[float] | None) -> list[float]:
    """Resolve bbox from request or environment."""
    if bbox is not None:
        return bbox

    env_bbox = _bbox_from_env()
    if env_bbox is not None:
        return env_bbox

    raise ValueError(
        "A bbox is required for this dataset. Provide it in the request or set DOWNLOAD_BBOX in the environment."
    )


def _bbox_from_env() -> list[float] | None:
    """Parse a default bbox from environment if configured."""
    raw_bbox = os.getenv("DOWNLOAD_BBOX") or os.getenv("DEFAULT_DOWNLOAD_BBOX")
    if not raw_bbox:
        return None

    parts = [part.strip() for part in raw_bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("DOWNLOAD_BBOX must contain four comma-separated numbers: xmin,ymin,xmax,ymax")
    return [float(part) for part in parts]
