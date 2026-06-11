"""Write raster datasets to Icechunk stores with GeoZarr conventions."""

import logging
import os
from pathlib import Path
from typing import Any

import xarray as xr
import xproj  # noqa: F401  # type: ignore[import-untyped]  # pyright: ignore[reportUnusedImport]
import zarr
from geozarr_toolkit import MultiscalesConventionMetadata, create_geozarr_attrs
from topozarr.coarsen import create_pyramid

from open_climate_service import config as api_config

logger = logging.getLogger(__name__)


def _resolve_download_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "downloads"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-service" / "downloads"


DOWNLOAD_DIR = _resolve_download_dir()


_PYRAMID_PIXEL_THRESHOLD = 2048 * 2048
_PYRAMID_MAX_LEVELS = 8
_PYRAMID_TARGET_TILE_SIZE = 512
_ROOT_TIME_COORD_MAX_CHUNK = 4096


def _needs_pyramid(ds: xr.Dataset, x_dim: str, y_dim: str) -> bool:
    """Return True when the spatial extent is large enough to benefit from a pyramid."""
    return ds.sizes[x_dim] * ds.sizes[y_dim] > _PYRAMID_PIXEL_THRESHOLD


def _pyramid_levels(ds: xr.Dataset, x_dim: str, y_dim: str) -> int:
    """Compute the number of pyramid levels needed to reach a manageable tile size."""
    import math

    max_dim = max(ds.sizes[x_dim], ds.sizes[y_dim])
    levels = math.ceil(math.log2(max_dim / _PYRAMID_TARGET_TILE_SIZE))
    return max(2, min(levels, _PYRAMID_MAX_LEVELS))


def _write_root_time_coordinate(zarr_store: "Path | Any", ds: xr.Dataset, *, time_dim: str) -> None:
    """Expose the time coordinate at the pyramid root with bounded chunking for browser clients.

    zarr_store may be a filesystem Path (plain Zarr) or a zarr-compatible store object
    (e.g. an Icechunk session store).
    """
    if time_dim not in ds.dims:
        logger.debug("Skipping root time coordinate write: no %s dimension found", time_dim)
        return
    if time_dim not in ds.coords or ds.sizes.get(time_dim, 0) == 0:
        logger.debug("Skipping root time coordinate write: empty or missing %s coordinate", time_dim)
        return

    store_arg = str(zarr_store) if isinstance(zarr_store, Path) else zarr_store
    root = zarr.open_group(store_arg, mode="a", zarr_format=3)
    root_attrs = dict(root.attrs)
    if time_dim in root:
        del root[time_dim]
    time_coord = xr.Dataset(coords={time_dim: ds[time_dim]})
    time_coord.to_zarr(
        store_arg,
        mode="a",
        zarr_format=3,
        consolidated=False,
        encoding={time_dim: {"chunks": (min(ds.sizes[time_dim], _ROOT_TIME_COORD_MAX_CHUNK),)}},
    )
    root = zarr.open_group(store_arg, mode="a", zarr_format=3)
    root.attrs.update(root_attrs)


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


def needs_pyramid(ds: xr.Dataset) -> bool:
    """Return True when the dataset is large enough to need a multiscale pyramid."""
    return _needs_pyramid(ds, "x", "y")


def write_to_icechunk_store(
    ds: xr.Dataset,
    store_path: Path,
    x_dim: str = "x",
    y_dim: str = "y",
    t_dim: str | None = "t",
    *,
    crs: str | None = None,
    commit_message: str = "Materialized dataset",
) -> None:
    """Write *ds* to an Icechunk store, building a multiscale pyramid when needed.

    Applies GeoZarr conventions throughout. Creates the store if it does not exist;
    overwrites any existing content in the new commit.

    Three CRS calls are required: ``proj.assign_crs`` sets the xproj CRS needed by
    topozarr, ``rio.write_crs`` populates ``spatial_ref`` attrs and encodes
    ``grid_mapping`` per variable, then ``proj.assign_crs`` again because
    ``rio.write_crs`` destroys xproj CRS detection.
    """
    import icechunk
    import rioxarray as _rxr  # noqa: F401  # pyright: ignore[reportUnusedImport]  # activates .rio accessor

    if crs is None:
        crs = api_config.get_crs()

    dims = [x_dim, y_dim]
    xmin = float(ds[x_dim].min())
    xmax = float(ds[x_dim].max())
    ymin = float(ds[y_dim].min())
    ymax = float(ds[y_dim].max())
    geozarr_attrs = create_geozarr_attrs(
        dimensions=dims,
        crs=crs,
        bbox=[xmin, ymin, xmax, ymax],
        shape=(ds.sizes[x_dim], ds.sizes[y_dim]),
    )

    ds = ds.proj.assign_crs(spatial_ref=crs)
    ds = ds.rio.write_crs(crs)
    ds = ds.proj.assign_crs(spatial_ref=crs)

    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.open_or_create(storage)
    session = repo.writable_session("main")

    if _needs_pyramid(ds, x_dim, y_dim):
        zarr_conventions = list(geozarr_attrs.get("zarr_conventions", []))
        zarr_conventions.append(MultiscalesConventionMetadata().model_dump())
        geozarr_attrs["zarr_conventions"] = zarr_conventions

        levels = _pyramid_levels(ds, x_dim, y_dim)
        logger.info(
            "Building %d-level pyramid into Icechunk store '%s' (dims %dx%d)",
            levels,
            store_path.name,
            ds.sizes[x_dim],
            ds.sizes[y_dim],
        )

        # topozarr.create_pyramid requires fully materialised numpy arrays.
        ds = ds.load()
        pyramid = create_pyramid(ds, levels=levels, x_dim=x_dim, y_dim=y_dim, method="mean")
        # Pyramid.write() writes the root group attributes from ``pyramid.attrs``,
        # so merge the GeoZarr conventions (multiscales / proj / spatial) in here.
        pyramid.attrs.update(geozarr_attrs)

        # Keep the no-data sentinel consistent across pyramid levels so the map
        # renders the same pixels transparent at every zoom. topozarr mean-coarsens
        # an integer-coded mask (e.g. a 0/1 hotspot raster, where 0 is the no-data
        # background) into float levels that would otherwise default to a NaN fill,
        # leaving the 0.0 background opaque at overview zooms. Pin the fill_value to
        # 0 for integer-coded variables (Pyramid.write applies it across all levels);
        # float variables already use NaN as both data and fill, so leave them be.
        for var_name, var_da in ds.data_vars.items():
            if var_da.dtype.kind != "f" and pyramid.fill_values.get(str(var_name)) is None:
                pyramid.fill_values[str(var_name)] = 0

        pyramid.write(session.store, mode="w")

        # topozarr demotes spatial_ref from coordinate to data variable in the pyramid.
        # Patch the root and each level group: add CRS to multiscales datasets entries so
        # ZarrLayer can detect the coordinate system without guessing (it defaults to
        # EPSG:3857 when crs is absent from the OME-NGFF datasets array).
        root = zarr.open_group(session.store, mode="a")
        root_attrs = dict(root.attrs)
        ms = root_attrs.get("multiscales")
        if isinstance(ms, list) and ms and isinstance(ms[0], dict):
            datasets = ms[0].get("datasets")
            if isinstance(datasets, list):
                for ds_entry in datasets:
                    if isinstance(ds_entry, dict):
                        ds_entry.setdefault("crs", crs)
                root_attrs["multiscales"] = ms
                root.attrs.update(root_attrs)
        for level_key in root.keys():
            if isinstance(root[level_key], zarr.Group):
                lvl_attrs = dict(root[level_key].attrs)
                lvl_attrs["coordinates"] = "spatial_ref"
                root[level_key].attrs.update(lvl_attrs)

        if t_dim is not None:
            _write_root_time_coordinate(session.store, ds, time_dim=t_dim)
    else:
        logger.info(
            "Writing flat Icechunk store '%s' (dims %dx%d)",
            store_path.name,
            ds.sizes[x_dim],
            ds.sizes[y_dim],
        )
        ds = ds.assign_attrs({**ds.attrs, **geozarr_attrs})
        ds.to_zarr(session.store, mode="w", zarr_format=3)

        # xarray demotes scalar coordinates (like spatial_ref) to data variables in zarr v3.
        # Patch the root group so rioxarray can follow the CF grid_mapping attribute.
        root_flat = zarr.open_group(session.store, mode="a")
        flat_attrs = dict(root_flat.attrs)
        flat_attrs["coordinates"] = "spatial_ref"
        root_flat.attrs.update(flat_attrs)

    session.commit(commit_message)
