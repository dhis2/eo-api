"""Read a dataset's own CRS.

Data is stored in its **native** CRS — the CRS the source provides it in — never
the instance-wide config CRS. These helpers recover that native CRS from a
dataset so that ingestion writes it, and serving (STAC, coverage) reports it,
without ever consulting ``api_config.get_crs()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import xarray as xr


def dataset_crs(ds: "xr.Dataset", default: str = "EPSG:4326") -> str:
    """Return *ds*'s own CRS as an ``EPSG:xxxx`` string.

    Prefers the GeoZarr ``proj:code`` / ``proj:epsg`` root attribute written at
    ingest, then the rioxarray-detected CRS, falling back to *default*
    (WGS84). The instance config CRS is deliberately never consulted — every
    dataset keeps the CRS its source delivered it in.
    """
    code = ds.attrs.get("proj:code") or ds.attrs.get("proj:epsg")
    if code:
        return str(code)
    try:
        import rioxarray  # noqa: F401  # pyright: ignore[reportUnusedImport]

        rio_crs = ds.rio.crs
        if rio_crs is not None:
            epsg = rio_crs.to_epsg()
            if epsg:
                return f"EPSG:{epsg}"
    except Exception:
        pass
    return default
