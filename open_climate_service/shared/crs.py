"""Read a dataset's own CRS.

Data is stored in its **native** CRS — the CRS the source provides it in — never
the instance-wide config CRS. These helpers recover that native CRS from a
dataset so that ingestion writes it, and serving (STAC, coverage) reports it,
without ever consulting ``api_config.get_crs()``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import xarray as xr


# CRSes that map clients (proj4js, GDAL) resolve from the authority code alone; any
# other CRS needs a full definition (proj4 / WKT) surfaced to the client. CRS84 aliases
# are normalized to EPSG:4326 before this check (see canonical_crs_code).
_BUILTIN_CRS_CODES = frozenset({"EPSG:4326", "EPSG:3857"})


def canonical_crs_code(code: str | int) -> str:
    """Collapse CRS84 geographic aliases to the canonical ``EPSG:4326``.

    A CRS84 alias (``CRS84``, ``OGC:CRS84``, the short OGC form ``CRS:84``, or a CRS84
    URI) is geographic WGS84, but map reprojectors resolve only ``EPSG:4326`` /
    ``EPSG:3857`` from a code — so mapping the alias to ``EPSG:4326`` lets every consumer
    work with one canonical code. Separators are stripped before matching so ``CRS:84`` is
    caught too. A bare EPSG number (the int ``4326`` or the string ``"4326"``) is prefixed
    to ``EPSG:4326`` so downstream checks like :func:`is_builtin_crs` see a full code; any
    other input is returned unchanged (as a string).
    """
    s = str(code).strip()
    if s.isdigit():
        return f"EPSG:{s}"
    return "EPSG:4326" if re.sub(r"[^A-Z0-9]", "", s.upper()).endswith("CRS84") else s


def is_builtin_crs(code: str | int) -> bool:
    """True for a CRS a client resolves from the code alone (EPSG:4326 / EPSG:3857).

    CRS84 aliases are normalized to EPSG:4326 first.
    """
    return canonical_crs_code(code).upper() in _BUILTIN_CRS_CODES


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
