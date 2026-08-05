"""The layout contract every published store must satisfy, enforced at the write boundary.

Dimension naming, axis order and CRS consistency are all *ingest-normalisation invariants*:
properties of a published GeoZarr, not of any one source. Enforced per-plugin they get
forgotten one plugin at a time — that is how datasets have shipped on a `time` dim the map
viewer could not find, and with a projected CRS stamped over geographic coordinates. So they
are enforced here instead, once, and plugins may return data in whatever orientation their
source delivers (see CLIM-821).

The contract:

1. the temporal dimension is named ``t``
2. spatial dims are named ``y`` / ``x`` and ordered ``(…, y, x)``
3. for a geographic CRS, longitudes run −180…180 rather than 0…360
4. the declared CRS matches the coordinates it describes

**Not** in the contract: the direction of ``y``. The ticket originally required ascending
(south→north) because the map viewer's zarr-layer did; as of 0.6.1 it detects the direction
from the ``y`` coordinate array instead (``detectedLatAscending = y1 > y0``), and OCS stores
always take its "untiled" path, so either direction renders. Stores therefore keep the
direction their source delivered — which for most rasters is north-up, matching GDAL's
convention for the ``GeoTransform`` written alongside. A reader must honour the ``y``
coordinate rather than assume a direction.

Axis order here is *array* order, ``(…, y, x)`` — NumPy row-major, CF's ``T,Z,Y,X``, and what
xarray/rioxarray/GDAL/GeoZarr all assume. It does not conflict with GeoJSON's ``[x, y]``
coordinate pairs and ``[west, south, east, north]`` bboxes, which stay as RFC 7946 requires:
those describe coordinate tuples, not an N-D array. See docs/conventions.md.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import xarray as xr

logger = logging.getLogger(__name__)

# Source spellings for the temporal dimension, mapped onto the canonical ``t``. The map
# viewer looks for `t`; a store that ships `time` renders without a time control.
_TIME_ALIASES = ("time", "valid_time", "date", "time_counter")

# Beyond this longitude a geographic x axis is on the 0…360 frame rather than −180…180.
_LON_ROLL_THRESHOLD = 180.0


def normalize_dim_layout(
    ds: "xr.Dataset",
    *,
    time_dim: str = "t",
    x_dim: str = "x",
    y_dim: str = "y",
) -> "xr.Dataset":
    """Rename the temporal dimension to ``time_dim`` and order spatial dims ``(…, y, x)``.

    Renames only coordinate/dimension *names* and axis order — no coordinate value changes —
    so this is safe to apply to a single period being appended to an existing store as well
    as to a whole-store rewrite. Longitude rolling, which does change values, is separate
    (:func:`normalize_longitudes`).

    A no-op when the dataset already conforms, so the common case costs one dims comparison
    rather than a copy of the data.
    """
    if time_dim not in ds.dims:
        for alias in _TIME_ALIASES:
            if alias in ds.dims or alias in ds.coords:
                ds = ds.rename({alias: time_dim})
                logger.info("Renamed temporal dimension %r to %r", alias, time_dim)
                break

    if x_dim not in ds.dims or y_dim not in ds.dims:
        # Nothing to order — the caller (orchestrator or downloader) raises a clearer error
        # about the missing spatial dims than a transpose would.
        return ds

    for name, var in ds.data_vars.items():
        dims = tuple(str(d) for d in var.dims)
        if y_dim not in dims or x_dim not in dims:
            continue  # a non-spatial variable, e.g. a scalar CRS holder
        if dims[-2:] == (y_dim, x_dim):
            continue
        leading = [d for d in dims if d not in (y_dim, x_dim)]
        ds[name] = var.transpose(*leading, y_dim, x_dim)
        logger.info(
            "Transposed %r from %s to (…, %s, %s) — array order is row-major",
            str(name),
            dims,
            y_dim,
            x_dim,
        )
    return ds


def normalize_longitudes(ds: "xr.Dataset", *, x_dim: str = "x", crs: str | None = None) -> "xr.Dataset":
    """Roll a geographic x axis from 0…360 to −180…180 and sort it ascending.

    Only for a geographic CRS — an easting in a projected CRS is legitimately larger than 180
    and must not be touched. This *does* change coordinate values and reorders the array, so
    it belongs to a whole-store rewrite, not to a period being appended to a store whose x
    axis is already fixed.

    A no-op when no longitude exceeds 180, which is the case for every source that already
    publishes on the −180…180 frame.
    """
    if x_dim not in ds.coords:
        return ds
    if crs is not None and _axis_units(crs) == "linear":
        return ds

    x = ds[x_dim]
    try:
        needs_roll = bool((x > _LON_ROLL_THRESHOLD).any())
    except (TypeError, ValueError):
        return ds
    if not needs_roll:
        return ds

    logger.info("Rolling %r from the 0…360 frame to −180…180", x_dim)
    rolled = ds.assign_coords({x_dim: ((ds[x_dim] + 180.0) % 360.0) - 180.0})
    return rolled.sortby(x_dim)


def _axis_units(crs: str | int) -> str | None:
    """``"degrees"`` for a lon/lat CRS, ``"linear"`` for a projected one, ``None`` if unknown.

    The three-way answer matters: a CRS that pyproj cannot resolve — a polluted host PROJ
    database is a common developer-machine failure, see ``startup._ensure_proj_database`` — must
    come back as *unknown* rather than being lumped in with projected. Treating an unresolvable
    CRS as projected would let a perfectly good geographic one (ETRS89, say) be overridden with
    EPSG:4326 purely because the database was unreadable.
    """
    from open_climate_service.shared.crs import canonical_crs_code

    code = canonical_crs_code(crs)
    if code == "EPSG:4326":
        return "degrees"
    try:
        from pyproj import CRS

        return "degrees" if CRS.from_user_input(code).is_geographic else "linear"
    except Exception:  # noqa: BLE001 — unresolvable: report unknown, never guess "projected"
        logger.debug("Could not resolve CRS %s to decide its axis units", code, exc_info=True)
        return None


def _looks_geographic(ds: "xr.Dataset", *, x_dim: str, y_dim: str) -> bool:
    """True when the coordinate magnitudes can only be degrees.

    Latitude carries the argument: it fits ±90, where a projected northing runs to millions
    within a few hundred metres of the origin. Longitude is allowed up to 360 rather than 180
    because a grid still on the 0…360 frame has not been rolled yet — testing it against 180
    would make this return False for exactly the datasets that need rolling, and the roll needs
    this answer to know the axis is a longitude at all.

    Deliberately one-directional: this answers "these cannot be metres", never "these cannot be
    degrees". A projected grid confined to a few hundred metres either side of its own origin
    would pass, but no real dataset extent is that small.
    """
    try:
        x = ds[x_dim].values
        y = ds[y_dim].values
        return bool(x.min() >= -180.0 and x.max() <= 360.0 and abs(y).max() <= 90.0)
    except (KeyError, ValueError, TypeError):
        return False


def resolve_store_crs(
    ds: "xr.Dataset",
    declared: str | None = None,
    *,
    x_dim: str = "x",
    y_dim: str = "y",
) -> str:
    """The CRS to write for *ds*, refusing a declared CRS its coordinates contradict.

    ``declared`` is whatever the caller was going to write. When it is a projected CRS but the
    coordinates can only be degrees, it is wrong — a projected code over geographic
    coordinates puts the store at its projection's origin rather than on the map — so the
    dataset's own CRS is preferred and the contradiction logged with both values.

    Never consults the instance config CRS. That is how the contradiction arises in the first
    place: an untagged cube picks up the instance's ``crs:`` (e.g. ``EPSG:32633`` for Norway)
    regardless of where its coordinates actually are.
    """
    from open_climate_service.shared.crs import canonical_crs_code, dataset_crs

    if declared is None:
        return canonical_crs_code(dataset_crs(ds))

    code = canonical_crs_code(declared)
    # Override only on a definite contradiction: the declared CRS is known to be in linear
    # units and the coordinates can only be degrees. An unresolvable CRS is left alone.
    if _axis_units(code) != "linear" or not _looks_geographic(ds, x_dim=x_dim, y_dim=y_dim):
        return code

    from_data = canonical_crs_code(dataset_crs(ds))
    logger.warning(
        "Declared CRS %s is projected but the coordinates are within ±180/±90 "
        "(x %.4f…%.4f, y %.4f…%.4f) — they can only be degrees. Using %s from the data "
        "instead; a projected code over geographic coordinates puts the store off the map.",
        code,
        float(ds[x_dim].min()),
        float(ds[x_dim].max()),
        float(ds[y_dim].min()),
        float(ds[y_dim].max()),
        from_data,
    )
    if _axis_units(from_data) == "linear":
        # The dataset's own CRS is projected too — a stale `proj:code` is copied forward on
        # every rewrite, so both sources can be wrong at once (Norway's WorldPop store has
        # EPSG:32633 at the root and EPSG:4326 on level 0). Neither agrees with the
        # coordinates, so fall back to plain WGS84 rather than propagating a known-bad code.
        logger.warning("Dataset CRS %s is also projected; falling back to EPSG:4326", from_data)
        return "EPSG:4326"
    return from_data


def published_contract_violations(
    ds: "xr.Dataset",
    *,
    time_dim: str = "t",
    x_dim: str = "x",
    y_dim: str = "y",
) -> list[str]:
    """Ways *ds* fails the published-store contract, as human-readable strings.

    Used by the regression tests to assert the contract on a written store rather than
    re-deriving it per test, and available to anyone debugging a store that renders oddly.
    Empty list means conformant. The direction of ``y`` is deliberately not checked — see the
    module docstring.
    """
    problems: list[str] = []
    dims = set(str(d) for d in ds.dims)
    if time_dim not in dims:
        found = [a for a in _TIME_ALIASES if a in dims]
        problems.append(f"temporal dimension is not named {time_dim!r}" + (f" (found {found[0]!r})" if found else ""))
    for expected in (y_dim, x_dim):
        if expected not in dims:
            problems.append(f"spatial dimension {expected!r} is missing")
    for name, var in ds.data_vars.items():
        var_dims = tuple(str(d) for d in var.dims)
        if y_dim in var_dims and x_dim in var_dims and var_dims[-2:] != (y_dim, x_dim):
            problems.append(f"variable {str(name)!r} has dims {var_dims}, not (…, {y_dim}, {x_dim})")
    declared: Any = ds.attrs.get("proj:code")
    units = _axis_units(str(declared)) if declared else None
    if declared and units == "linear" and _looks_geographic(ds, x_dim=x_dim, y_dim=y_dim):
        problems.append(f"declared CRS {declared} is projected but the coordinates are degrees")
    if declared and units == "degrees" and x_dim in ds.coords:
        try:
            if bool((ds[x_dim] > _LON_ROLL_THRESHOLD).any()):
                problems.append(f"geographic {x_dim!r} exceeds 180 — still on the 0…360 frame")
        except (TypeError, ValueError):
            pass
    return problems
