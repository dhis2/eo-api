# Raster and coordinate conventions

Two different standards apply to two different things, and confusing them is how rasters end
up transposed or mislocated. This page settles which applies where, and what the framework
guarantees so that plugin authors do not have to.

## The two orders, and why they don't conflict

| Concern                                | Order                            | Standard                                       |
| -------------------------------------- | -------------------------------- | ---------------------------------------------- |
| Raster array / data cube dimensions    | `(…, y, x)` — x last             | NumPy row-major, CF `T,Z,Y,X`, GeoZarr, GDAL   |
| GeoJSON geometry coordinate pairs      | `[x, y]` = `[lon, lat]`          | RFC 7946                                       |
| Bounding boxes                         | `[xmin, ymin, xmax, ymax]`       | RFC 7946 (`[west, south, east, north]`)        |

These are not in tension — they describe different objects. `(…, y, x)` is the shape of an
N-dimensional array, where the row index comes before the column index because that is how
row-major memory is laid out. `[x, y]` is a coordinate *tuple*, where longitude comes first
because RFC 7946 says so. A single codebase uses both, in their own places.

So: **array dimensions are `(…, y, x)`; coordinate variables are named `x` / `y`; GeoJSON
pairs and bboxes are `[x, y]`-ordered.**

openEO, which the processing layer is built on, is deliberately neutral here — it addresses
dimensions by *name* (`reduce_dimension(dimension="t")`) and spatial extents by *field*
(`west`/`east`/`south`/`north`), so the question never arises at the API level. But the
reference implementation runs on xarray, which is NumPy/CF-ordered, so every cube flowing
through the engine is physically `(…, y, x)` regardless.

## What the framework guarantees

These are properties of a *published store*, not of any one source, so they are enforced once
at the write boundary — see [`shared/raster_contract.py`](https://github.com/dhis2/open-climate-service/blob/main/open_climate_service/shared/raster_contract.py).
A plugin may return data in whatever orientation and naming its source delivers.

1. **The temporal dimension is named `t`.** Sources spelling it `time`, `valid_time`, `date`
   or `time_counter` are renamed. The map viewer looks for `t`; a store that ships `time`
   renders with no time control.
2. **Spatial dimensions are named `y` / `x` and ordered `(…, y, x)`.** Variables arriving in
   another order are transposed.
3. **A geographic x axis runs −180…180.** Sources on the 0…360 frame (ERA5 among them) are
   rolled and re-sorted. Projected eastings are left alone — one legitimately exceeds 180.
4. **The declared CRS matches the coordinates it describes.** Data is stored in its *native*
   CRS, whatever the source delivered; nothing is reprojected on ingest. The invariant is
   consistency, not a fixed CRS. The instance-wide `crs:` setting is never used as a fallback
   for an untagged cube — doing so stamped `EPSG:32633` onto degree coordinates and put the
   store at the projection's origin instead of on the map.

## What is *not* guaranteed: the direction of `y`

A store keeps the `y` direction its source delivered. Most rasters are north-up, so `y`
usually descends; some sources (ERA5 among them) ascend. **A reader must honour the `y`
coordinate array rather than assume a direction.**

This is a deliberate choice, and it used to be the other way. Stores were normalised to
ascending because the map viewer's `zarr-layer` required it. As of 0.6.1 it detects the
direction from the `y` coordinate array instead, and OCS stores always take its "untiled"
path, so both directions render correctly. Normalising would mean reordering every raster's
rows on ingest, and would make the GDAL `GeoTransform` written alongside south-up (positive
`stepY`) — unusual for the GDAL and QGIS consumers of the same store.

`aggregate_spatial` is the model for a consumer doing this properly: it rasterises a geometry
mask top-row-first as rasterio does, then flips it only when the cube's `y` ascends.

## Where the two worlds meet

Points to be careful at, all currently consistent:

- **Bbox unpacking** is always `xmin, ymin, xmax, ymax`, and rasterio's `from_bounds` takes
  the same order followed by `width, height`.
- **`out_shape` is `(height, width)`** for mask rasterisation — array order, not bbox order.
- **GeoZarr's `spatial:dimensions` and `spatial:shape` are positional**: the second-to-last
  entry is the row axis and the last is the column axis, so they are `["y", "x"]` and
  `[height, width]`. See [Zarr and GeoZarr](zarr_and_geozarr.md#geozarr-root-attributes).
- **`spatial:transform` carries the sign of the y step**, negative for a north-up grid. That
  is what lets a reader place the raster without assuming a direction.
