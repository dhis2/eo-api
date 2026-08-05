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
3. **`y` descends — array row 0 is the northernmost row.** South-up sources are reversed,
   coordinate and data together, so the value at a given latitude does not move; only its row
   index does. See below for why this is guaranteed rather than left to the reader.
4. **A geographic x axis runs −180…180.** Sources on the 0…360 frame (ERA5 among them) are
   rolled and re-sorted. Projected eastings are left alone — one legitimately exceeds 180.
5. **The declared CRS matches the coordinates it describes.** Data is stored in its *native*
   CRS, whatever the source delivered; nothing is reprojected on ingest. The invariant is
   consistency, not a fixed CRS. The instance-wide `crs:` setting is never used as a fallback
   for an untagged cube — doing so stamped `EPSG:32633` onto degree coordinates and put the
   store at the projection's origin instead of on the map.

## Why the direction of `y` is guaranteed

Because real consumers assume one and never check. Two that matter:

**OpenLayers' `ol/source/GeoZarr`** — the renderer STAC Browser uses. It derives its tile grid
origin as the top-left corner and counts rows down from it:

```js
const origin = [extent[0], extent[3]];                                  // top-left
const minRow = Math.round((origin[1] - tileExtent[3]) / bandResolution);
... get(array, [slice(minRow, maxRow), colSlice])                        // straight into the array
```

There is no `reverse`, no `flip`, and no read of `spatial:transform`'s y step anywhere in that
file, so array row 0 is always painted at the north edge. Worked through for a 3°-tall grid of
0.05° rows:

```
OL asks for array row 0 to paint the northern tile (lat ~59.975)
  descending store: row 0 holds lat 59.975  -> CORRECT
  ascending  store: row 0 holds lat 57.025  -> WRONG, mirrored by 2.95 deg
```

**Result thumbnails** render with `imshow(..., origin="upper")`, which is the same assumption.

Descending rather than ascending, given the choice:

| | descending | ascending |
| --- | --- | --- |
| OpenLayers / STAC Browser | correct | mirrored, undetectably |
| carbonplan/zarr-layer (the `/map` viewer) | detects from the coordinate | detects from the coordinate |
| GDAL `GeoTransform` | negative y step — the north-up convention | positive — legal but unusual |
| cost for a typical source | none, already north-up | reverses every raster |

zarr-layer detects the direction either way (`detectedLatAscending = y1 > y0`), so it does not
constrain the choice; OpenLayers does.

A consumer should still prefer reading the `y` coordinate over assuming — `aggregate_spatial`
does exactly that, flipping its rasterio mask only when `y` ascends, and stays correct whatever
it is handed.

## Appending to a store written before this contract

Invariants 3 and 4 reorder the array. An append writes along the time axis only, so a store's
committed coordinate arrays stay as they are — and a period normalised one way, appended to a
store written the other, would put rows or columns under coordinates that no longer describe
them. That is silently mirrored data, worse than metadata being out of date.

So an ingest checks the committed axes before its first append and **fails with an actionable
message** rather than guessing. The fix is a re-ingest, which rebuilds the store under the
current contract.

## Where the two worlds meet

Points to be careful at, all currently consistent:

- **Bbox unpacking** is always `xmin, ymin, xmax, ymax`, and rasterio's `from_bounds` takes
  the same order followed by `width, height`.
- **`out_shape` is `(height, width)`** for mask rasterisation — array order, not bbox order.
- **GeoZarr's `spatial:dimensions` and `spatial:shape` are positional**: the second-to-last
  entry is the row axis and the last is the column axis, so they are `["y", "x"]` and
  `[height, width]`. See [Zarr and GeoZarr](zarr_and_geozarr.md#geozarr-root-attributes).
- **`spatial:transform` carries the sign of the y step**, always negative now that stores are
  guaranteed north-up. A reader that honours it stays correct even for data from elsewhere.
