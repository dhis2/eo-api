# API Examples

Base URL (local):

http://127.0.0.1:8000

OGC landing page:

http://127.0.0.1:8000/

```bash
curl "http://127.0.0.1:8000/"
```

## Collections (`/collections`)

Conformance declaration:

http://127.0.0.1:8000/conformance

```bash
curl "http://127.0.0.1:8000/conformance"
```

Note: values used in `range-subset` and `parameter-name` must match keys in `datasets/<dataset-id>.yaml` under `parameters`.

List collections:

http://127.0.0.1:8000/collections

```bash
curl "http://127.0.0.1:8000/collections"
```

Get CHIRPS collection:

http://127.0.0.1:8000/collections/chirps-daily

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily"
```

Get ERA5-Land collection:

http://127.0.0.1:8000/collections/era5-land-daily

Collections in this section correspond to OGC API - Common collection discovery endpoints.

Get CHIRPS coverage (default extent/time):

http://127.0.0.1:8000/collections/chirps-daily/coverage

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/coverage"
```

Get CHIRPS coverage for a specific datetime and bbox:

http://127.0.0.1:8000/collections/chirps-daily/coverage?datetime=2026-01-31T00:00:00Z&bbox=30,-5,35,2

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/coverage?datetime=2026-01-31T00:00:00Z&bbox=30,-5,35,2"
```

Get ERA5-Land coverage for a range-subset parameter:

http://127.0.0.1:8000/collections/era5-land-daily/coverage?range-subset=2m_temperature

```bash
curl "http://127.0.0.1:8000/collections/era5-land-daily/coverage?range-subset=2m_temperature"
```

Get CHIRPS EDR position query:

http://127.0.0.1:8000/collections/chirps-daily/position?coords=POINT(30%20-1)&datetime=2026-01-31T00:00:00Z&parameter-name=precip

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/position?coords=POINT(30%20-1)&datetime=2026-01-31T00:00:00Z&parameter-name=precip"
```

Get ERA5-Land EDR position query:

http://127.0.0.1:8000/collections/era5-land-daily/position?coords=POINT(36.8%20-1.3)&parameter-name=2m_temperature

```bash
curl "http://127.0.0.1:8000/collections/era5-land-daily/position?coords=POINT(36.8%20-1.3)&parameter-name=2m_temperature"
```

Get CHIRPS EDR area query:

http://127.0.0.1:8000/collections/chirps-daily/area?bbox=30,-5,35,2&datetime=2026-01-31T00:00:00Z&parameter-name=precip

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/area?bbox=30,-5,35,2&datetime=2026-01-31T00:00:00Z&parameter-name=precip"
```

Get ERA5-Land EDR area query:

http://127.0.0.1:8000/collections/era5-land-daily/area?bbox=36,-2,38,0&parameter-name=2m_temperature

```bash
curl "http://127.0.0.1:8000/collections/era5-land-daily/area?bbox=36,-2,38,0&parameter-name=2m_temperature"
```

## COG (`/cog`)

COG info:

http://127.0.0.1:8000/cog/info?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif

```bash
curl "http://127.0.0.1:8000/cog/info?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif"
```

COG preview:

http://127.0.0.1:8000/cog/preview.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&max_size=2048&colormap_name=delta

```bash
curl -o chirps-preview.png "http://127.0.0.1:8000/cog/preview.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&max_size=2048&colormap_name=delta"
```

Tile:

http://127.0.0.1:8000/cog/tiles/WebMercatorQuad/4/5/5.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&colormap_name=delta

```bash
curl -o chirps-tile.png "http://127.0.0.1:8000/cog/tiles/WebMercatorQuad/4/5/5.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&colormap_name=delta"
```

CHIRPS COG test file:

https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/2026/chirps-v3.0.rnl.2026.01.31.tif
