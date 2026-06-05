# Setup Guide

This guide walks through configuring a new Open Climate Service instance for a specific country, using Rwanda as the example.

## Prerequisites

- Python 3.13 or higher
- [uv](https://docs.astral.sh/uv/) for dependency management
- Git
- Make (`make` — available by default on macOS and most Linux distributions; on Windows use [WSL](https://learn.microsoft.com/en-us/windows/wsl/) or run the commands in the Makefile directly)
- [jq](https://jqlang.org/download/) for pretty-printing API responses in the curl examples (optional — omit `| jq` if not installed)

## Step 1: Clone and install

```bash
git clone https://github.com/dhis2/open-climate-service.git
cd open-climate-service
make sync
```

## Step 2: Configure the spatial extent

The repo includes `climate-service.yaml.example` with Sierra Leone as a starting point. Copy it to `climate-service.yaml` (which is gitignored so your local extent stays out of version control) and replace the entry with your country:

```bash
cp climate-service.yaml.example climate-service.yaml
```

```yaml
id: rwanda-climate-service
name: Rwanda Climate Service

extent:
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]
  country_code: RWA

data_dir: ./data
```

Field reference:

| Field          | Required | Description |
| -------------- | -------- | ----------- |
| `id`           | No  | Unique instance identifier used as the STAC catalog id. Lowercase, hyphen-separated. Defaults to `open-climate-service` |
| `name`         | No  | Display name shown in the web UI. Defaults to `Open Climate Service` |
| `extent.name`  | No  | Human-readable label shown in API responses |
| `bbox`         | Yes | Bounding box as `[xmin, ymin, xmax, ymax]` in WGS84 decimal degrees |
| `country_code` | No  | ISO 3166-1 alpha-3 code — required for WorldPop downloads |
| `utc_offset_hours` | No  | UTC offset in hours (e.g. `3` for East Africa). Affects daily datasets computed from hourly data — the "local day" is shifted accordingly. Defaults to `0` (UTC) |

`data_dir` sets the directory where downloaded NetCDF files and Zarr stores are kept. It is required when a config file is present and is resolved relative to the config file. Each instance must have its own `data_dir` to avoid mixing data between deployments.

To find the bounding box for a country, [bboxfinder.com](http://bboxfinder.com) is a useful tool.

Values can reference environment variables using `${VAR:-default}` syntax:

```yaml
extent:
  name: ${EXTENT_NAME:-Rwanda}
  bbox: [28.8, -2.9, 30.9, -1.0]
```

## Step 3: Configure environment variables

Copy the example environment file:

```bash
cp .env.example .env
```

`CLIMATE_SERVICE_CONFIG=./climate-service.yaml` is already set in `.env.example`. The remaining defaults are sufficient to run the API and ingest CHIRPS3 and WorldPop data. Review the file and adjust as needed — the comments explain each variable.

For ERA5-Land downloads see [ERA5-Land datasets](era5_land_datasets.md).

## Step 4: Start the API

```bash
make run
```

The API starts on `http://127.0.0.1:8000`. Open `http://127.0.0.1:8000/docs` for the interactive API documentation.

Alternatively, if the package is installed (e.g. via `pip install .`), you can start it with:

```bash
climate-service
```

When running the `climate-service` command from a directory other than the repo root, the relative path `./climate-service.yaml` in `CLIMATE_SERVICE_CONFIG` will not resolve correctly. Use an absolute path in that case:

```bash
CLIMATE_SERVICE_CONFIG=/path/to/your/climate-service.yaml climate-service
```

## Step 5: Verify the configured extent

```bash
curl -s http://127.0.0.1:8000/extent | jq
```

Expected response:

```json
{
  "name": "Rwanda",
  "description": null,
  "bbox": [28.8, -2.9, 30.9, -1.0]
}
```

## Step 6: Ingest your first dataset

CHIRPS3 (daily precipitation) requires no API key and is a good first dataset to verify the setup.

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

A successful response returns `"status": "completed"` and a `dataset` object with `dataset_id` of `chirps3_precipitation_daily`.

## Step 7: Access the data

Browse the STAC catalog to confirm the dataset is published:

```bash
curl -s http://127.0.0.1:8000/stac/catalog.json | jq
```

Open the dataset with xarray — the catalog discovery picks up whichever extent you configured:

```python
import httpx
import xarray as xr

catalog = httpx.get("http://127.0.0.1:8000/stac/catalog.json").json()
children = [link for link in catalog["links"] if link["rel"] == "child"]
collection_url = children[0]["href"]

collection = httpx.get(collection_url).json()
asset = collection["assets"]["zarr"]
ds = xr.open_zarr(
    asset["href"],
    consolidated=asset["xarray:open_kwargs"]["consolidated"],
)
print(ds)
```

See [user_guide.md](user_guide.md) for more usage examples.

---

## ERA5-Land datasets

ERA5-Land temperature and precipitation data requires registration with the DestinE Earth Data Hub and/or the Copernicus Climate Data Store. See **[ERA5-Land datasets](era5_land_datasets.md)** for setup instructions and the full list of available datasets.

---

## Keeping datasets up to date

Use the sync endpoint to advance an existing dataset to the latest available data:

```bash
# Check what would be downloaded without executing
curl -s "http://127.0.0.1:8000/sync/chirps3_precipitation_daily/plan" | jq

# Execute the sync
curl -s -X POST "http://127.0.0.1:8000/sync/chirps3_precipitation_daily" \
  -H "Content-Type: application/json" \
  -d '{"prefer_zarr": true, "publish": true}' | jq
```

See [managed_data_api_guide.md](managed_data_api_guide.md) for the full sync API reference.

See [adding_custom_datasets.md](adding_custom_datasets.md) for adding new dataset sources beyond the built-in templates.
