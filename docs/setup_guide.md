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
extent:
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]
  country_code: RWA

data_dir: ./data
```

Field reference:

| Field          | Required | Description |
| -------------- | -------- | ----------- |
| `name`         | No  | Human-readable name shown in API responses |
| `bbox`         | Yes | Bounding box as `[xmin, ymin, xmax, ymax]` in WGS84 decimal degrees |
| `country_code` | No  | ISO 3166-1 alpha-3 code — required for WorldPop downloads |

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

For ERA5-Land downloads see [ERA5-Land setup](#era5-land-via-destine-earth-data-hub) below.

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

## ERA5-Land via DestinE Earth Data Hub

ERA5-Land hourly temperature and precipitation data is downloaded from the [DestinE Earth Data Hub](https://earthdatahub.destine.eu). Access is free but requires registration.

### 1. Register

Create a free account at [earthdatahub.destine.eu](https://earthdatahub.destine.eu). Free accounts include a monthly request limit of 500,000 — sufficient for national-scale downloads.

### 2. Configure authentication

DestinE authentication uses a `.netrc` file in your home directory. Create or append to `~/.netrc`:

```
machine data.earthdatahub.destine.eu
login edh
password <your-personal-access-token>
```

The `login` is the literal string `edh`. The `password` is a personal access token, not your account password — generate one at `earthdatahub.destine.eu/account-settings` under "My Personal Access Tokens"

Set the correct permissions:

```bash
chmod 600 ~/.netrc
```

**Windows:** Python's `netrc` module looks for `_netrc` (underscore) instead of `.netrc`. Create the file as `%USERPROFILE%\_netrc` with the same content. The `chmod` step is not needed on Windows.

### 3. Ingest ERA5-Land data

Once authenticated, ingest ERA5-Land temperature for Rwanda:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "era5land_temperature_hourly",
    "start": "2024-01-01T00",
    "end": "2024-01-31T23",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

ERA5-Land data has a configured lag of 120 hours (5 days) — the sync planner will not request data from the last 120 hours. This can be adjusted by placing a custom `era5_land.yaml` in `plugins_dir/datasets/` — see `adding_custom_datasets.md`.

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
