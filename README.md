# Open Climate Service

An open-source platform that integrates data from many different sources to produce tailored climate services — to help make informed decisions, manage risks, and adapt to climate change.

> **Status: under active development.** APIs and data models may change without notice.

📖 **Documentation: <https://dhis2.github.io/open-climate-service/>**

Each instance is configured for a specific country or region: it scopes all data extraction, processing, and storage to that spatial extent, draws from sources such as CHIRPS, ERA5, and WorldPop, stores outputs as GeoZarr, and exposes them through open standards (STAC, Zarr over HTTP, openEO). It runs independently of DHIS2 and can be deployed on local, cloud-hosted, or sovereign country infrastructure.

## Install

```bash
pip install open-climate-service            # client only — talk to an instance over HTTP
pip install open-climate-service[xarray]    # + open published datasets as xarray
pip install open-climate-service[server]    # full server stack — run your own instance
```

## Quick start (client)

```python
from open_climate_service import ClimateService

service = ClimateService("https://my-instance.example.org")
datasets = service.datasets()                  # discover published collections
ds = service.open_dataset(datasets[0]["id"])   # open as xarray (needs the [xarray] extra)
```

## Run a server

To run your own instance, see the [setup guide](https://dhis2.github.io/open-climate-service/setup_guide/). In short:

```bash
uv sync --extra server
uv run uvicorn open_climate_service.main:app --reload
```

## Documentation

- [Get started](https://dhis2.github.io/open-climate-service/setup_guide/) — set up an instance and ingest data
- [Accessing data](https://dhis2.github.io/open-climate-service/user_guide/) — the Python client, STAC, and xarray
- [openEO](https://dhis2.github.io/open-climate-service/openeo/) — process graphs, workflows, and exports
- [API reference](https://dhis2.github.io/open-climate-service/managed_data_api_guide/)
- [Roadmap](https://dhis2.github.io/open-climate-service/roadmap/) · [Team](https://dhis2.github.io/open-climate-service/team/)

## Development

```bash
make sync   # install all dependencies (client + server + xarray)
make run    # start the dev server with hot reload
make lint   # ruff + mypy + pyright
make test   # pytest
```

## License

BSD-3-Clause.
