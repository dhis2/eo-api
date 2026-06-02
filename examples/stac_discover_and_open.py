"""Discover published datasets via the STAC catalog and open one with xarray.

Requires a running Open Climate Service instance and at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

import json

from open_climate_service.client import Client

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    """Discover and open the first published dataset."""
    api = Client(BASE_URL)
    datasets = api.catalog()

    if not datasets:
        print("No published datasets found. Run an ingestion first.")
        return

    print(json.dumps(datasets, indent=2))

    first = datasets[0]
    print(f"\nOpening: {first['title']}")

    ds = api.open(first["id"])
    print(ds)

    print(f"\nTime range: {ds.time.values[0]}  →  {ds.time.values[-1]}")
    print(f"Time steps: {ds.sizes['time']}")
    print(f"Latitude:  {ds.latitude.min().item()}  →  {ds.latitude.max().item()}")
    print(f"Longitude: {ds.longitude.min().item()}  →  {ds.longitude.max().item()}")

    variable = list(ds.data_vars)[0]
    centre_lat = ds.latitude.mean().item()
    centre_lon = ds.longitude.mean().item()
    sample = ds[variable].isel(time=0).sel(latitude=centre_lat, longitude=centre_lon, method="nearest")
    print(f"\n{variable} at domain centre, t=0: {sample.item()}")


if __name__ == "__main__":
    main()
