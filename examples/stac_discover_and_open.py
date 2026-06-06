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

    t_dim = next((d for d in ds.dims if d in {"t", "time"}), None)
    y_dim = next((d for d in ds.dims if d in {"y", "latitude", "lat"}), None)
    x_dim = next((d for d in ds.dims if d in {"x", "longitude", "lon"}), None)

    if t_dim:
        print(f"\nTime range: {ds[t_dim].values[0]}  →  {ds[t_dim].values[-1]}")
        print(f"Time steps: {ds.sizes[t_dim]}")
    if y_dim:
        print(f"Latitude:  {ds[y_dim].min().item():.4f}  →  {ds[y_dim].max().item():.4f}")
    if x_dim:
        print(f"Longitude: {ds[x_dim].min().item():.4f}  →  {ds[x_dim].max().item():.4f}")

    variable = list(ds.data_vars)[0]
    if t_dim and y_dim and x_dim:
        centre_y = ds[y_dim].mean().item()
        centre_x = ds[x_dim].mean().item()
        sample = ds[variable].isel({t_dim: 0}).sel({y_dim: centre_y, x_dim: centre_x}, method="nearest")
        print(f"\n{variable} at domain centre, t=0: {sample.compute().item()}")


if __name__ == "__main__":
    main()
