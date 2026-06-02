"""Open a published GeoZarr dataset directly and demonstrate spatial and temporal subsetting.

Requires a running Open Climate Service instance with at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

from open_climate_service.client import Client

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    """Open a Zarr store directly and demonstrate spatial and temporal subsetting."""
    api = Client(BASE_URL)
    datasets = api.catalog()
    if not datasets:
        print("No published datasets found. Run an ingestion first.")
        return

    first = datasets[0]
    print(f"Opening: {first['title']}\n")

    ds = api.open(first["id"])
    print(ds)

    print(f"\nDimensions:  {dict(ds.sizes)}")
    print(f"Time range:  {ds.time.values[0]}  →  {ds.time.values[-1]}")
    print(f"Latitude:    {ds.latitude.min().item()}  →  {ds.latitude.max().item()}")
    print(f"Longitude:   {ds.longitude.min().item()}  →  {ds.longitude.max().item()}")

    variable = list(ds.data_vars)[0]

    # Select a single time step
    t0 = ds.time.values[0]
    snapshot = ds[variable].sel(time=t0)
    print(f"\n{variable} snapshot at {t0}:")
    print(f"  shape: {snapshot.shape},  min: {snapshot.min().item()},  max: {snapshot.max().item()}")

    # Select the point closest to the spatial centre of the domain
    centre_lat = ds.latitude.mean().item()
    centre_lon = ds.longitude.mean().item()
    point = ds[variable].sel(latitude=centre_lat, longitude=centre_lon, method="nearest")
    print(f"\n{variable} at domain centre ({centre_lat:.2f}, {centre_lon:.2f}):")
    print(point.to_dataframe()[[variable]].head(10))

    # Spatial mean over the full domain — first 10 time steps
    spatial_mean = ds[variable].isel(time=slice(10)).mean(dim=["latitude", "longitude"])
    print(f"\nSpatial mean {variable} time series (first 10 steps):")
    print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
