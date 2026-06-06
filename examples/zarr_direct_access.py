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

    t_dim = next((d for d in ds.dims if d in {"t", "time"}), None)
    y_dim = next((d for d in ds.dims if d in {"y", "latitude", "lat"}), None)
    x_dim = next((d for d in ds.dims if d in {"x", "longitude", "lon"}), None)

    print(f"\nDimensions:  {dict(ds.sizes)}")
    if t_dim:
        print(f"Time range:  {ds[t_dim].values[0]}  →  {ds[t_dim].values[-1]}")
    if y_dim:
        print(f"Latitude:    {ds[y_dim].min().item():.4f}  →  {ds[y_dim].max().item():.4f}")
    if x_dim:
        print(f"Longitude:   {ds[x_dim].min().item():.4f}  →  {ds[x_dim].max().item():.4f}")

    variable = list(ds.data_vars)[0]

    if t_dim:
        # Select a single time step
        t0 = ds[t_dim].values[0]
        snapshot = ds[variable].isel({t_dim: 0}).compute()
        print(f"\n{variable} snapshot at {t0}:")
        print(f"  shape: {snapshot.shape},  min: {snapshot.min().item()},  max: {snapshot.max().item()}")

    if y_dim and x_dim:
        # Select the point closest to the spatial centre of the domain
        centre_y = ds[y_dim].mean().item()
        centre_x = ds[x_dim].mean().item()
        point = ds[variable].sel({y_dim: centre_y, x_dim: centre_x}, method="nearest")
        print(f"\n{variable} at domain centre ({centre_y:.2f}, {centre_x:.2f}):")
        print(point.to_dataframe()[[variable]].head(10))

    if t_dim and y_dim and x_dim:
        # Spatial mean over the full domain — first 10 time steps
        spatial_mean = ds[variable].isel({t_dim: slice(10)}).mean(dim=[y_dim, x_dim])
        print(f"\nSpatial mean {variable} time series (first 10 steps):")
        print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
