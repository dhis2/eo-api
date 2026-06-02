"""Process a collection via the openEO Python client.

Replicates the canonical openEO docs example
(https://open-eo.github.io/openeo-python-client/) against a local Open Climate Service
instance: connect → load_collection → apply rescale → max_time → execute.

Requires:
  pip install openeo
  A running Open Climate Service with at least one published dataset.
Adjust BASE_URL if the API is not on the default local address.
"""

import json

import openeo

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    """Run a load → apply → max_time pipeline via the openEO client."""
    # Connect — no authentication required for a local deployment
    conn = openeo.connect(BASE_URL)
    print(f"Connected  api_version={conn.capabilities().api_version()}")

    # List available collections and pick the first one
    collections = conn.list_collections()
    if not collections:
        print("No published collections found. Run an ingestion first.")
        return

    collection_id = collections[0]["id"]
    spatial_extent = collections[0]["extent"]["spatial"]["bbox"][0]
    temporal_extent = collections[0]["extent"]["temporal"]["interval"][0]
    west, south, east, north = spatial_extent
    print(f"Collection : {collection_id}")
    print(f"Spatial    : {west:.2f}°W  {south:.2f}°N  →  {east:.2f}°E  {north:.2f}°N")
    print(f"Temporal   : {temporal_extent[0]}  →  {temporal_extent[1]}")

    # Load the collection (equivalent to load_collection in the docs example)
    cube = conn.load_collection(
        collection_id,
        spatial_extent={"west": west, "south": south, "east": east, "north": north},
        temporal_extent=temporal_extent,
    )

    # Apply element-wise rescaling — mirrors: cube.apply(lambda x: 0.004*x - 0.08)
    cube_scaled = cube.apply(lambda x: x / 1_000_000)

    # Reduce the time dimension to its maximum — mirrors: cube.max_time()
    cube_max = cube_scaled.max_time()

    print(f"\nProcess graph nodes: {list(cube_max.flat_graph().keys())}")

    # Execute synchronously — mirrors: cube.download("ndvi-max.tiff")
    result = conn.execute(cube_max)
    print(f"\nResult:\n{json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
