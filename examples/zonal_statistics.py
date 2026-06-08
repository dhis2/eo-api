"""Compute monthly precipitation totals per administrative district.

Demonstrates aggregate_spatial over a GeoJSON file of Sierra Leone districts,
using rename_labels to attach DHIS2 org unit IDs so they survive into the output.

Requires:
  pip install openeo requests
  A running Open Climate Service with era5land_precipitation_daily ingested.
  examples/data/sle-districts.geojson (included in this repo)

Adjust BASE_URL if the API is not on the default local address.
"""

from __future__ import annotations

import csv
import io
import json
from collections import defaultdict
from pathlib import Path

import requests

BASE_URL = "http://127.0.0.1:8000"
DISTRICTS_FILE = Path(__file__).parent / "data" / "sle-districts.geojson"


COLLECTION_ID = "era5land_precipitation_daily"


def main() -> None:
    """Run load → monthly sum → zonal mean per district → CSV."""
    geojson = json.loads(DISTRICTS_FILE.read_text())
    features = geojson["features"]

    # Map org unit UID → display name. aggregate_spatial labels the geometry
    # dimension with each feature's GeoJSON `id` (the DHIS2 org unit UID), so the
    # CSV geometry column carries the UIDs directly.
    id_to_name = {f["properties"]["id"]: f["properties"]["name"] for f in features}

    # Discover available temporal extent from the STAC collection
    coll = requests.get(f"{BASE_URL}/stac/collections/{COLLECTION_ID}", timeout=30)
    coll.raise_for_status()
    coll = coll.json()
    interval = coll["extent"]["temporal"]["interval"][0]
    temporal_extent = [interval[0][:10], interval[1][:10]]

    print(f"Districts  : {len(features)}")
    print(f"Period     : {temporal_extent[0]} – {temporal_extent[1]}")
    print()

    process_graph = {
        # 1. Load daily CHIRPS precipitation
        "load": {
            "process_id": "load_collection",
            "arguments": {
                "id": COLLECTION_ID,
                "temporal_extent": temporal_extent,
            },
        },
        # 2. Sum daily values into monthly totals
        "monthly": {
            "process_id": "aggregate_temporal_period",
            "arguments": {
                "data": {"from_node": "load"},
                "period": "month",
                "reducer": {
                    "process_graph": {
                        "sum": {
                            "process_id": "sum",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        }
                    }
                },
            },
        },
        # 3. Compute mean over each district polygon
        "zones": {
            "process_id": "aggregate_spatial",
            "arguments": {
                "data": {"from_node": "monthly"},
                "geometries": geojson,
                "reducer": {
                    "process_graph": {
                        "mean": {
                            "process_id": "mean",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        }
                    }
                },
            },
        },
        # 4. Save as CSV. aggregate_spatial already labels the geometry dimension
        #    with each feature's GeoJSON `id` (the DHIS2 org unit UID), so the
        #    geometry column carries the org unit IDs directly — no rename needed.
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "zones"}, "format": "CSV"},
            "result": True,
        },
    }

    resp = requests.post(
        f"{BASE_URL}/result",
        json={"process": {"process_graph": process_graph}},
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()

    # Parse CSV — rows are (geometry=org_unit_id, t, tp)
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    # Find the precipitation variable column (first non-index column)
    index_cols = {"geometry", "t"}
    precip_col = next((c for c in (rows[0] if rows else {}) if c not in index_cols), "tp")
    by_id: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        by_id[row["geometry"]][row["t"][:7]] = f"{float(row[precip_col]):.1f}"

    months = sorted({row["t"][:7] for row in rows})

    # Display pivot table
    header = f"{'District':<22} {'Org unit ID':<14}" + "".join(f"{m:>10}" for m in months)
    print(header)
    print("-" * len(header))
    for uid, vals in sorted(by_id.items(), key=lambda x: id_to_name.get(x[0], x[0])):
        name = id_to_name.get(uid, uid)
        row_vals = "".join(f"{vals.get(m, 'n/a'):>10}" for m in months)
        print(f"{name:<22} {uid:<14}{row_vals}")

    print(f"\n{len(by_id)} districts × {len(months)} months  (mm total monthly precipitation)")


if __name__ == "__main__":
    main()
