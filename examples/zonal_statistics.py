"""Compute monthly precipitation totals per administrative district.

Demonstrates aggregate_spatial over a GeoJSON file of Sierra Leone districts,
using rename_labels to attach DHIS2 org unit IDs so they survive into the output.

Requires:
  pip install openeo requests
  A running Open Climate Service with chirps3_precipitation_daily ingested.
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


def main() -> None:
    """Run load → monthly sum → zonal mean per district → CSV."""
    geojson = json.loads(DISTRICTS_FILE.read_text())
    features = geojson["features"]

    # Extract DHIS2 org unit IDs and names for display
    district_ids = [f["properties"]["id"] for f in features]
    id_to_name = {f["properties"]["id"]: f["properties"]["name"] for f in features}

    print(f"Districts  : {len(features)}")
    print("Period     : 2026-01 – 2026-03")
    print()

    process_graph = {
        # 1. Load daily CHIRPS precipitation
        "load": {
            "process_id": "load_collection",
            "arguments": {
                "id": "chirps3_precipitation_daily",
                "temporal_extent": ["2026-01-01", "2026-03-31"],
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
        # 4. Replace Shapely geometry objects with DHIS2 org unit IDs so they
        #    appear as the geometry label in the output — without this step the
        #    geometry dimension uses raw geometry objects that are lost in CSV/JSON.
        "label": {
            "process_id": "rename_labels",
            "arguments": {
                "data": {"from_node": "zones"},
                "dimension": "geometry",
                "target": district_ids,
            },
        },
        # 5. Save as CSV (geometry column now contains org unit IDs)
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "label"}, "format": "CSV"},
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

    # Parse CSV — rows are (t, geometry=org_unit_id, precip)
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    by_id: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        by_id[row["geometry"]][row["t"][:7]] = f"{float(row['precip']):.1f}"

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
