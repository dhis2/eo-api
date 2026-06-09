# Importing data to DHIS2 and Chap

This guide shows the full round-trip: pull organisation unit boundaries from DHIS2, aggregate a published dataset to those org units with Open Climate Service, and import the result back into DHIS2 as data values.

The spatial aggregation happens via the built-in [`aggregate_to_dhis2_json`](workflows.md#built-in-workflows) workflow, which returns a ready-to-import DHIS2 `dataValueSet`. Because each org unit's GeoJSON `id` is its DHIS2 UID, the result imports without any remapping.

The full runnable script is [`examples/aggregate_and_import_to_dhis2.py`](https://github.com/dhis2/open-climate-service/blob/main/examples/aggregate_and_import_to_dhis2.py).

## Prerequisites

- A running Open Climate Service instance with the dataset published (see [Accessing data](user_guide.md)).
- A DHIS2 instance whose organisation units have geometry, plus a data element to import into. The data element referenced in the payload must already exist in DHIS2 — see the DHIS2 Climate Tools [Prepare metadata](https://climate-tools.dhis2.org/guides/import-data/prepare-metadata/) guide for creating it.
- The two clients:

  ```bash
  pip install open-climate-service "dhis2-client @ git+https://github.com/dhis2/dhis2-python-client.git"
  ```

  `open-climate-service` ships the `ClimateService` client; [dhis2-python-client](https://github.com/dhis2/dhis2-python-client) handles the DHIS2 Web API calls.

## 1. Fetch organisation units from DHIS2

Pull the org unit boundaries as GeoJSON. Each feature's `id` is the org unit UID, which the workflow uses as the `orgUnit`.

```python
from dhis2_client import DHIS2Client
from dhis2_client.settings import ClientSettings

dhis2 = DHIS2Client(
    settings=ClientSettings(
        base_url="https://my-dhis2.example.org",
        username="admin",
        password="district",
    )
)

org_units = dhis2.get_org_units_geojson(level=2)
print(len(org_units["features"]), "org units")
```

!!! note
    `admin` / `district` are the public DHIS2 demo credentials, shown for illustration — point the client at your own instance and credentials.

## 2. Aggregate on Open Climate Service

Run the `aggregate_to_dhis2_json` workflow with `ClimateService.execute()`. It loads the dataset over the time range, aggregates it within each org-unit polygon, and returns a DHIS2 `dataValueSet`.

```python
from open_climate_service import ClimateService

service = ClimateService("http://127.0.0.1:8000")

data_value_set = service.execute(
    {
        "agg": {
            "process_id": "aggregate_to_dhis2_json",
            "arguments": {
                "dataset_id": "era5land_temperature_monthly",  # a published collection
                "temporal_extent": ["2025-01-01", "2025-12-31"],
                "geometries": org_units,
                "data_element_id": "BXgDHhPdFVU",  # DHIS2 data element to import into
                "method": "mean",  # mean (default), min, max, or sum
                "period_type": "month",
            },
            "result": True,
        }
    }
)

print(len(data_value_set["dataValues"]), "data values")
```

The workflow fills in `orgUnit`, `period`, `value`, and `dataElement` for every cell — a valid DHIS2 `dataValueSet`, ready to import as-is.

## 3. Import into DHIS2

```python
report = dhis2.post_data_value_set(data_value_set)
print(report["response"]["importCount"])
```

## Producing a CHAP CSV instead

To feed the [Chap Modeling Platform](https://chap.dhis2.org/chap-modeling-platform/) rather than DHIS2 data values, use the companion [`aggregate_to_chap_csv`](workflows.md#built-in-workflows) workflow — same call, but omit `data_element_id`; it returns a CHAP-ready CSV instead of a `dataValueSet`. Pass `path=` to write it to disk:

```python
csv_path = service.execute(
    {
        "agg": {
            "process_id": "aggregate_to_chap_csv",
            "arguments": {
                "dataset_id": "era5land_precipitation_monthly",
                "temporal_extent": ["2025-01-01", "2025-12-31"],
                "geometries": org_units,
                "method": "mean",
                "period_type": "month",
            },
            "result": True,
        }
    },
    path="climate-monthly.csv",
)
```

## See also

- [Workflows](workflows.md) — reference for the `aggregate_to_dhis2_json` and `aggregate_to_chap_csv` workflows.
- [openEO](openeo.md) — process graphs, export formats, and more examples.
