import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_preview_collection_items_filter_by_job_id(client: TestClient) -> None:
    target = Path("/tmp/generic_dhis2_datavalue_preview.geojson")
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "job-a-0",
                "geometry": None,
                "properties": {
                    "job_id": "job-a",
                    "dataset_type": "chirps3",
                    "orgUnit": "OU_1",
                    "period": "202501",
                    "value": "1.23",
                },
            },
            {
                "type": "Feature",
                "id": "job-b-0",
                "geometry": None,
                "properties": {
                    "job_id": "job-b",
                    "dataset_type": "worldpop",
                    "orgUnit": "OU_2",
                    "period": "2026",
                    "value": "2.34",
                },
            },
        ],
    }
    target.write_text(json.dumps(payload), encoding="utf-8")

    response = client.get("/ogcapi/collections/generic-dhis2-datavalue-preview/items?job_id=job-a")
    assert response.status_code == 200
    body = response.json()
    assert body["numberMatched"] == 1
    assert body["numberReturned"] == 1
    assert body["features"][0]["properties"]["job_id"] == "job-a"
