from typing import Any

from eo_api.routers.ogcapi.plugins.processes.era5_land import ERA5LandProcessor


def test_era5_land_uses_component_trace(monkeypatch: Any) -> None:
    from eo_api.routers.ogcapi.plugins.processes import era5_land as module

    monkeypatch.setattr(
        module,
        "download_era5_land",
        lambda **kwargs: {
            "files": ["/tmp/data/era5_2024-01.nc"],
            "summary": {
                "file_count": 1,
                "variables": kwargs["variables"],
                "start": kwargs["start"],
                "end": kwargs["end"],
            },
        },
    )

    processor = ERA5LandProcessor({"name": "era5-land-download"})
    mimetype, output = processor.execute(
        {
            "start": "2024-01",
            "end": "2024-01",
            "bbox": [32.0, -2.0, 35.0, 1.0],
            "variables": ["2m_temperature"],
        }
    )

    assert mimetype == "application/json"
    assert output["status"] == "completed"
    assert output["files"] == ["/tmp/data/era5_2024-01.nc"]
    assert output["summary"]["file_count"] == 1
    assert len(output["workflowTrace"]) == 1
    assert output["workflowTrace"][0]["step"] == "era5_land_download"
