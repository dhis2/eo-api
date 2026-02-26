from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.transform import from_origin

os.environ.setdefault("PYGEOAPI_CONFIG", str(Path(__file__).resolve().parents[1] / "pygeoapi-config.yml"))
os.environ.setdefault("PYGEOAPI_OPENAPI", str(Path(__file__).resolve().parents[1] / "pygeoapi-openapi.yml"))

from eo_api.routers.ogcapi.plugins.processes.zonal_statistics import ZonalStatisticsProcessor


def _create_test_raster(path: Path) -> None:
    data = np.array(
        [
            [1.0, 2.0, 3.0, 4.0],
            [5.0, 6.0, 7.0, 8.0],
            [9.0, 10.0, 11.0, 12.0],
            [13.0, 14.0, 15.0, 16.0],
        ],
        dtype=np.float32,
    )

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0.0, 4.0, 1.0, 1.0),
        nodata=-9999.0,
    ) as dst:
        dst.write(data, 1)


def _sample_geojson() -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "zone-1",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 2], [2, 2], [2, 4], [0, 4], [0, 2]]],
                },
                "properties": {"name": "Zone 1"},
            },
            {
                "type": "Feature",
                "id": "zone-empty",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]],
                },
                "properties": {"name": "No overlap"},
            },
        ],
    }


def test_zonal_statistics_with_geojson_object(tmp_path: Path) -> None:
    raster_path = tmp_path / "sample.tif"
    _create_test_raster(raster_path)

    processor = ZonalStatisticsProcessor({"name": "zonal-statistics"})
    mimetype, output = processor.execute(
        {
            "geojson": _sample_geojson(),
            "raster": str(raster_path),
            "stats": ["count", "sum", "mean", "min", "max"],
        }
    )

    assert mimetype == "application/json"
    assert output["id"] == "features"

    features = output["value"]["features"]
    stats_1 = features[0]["properties"]["zonal_statistics"]
    assert stats_1["count"] == 4.0
    assert stats_1["sum"] == 14.0
    assert stats_1["mean"] == 3.5
    assert stats_1["min"] == 1.0
    assert stats_1["max"] == 6.0

    stats_2 = features[1]["properties"]["zonal_statistics"]
    assert stats_2["count"] is None
    assert stats_2["sum"] is None
    assert stats_2["mean"] is None


def test_zonal_statistics_with_geojson_file_input(tmp_path: Path) -> None:
    raster_path = tmp_path / "sample.tif"
    geojson_path = tmp_path / "zones.geojson"
    _create_test_raster(raster_path)
    geojson_path.write_text(json.dumps(_sample_geojson()), encoding="utf-8")

    processor = ZonalStatisticsProcessor({"name": "zonal-statistics"})
    _, output = processor.execute(
        {
            "geojson": str(geojson_path),
            "raster": str(raster_path),
            "stats": ["median", "std"],
            "output_property": "stats",
        }
    )

    features = output["value"]["features"]
    stats_1 = features[0]["properties"]["stats"]
    assert stats_1["median"] == 3.5
    assert stats_1["std"] == 2.0615528128088303
