'''Temporal reduce process plugin for raster and time period input.

Example invocation with curl (Windows):

curl -X POST "http://localhost:8000/ogcapi/processes/temporal-reduce/execution" ^
-H "Content-Type: application/json" ^
-d "{\"inputs\": {\"raster\": \"2m_temperature_hourly\", \"band\": \"t2m\", \"period_type\": \"monthly\", \"time_period\": \"2025-12\", \"stats\": [\"mean\"]}}" ^
--output temporal_reduce_map.png

'''

from __future__ import annotations

import logging
from typing import Any

from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.routers.ogcapi.plugins.processes.schemas import TemporalReduceInput
from eo_api.datasets import raster as raster_ops
from eo_api.datasets import serialize
from eo_api.datasets import registry


logger = logging.getLogger(__name__)


PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "temporal-reduce",
    "title": "Temporal reduce",
    "description": "Reduces a raster dataset to a specified period type and time period.",
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["raster", "temporal", "time", "reduce", "aggregation"],
    "inputs": {
        "raster": {
            "title": "Raster dataset id",
            "description": "Raster dataset id as defined and created by /datasets endpoints.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "band": {
            "title": "Raster band name",
            "description": "Name of raster band or variable to reduce.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "period_type": {
            "title": "Period type",
            "description": "Type of period to reduce to: daily, monthly.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "time_period": {
            "title": "Time period",
            "description": "Time period to reduce to, in ISO format corresponding to period_type.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "stats": {
            "title": "Statistics",
            "description": "Statistics to compute for the specified time period.",
            "schema": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["count", "sum", "mean", "min", "max"],
                },
                "default": ["mean"],
            },
            "minOccurs": 0,
            "maxOccurs": 1,
        },
    },
    "outputs": {
        "preview": {
            "title": "Rendered map image of reduced raster",
            "schema": {"type": "object", "contentMediaType": "image/png"},
        }
    },
}


class TemporalReduceProcessor(BaseProcessor):
    """Processor that reduces a raster to a given time period."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, bytes]:
        try:
            inputs = TemporalReduceInput.model_validate(data)
            logger.info(f'Process inputs: {inputs.model_dump()}')
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        # get dataset metadata
        dataset_id = inputs.raster
        dataset = registry.get_dataset(dataset_id)
        if not dataset:
            raise ProcessorExecuteError(f"Dataset '{dataset_id}' not found")

        # retrieve and limit raster to a single time period
        start = end = inputs.time_period
        ds = raster_ops.get_data(dataset, start, end)

        # reduce to time dimension
        # NOTE: only allows a single statistic for now
        statistic = inputs.stats[0]
        ds = raster_ops.to_timeperiod(ds, dataset, inputs.period_type, statistic=statistic)

        # serialize to image preview
        image_data = serialize.xarray_to_preview(ds, dataset, inputs.period_type)

        # return
        logger.info('Process finished')
        return "image/png", image_data

    def __repr__(self) -> str:
        return "<TemporalReduceProcessor>"
