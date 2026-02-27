"""Build DHIS2 dataValueSet payloads from aggregated rows."""

from __future__ import annotations

from typing import Any

from dhis2eo.integrations.pandas import format_value_for_dhis2
from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.routers.ogcapi.plugins.processes.schemas import DataValueBuildInput

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "dhis2-datavalue-build",
    "title": "Build DHIS2 dataValueSet",
    "description": "Build a DHIS2-compatible dataValueSet and table output from aggregated rows.",
    "jobControlOptions": ["sync-execute"],
    "keywords": ["dhis2", "datavalueset", "format", "table"],
    "inputs": {
        "rows": {
            "title": "Aggregated rows",
            "description": "List of aggregated rows with orgUnit, period, value.",
            "schema": {"type": "array", "items": {"type": "object"}},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "data_element": {
            "title": "Data element UID",
            "description": "DHIS2 data element UID.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "category_option_combo": {
            "title": "Category option combo UID",
            "description": "Optional category option combo UID.",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "attribute_option_combo": {
            "title": "Attribute option combo UID",
            "description": "Optional attribute option combo UID.",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "data_set": {
            "title": "Dataset UID",
            "description": "Optional dataset UID.",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
    },
    "outputs": {
        "result": {
            "title": "Built dataValueSet and table",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


class DataValueBuildProcessor(BaseProcessor):
    """Processor that builds dataValueSet/table output from aggregated rows."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = DataValueBuildInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        data_values: list[dict[str, Any]] = []
        for row in inputs.rows:
            data_value = {
                "dataElement": inputs.data_element,
                "orgUnit": row["orgUnit"],
                "period": row["period"],
                "value": format_value_for_dhis2(row["value"]),
            }
            if inputs.category_option_combo:
                data_value["categoryOptionCombo"] = inputs.category_option_combo
            if inputs.attribute_option_combo:
                data_value["attributeOptionCombo"] = inputs.attribute_option_combo
            data_values.append(data_value)

        payload: dict[str, Any] = {"dataValues": data_values}
        if inputs.data_set:
            payload["dataSet"] = inputs.data_set

        columns = ["orgUnit", "period", "value", "dataElement", "categoryOptionCombo", "attributeOptionCombo"]
        table_rows = [{column: value.get(column) for column in columns} for value in data_values]

        return "application/json", {
            "dataValueSet": payload,
            "table": {
                "columns": columns,
                "rows": table_rows,
            },
        }

    def __repr__(self) -> str:
        return "<DataValueBuildProcessor>"
