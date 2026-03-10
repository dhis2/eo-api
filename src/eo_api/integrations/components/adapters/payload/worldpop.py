"""WorldPop DHIS2 payload adapter using canonical row formatting."""

from __future__ import annotations

from typing import Any

from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.components.services.dhis2_datavalues_service import build_data_value_set


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Build DHIS2 payload for WorldPop canonical rows."""
    rows = params.get("rows", [])
    if not rows:
        raise ProcessorExecuteError("No rows supplied for WorldPop payload builder")

    result = build_data_value_set(
        rows=rows,
        data_element=params["data_element"],
        category_option_combo=params.get("category_option_combo"),
        attribute_option_combo=params.get("attribute_option_combo"),
        data_set=params.get("data_set"),
    )

    summary = params.get("summary")
    return {
        "dataValueSet": result["dataValueSet"],
        "table": result["table"],
        "summary": summary,
    }
