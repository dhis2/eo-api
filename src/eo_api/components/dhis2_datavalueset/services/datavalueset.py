"""DHIS2 DataValueSet builder component."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import numpy as np

from ...download.services.download import DOWNLOAD_DIR
from ...schemas import PeriodType
from ..schemas.datavalueset import Dhis2DataValueSetConfig


def build_data_value_set(
    records: list[dict[str, Any]],
    *,
    dataset_id: str,
    period_type: PeriodType,
    config: Dhis2DataValueSetConfig,
) -> tuple[dict[str, Any], str]:
    """Build and serialize a DHIS2-compatible DataValueSet JSON payload."""
    data_values: list[dict[str, Any]] = []
    for record in records:
        period = _format_period(record["time"], period_type)
        data_values.append(
            {
                "dataElement": config.data_element_uid,
                "period": period,
                "orgUnit": record["org_unit"],
                "categoryOptionCombo": config.category_option_combo_uid,
                "attributeOptionCombo": config.attribute_option_combo_uid,
                "value": str(record["value"]),
            }
        )

    payload: dict[str, Any] = {"dataValues": data_values}
    if config.data_set_uid:
        payload["dataSet"] = config.data_set_uid
    if config.stored_by:
        payload["storedBy"] = config.stored_by
    output_file = _write_data_value_set(payload, dataset_id)
    return payload, output_file


def _write_data_value_set(payload: dict[str, Any], dataset_id: str) -> str:
    """Persist DataValueSet payload and return file path."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = DOWNLOAD_DIR / f"{dataset_id}_datavalueset_{now}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def _format_period(time_value: Any, period_type: PeriodType) -> str:
    ts = np.datetime64(time_value)
    s = np.datetime_as_string(ts, unit="D")
    year, month, day = s.split("-")
    if period_type == PeriodType.DAILY:
        return f"{year}{month}{day}"
    if period_type == PeriodType.MONTHLY:
        return f"{year}{month}"
    if period_type == PeriodType.YEARLY:
        return year
    return s.replace("-", "")


def build_datavalueset_component(
    *,
    dataset_id: str,
    period_type: PeriodType,
    records: list[dict[str, Any]],
    dhis2: Dhis2DataValueSetConfig,
) -> tuple[dict[str, Any], str]:
    """Build and serialize DHIS2 DataValueSet from records."""
    return build_data_value_set(records=records, dataset_id=dataset_id, period_type=period_type, config=dhis2)


# from workflows engine
# def _run_build_datavalueset(
#     *,
#     runtime: WorkflowRuntime,
#     request: WorkflowExecuteRequest,
#     dataset: dict[str, Any],
#     context: dict[str, Any],
#     step_config: dict[str, Any],
# ) -> dict[str, Any]:
#     del dataset
#     period_type = PeriodType(str(step_config.get("period_type", request.temporal_aggregation.target_period_type)))
#     data_value_set, output_file = runtime.run(
#         "build_datavalueset",
#         component_services.build_datavalueset_component,
#         records=_require_context(context, "records"),
#         dataset_id=request.dataset_id,
#         period_type=period_type,
#         dhis2=request.dhis2,
#     )
#     return {"data_value_set": data_value_set, "output_file": output_file}
