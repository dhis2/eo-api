"""Shared builders for DHIS2 dataValueSet payloads."""

from __future__ import annotations

from typing import Any

from dhis2eo.integrations.pandas import format_value_for_dhis2


def build_data_value_set(
    *,
    rows: list[dict[str, Any]],
    data_element: str,
    category_option_combo: str | None = None,
    attribute_option_combo: str | None = None,
    data_set: str | None = None,
) -> dict[str, Any]:
    """Build DHIS2 dataValueSet payload and tabular output from normalized rows."""
    data_values: list[dict[str, Any]] = []
    for row in rows:
        data_value: dict[str, Any] = {
            "dataElement": data_element,
            "orgUnit": row["orgUnit"],
            "period": row["period"],
            "value": format_value_for_dhis2(row["value"]),
        }
        if category_option_combo:
            data_value["categoryOptionCombo"] = category_option_combo
        if attribute_option_combo:
            data_value["attributeOptionCombo"] = attribute_option_combo
        data_values.append(data_value)

    payload: dict[str, Any] = {"dataValues": data_values}
    if data_set:
        payload["dataSet"] = data_set

    columns = ["orgUnit", "period", "value", "dataElement", "categoryOptionCombo", "attributeOptionCombo"]
    table_rows = [{column: value.get(column) for column in columns} for value in data_values]

    return {
        "dataValueSet": payload,
        "table": {
            "columns": columns,
            "rows": table_rows,
        },
    }
