"""Output formatter stubs for skeleton process execution results."""

import csv
import io
import json
from typing import Any


def rows_to_csv(rows: list[dict[str, Any]]) -> str:
    """Serialize canonical rows into a CSV payload string."""

    if not rows:
        return ""

    fieldnames = sorted({key for row in rows for key in row.keys()})
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()

    for row in rows:
        serialized_row: dict[str, str | int | float | bool | None] = {}
        for key in fieldnames:
            value = row.get(key)
            if isinstance(value, (dict, list)):
                serialized_row[key] = json.dumps(value, separators=(",", ":"), sort_keys=True)
            else:
                serialized_row[key] = value
        writer.writerow(serialized_row)

    return buffer.getvalue()


def rows_to_dhis2(
    rows: list[dict[str, Any]],
    *,
    data_element: str | None = None,
    category_option_combo: str | None = None,
    org_unit: str | None = None,
    period: str | None = None,
) -> dict[str, Any]:
    """Format row results as a DHIS2 dataValueSets-compatible payload.

    Each row with a non-null ``value`` becomes one ``dataValue`` entry.
    When DHIS2 UIDs are not supplied, placeholder strings mark the fields
    that the caller must substitute before posting to DHIS2.

    Returns a dict with:
      - ``dataValues``: list of dataValue objects
      - ``count``: total rows received
      - ``computed``: number of rows with a non-null value (= len(dataValues))
    """
    data_values: list[dict[str, Any]] = []

    for row in rows:
        value = row.get("value")
        if value is None:
            continue

        dv: dict[str, Any] = {
            "dataElement": data_element or "<dataElement UID>",
            "orgUnit": org_unit or "<orgUnit UID>",
            "period": period or "<YYYYMMDD>",
            "value": str(value),
        }
        if category_option_combo is not None:
            dv["categoryOptionCombo"] = category_option_combo

        parameter = row.get("parameter", "")
        stat = row.get("stat", "")
        comment = f"{parameter} {stat}".strip()
        if comment:
            dv["comment"] = comment

        data_values.append(dv)

    return {
        "dataValues": data_values,
        "count": len(rows),
        "computed": len(data_values),
    }


def rows_to_dhis2_stub(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a placeholder DHIS2 payload envelope for skeleton processes."""

    return {
        "dataValues": [],
        "count": len(rows),
        "status": "stub",
        "message": "DHIS2 formatting is not implemented yet.",
    }
