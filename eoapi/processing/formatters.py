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


def rows_to_dhis2_stub(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a placeholder DHIS2 payload envelope."""

    return {
        "dataValues": [],
        "count": len(rows),
        "status": "stub",
        "message": "DHIS2 formatting is not implemented yet.",
    }
