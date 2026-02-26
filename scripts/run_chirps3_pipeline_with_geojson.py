#!/usr/bin/env python3
"""Run chirps3-dhis2-pipeline using a local GeoJSON FeatureCollection."""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(
        description="Execute chirps3-dhis2-pipeline with features_geojson from a local file.",
    )
    parser.add_argument("--api-base", default="http://127.0.0.1:8000", help="EO API base URL")
    parser.add_argument(
        "--geojson-path",
        default="sample_data/ADM1.geojson",
        help="Path to input FeatureCollection GeoJSON",
    )
    parser.add_argument("--start-date", default="2024-01-01", help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2024-03-31", help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument(
        "--org-unit-id-property",
        default="shapeISO",
        help="Feature property to map to DHIS2 orgUnit when feature.id is missing",
    )
    parser.add_argument("--data-element", default="DE_UID", help="DHIS2 dataElement UID")
    parser.add_argument("--category-option-combo", default=None, help="Optional COC UID")
    parser.add_argument("--attribute-option-combo", default=None, help="Optional AOC UID")
    parser.add_argument("--data-set", default=None, help="Optional dataSet UID")
    parser.add_argument(
        "--temporal-resolution",
        choices=["daily", "weekly", "monthly"],
        default="monthly",
        help="Temporal output resolution",
    )
    parser.add_argument(
        "--temporal-reducer",
        choices=["sum", "mean"],
        default="sum",
        help="Reducer for weekly/monthly aggregation",
    )
    parser.add_argument("--spatial-reducer", choices=["mean", "sum"], default="mean", help="Spatial reducer")
    parser.add_argument("--stage", choices=["final", "prelim"], default="final", help="CHIRPS3 stage")
    parser.add_argument("--value-rounding", type=int, default=3, help="Decimal places")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=600.0,
        help="HTTP timeout in seconds for the execute call (set 0 for no timeout)",
    )
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True, help="Skip DHIS2 import")
    parser.add_argument(
        "--auto-import",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Import to DHIS2 (effective only when --no-dry-run)",
    )
    parser.add_argument("--import-strategy", default="CREATE_AND_UPDATE", help="DHIS2 import strategy")
    parser.add_argument(
        "--output-mode",
        choices=["summary", "head", "tail", "full"],
        default="summary",
        help="How much of response dataValues to print",
    )
    parser.add_argument(
        "--output-format",
        choices=["json", "table"],
        default="json",
        help="Render head/tail rows as JSON or table",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10,
        help="Number of rows for head/tail output modes",
    )
    return parser.parse_args()


def _print_table(rows: list[dict[str, Any]]) -> None:
    """Print rows as a simple terminal table."""
    columns = ["orgUnit", "period", "value", "dataElement", "categoryOptionCombo", "attributeOptionCombo"]
    present_columns = [col for col in columns if any(col in row for row in rows)]
    if not present_columns:
        print("(no tabular columns found)")
        return

    widths: dict[str, int] = {col: len(col) for col in present_columns}
    for row in rows:
        for col in present_columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    header = " | ".join(col.ljust(widths[col]) for col in present_columns)
    divider = "-+-".join("-" * widths[col] for col in present_columns)
    print(header)
    print(divider)
    for row in rows:
        print(" | ".join(str(row.get(col, "")).ljust(widths[col]) for col in present_columns))


def _print_response(body: dict[str, Any], *, output_mode: str, output_format: str, rows: int) -> None:
    """Print response in summary/head/tail/full modes."""
    if output_mode == "full":
        print(json.dumps(body, indent=2))
        return

    data_values = body.get("dataValueSet", {}).get("dataValues")
    if not isinstance(data_values, list):
        print(json.dumps(body, indent=2))
        return

    total = len(data_values)
    slim_body = dict(body)
    slim_dvs = dict(slim_body.get("dataValueSet", {}))
    slim_dvs["dataValues"] = []
    slim_body["dataValueSet"] = slim_dvs
    print(json.dumps(slim_body, indent=2))
    print(f"dataValues total: {total}")

    if output_mode == "summary":
        return

    n = max(rows, 1)
    selected = data_values[:n] if output_mode == "head" else data_values[-n:]
    label = "head" if output_mode == "head" else "tail"
    print(f"dataValues {label}({n}):")
    if output_format == "table":
        _print_table(selected)
    else:
        print(json.dumps(selected, indent=2))


def main() -> int:
    """Execute the pipeline request and print the response."""
    args = parse_args()
    geojson_path = Path(args.geojson_path)
    if not geojson_path.exists():
        print(f"GeoJSON file not found: {geojson_path}", file=sys.stderr)
        return 2

    try:
        features_geojson = json.loads(geojson_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON in {geojson_path}: {exc}", file=sys.stderr)
        return 2

    payload: dict[str, Any] = {
        "inputs": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "features_geojson": features_geojson,
            "org_unit_id_property": args.org_unit_id_property,
            "data_element": args.data_element,
            "temporal_resolution": args.temporal_resolution,
            "temporal_reducer": args.temporal_reducer,
            "spatial_reducer": args.spatial_reducer,
            "stage": args.stage,
            "value_rounding": args.value_rounding,
            "dry_run": args.dry_run,
            "auto_import": args.auto_import,
            "import_strategy": args.import_strategy,
        }
    }

    if args.category_option_combo:
        payload["inputs"]["category_option_combo"] = args.category_option_combo
    if args.attribute_option_combo:
        payload["inputs"]["attribute_option_combo"] = args.attribute_option_combo
    if args.data_set:
        payload["inputs"]["data_set"] = args.data_set

    endpoint = f"{args.api_base.rstrip('/')}/ogcapi/processes/chirps3-dhis2-pipeline/execution"
    stop_event = threading.Event()

    def _show_progress() -> None:
        symbols = [".  ", ".. ", "..."]
        index = 0
        while not stop_event.is_set():
            sys.stdout.write(f"\rSubmitting request{symbols[index % len(symbols)]}")
            sys.stdout.flush()
            index += 1
            time.sleep(0.5)
        sys.stdout.write("\rSubmitting request... done\n")
        sys.stdout.flush()

    timeout = None if args.timeout_seconds == 0 else args.timeout_seconds
    progress_thread = threading.Thread(target=_show_progress, daemon=True)
    progress_thread.start()
    response: httpx.Response | None = None
    try:
        response = httpx.post(endpoint, json=payload, timeout=timeout)
    except httpx.TimeoutException:
        print(f"\nPOST {endpoint}", file=sys.stderr)
        print(
            "Request timed out on the client side. "
            "Try a larger --timeout-seconds value (or --timeout-seconds 0 for no timeout).",
            file=sys.stderr,
        )
        return 1
    finally:
        stop_event.set()
        progress_thread.join(timeout=2.0)

    print(f"POST {endpoint}")
    print(f"HTTP {response.status_code}")
    try:
        body = response.json()
        if isinstance(body, dict):
            _print_response(
                body,
                output_mode=args.output_mode,
                output_format=args.output_format,
                rows=args.rows,
            )
        else:
            print(json.dumps(body, indent=2))
    except ValueError:
        print(response.text)

    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
