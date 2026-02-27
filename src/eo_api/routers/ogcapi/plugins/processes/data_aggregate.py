"""Aggregate CHIRPS3 files over workflow features."""

from __future__ import annotations

import hashlib
import os
from typing import Any, cast

import pandas as pd
import xarray as xr
from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from rioxarray.exceptions import NoDataInBounds

from eo_api.routers.ogcapi.plugins.processes.schemas import DataAggregateInput

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/data")

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "data-aggregate",
    "title": "Data aggregate",
    "description": "Aggregate downloaded raster files over normalized features.",
    "jobControlOptions": ["sync-execute"],
    "keywords": ["aggregation", "zonal", "timeseries"],
    "inputs": {
        "start_date": {"schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "end_date": {"schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "files": {"schema": {"type": "array", "items": {"type": "string"}}, "minOccurs": 1, "maxOccurs": 1},
        "valid_features": {"schema": {"type": "array", "items": {"type": "object"}}, "minOccurs": 1, "maxOccurs": 1},
        "stage": {"schema": {"type": "string", "enum": ["final", "prelim"], "default": "final"}},
        "flavor": {"schema": {"type": "string", "enum": ["rnl", "sat"], "default": "rnl"}},
        "spatial_reducer": {"schema": {"type": "string", "enum": ["mean", "sum"], "default": "mean"}},
        "temporal_resolution": {
            "schema": {"type": "string", "enum": ["daily", "weekly", "monthly"], "default": "monthly"}
        },
        "temporal_reducer": {"schema": {"type": "string", "enum": ["sum", "mean"], "default": "sum"}},
        "value_rounding": {"schema": {"type": "integer", "minimum": 0, "maximum": 10, "default": 3}},
    },
    "outputs": {
        "result": {
            "title": "Aggregated rows",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


def _resolve_value_var(dataset: xr.Dataset) -> str:
    if "precip" in dataset.data_vars:
        return "precip"
    keys = list(dataset.data_vars.keys())
    if not keys:
        raise ProcessorExecuteError("Downloaded CHIRPS3 dataset has no data variables")
    return str(keys[0])


def _clip_spatial_series(data_array: xr.DataArray, geometry: dict[str, Any], spatial_reducer: str) -> pd.Series:
    try:
        clipped = data_array.rio.clip([geometry], crs="EPSG:4326", drop=True)
    except NoDataInBounds:
        return pd.Series(dtype=float)
    spatial_dims = [dim for dim in clipped.dims if dim != "time"]
    if not spatial_dims:
        raise ProcessorExecuteError("Unable to resolve spatial dimensions in CHIRPS3 dataset")
    reduced = (
        clipped.mean(dim=spatial_dims, skipna=True)
        if spatial_reducer == "mean"
        else clipped.sum(dim=spatial_dims, skipna=True)
    )
    series: pd.Series[Any] = reduced.to_series().dropna()
    if series.empty:
        return series
    series.index = pd.DatetimeIndex(pd.to_datetime(series.index))
    return series


def _format_period_code(timestamp: pd.Timestamp, temporal_resolution: str) -> str:
    if temporal_resolution == "daily":
        return str(timestamp.strftime("%Y%m%d"))
    if temporal_resolution == "monthly":
        return str(timestamp.strftime("%Y%m"))
    iso = timestamp.isocalendar()
    iso_year, iso_week, _ = iso
    return f"{int(iso_year):04d}W{int(iso_week):02d}"


def _as_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if bool(pd.isna(ts)):
        raise ProcessorExecuteError("Encountered invalid timestamp while formatting periods")
    return ts


def _apply_temporal_aggregation(
    series: pd.Series,
    temporal_resolution: str,
    temporal_reducer: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[tuple[str, float]]:
    windowed = series[(series.index >= start_date) & (series.index <= end_date)]
    if windowed.empty:
        return []
    if temporal_resolution == "daily":
        return [
            (_format_period_code(_as_timestamp(ts), temporal_resolution), float(val)) for ts, val in windowed.items()
        ]
    if temporal_resolution == "monthly":
        aggregated = (
            windowed.resample("MS").mean().dropna()
            if temporal_reducer == "mean"
            else windowed.resample("MS").sum().dropna()
        )
        return [
            (_format_period_code(_as_timestamp(ts), temporal_resolution), float(val)) for ts, val in aggregated.items()
        ]
    index = pd.DatetimeIndex(pd.to_datetime(windowed.index))
    weekly_df = pd.DataFrame({"value": windowed.to_numpy()}, index=index)
    iso = index.isocalendar()
    weekly_df["iso_year"] = iso.year.to_numpy()
    weekly_df["iso_week"] = iso.week.to_numpy()
    grouped = weekly_df.groupby(["iso_year", "iso_week"])["value"]
    weekly = grouped.mean() if temporal_reducer == "mean" else grouped.sum()
    pairs: list[tuple[str, float]] = []
    for key, value in weekly.items():
        iso_year, iso_week = cast(tuple[Any, Any], key)
        pairs.append((f"{int(iso_year):04d}W{int(iso_week):02d}", float(value)))
    return pairs


def _scope_token_from_files(files: list[str], fallback_stage: str, fallback_flavor: str) -> str:
    if not files:
        return f"{fallback_stage}_{fallback_flavor}_unknown"
    # Expected name: chirps3_<scope_key>_YYYY-MM.nc
    name = os.path.basename(files[0])
    if name.endswith(".nc") and len(name) > 11:
        stem = name[:-3]
        token = stem.rsplit("_", 1)[0]
        if token:
            return token
    return f"{fallback_stage}_{fallback_flavor}_unknown"


def _cache_key(
    *,
    scope_token: str,
    spatial_reducer: str,
    temporal_resolution: str,
    temporal_reducer: str,
    value_rounding: int,
) -> str:
    raw = "|".join([scope_token, spatial_reducer, temporal_resolution, temporal_reducer, str(value_rounding)])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _target_periods(start_date: pd.Timestamp, end_date: pd.Timestamp, temporal_resolution: str) -> list[str]:
    if temporal_resolution == "daily":
        return [str(ts.strftime("%Y%m%d")) for ts in pd.date_range(start_date, end_date, freq="D")]
    if temporal_resolution == "monthly":
        return [str(ts.strftime("%Y%m")) for ts in pd.date_range(start_date, end_date, freq="MS")]
    days = pd.date_range(start_date, end_date, freq="D")
    keys: list[str] = []
    seen: set[str] = set()
    for ts in days:
        iso = ts.isocalendar()
        key = f"{int(iso.year):04d}W{int(iso.week):02d}"
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def _load_cached_rows(cache_file: str) -> list[dict[str, Any]]:
    if not os.path.exists(cache_file):
        return []
    df = pd.read_csv(cache_file, dtype={"orgUnit": str, "period": str})
    if df.empty:
        return []
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    records = df.to_dict(orient="records")
    return [
        {"orgUnit": str(record["orgUnit"]), "period": str(record["period"]), "value": float(record["value"])}
        for record in records
    ]


def _write_cached_rows(cache_file: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    df = pd.DataFrame(rows)
    deduped = df.drop_duplicates(subset=["orgUnit", "period"], keep="last").sort_values(by=["orgUnit", "period"])
    deduped.to_csv(cache_file, index=False)


class DataAggregateProcessor(BaseProcessor):
    """Process wrapper for workflow aggregation step."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = DataAggregateInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        dataset = xr.open_mfdataset(inputs.files, combine="by_coords")
        try:
            data_var = _resolve_value_var(dataset)
            data_array = dataset[data_var]
            start_dt = pd.Timestamp(inputs.start_date)
            end_dt = pd.Timestamp(inputs.end_date)
            target_periods = _target_periods(start_dt, end_dt, inputs.temporal_resolution)
            target_period_set = set(target_periods)
            target_org_units = {str(item["orgUnit"]) for item in inputs.valid_features}

            scope_token = _scope_token_from_files(inputs.files, inputs.stage, inputs.flavor)
            cache_key = _cache_key(
                scope_token=scope_token,
                spatial_reducer=inputs.spatial_reducer,
                temporal_resolution=inputs.temporal_resolution,
                temporal_reducer=inputs.temporal_reducer,
                value_rounding=inputs.value_rounding,
            )
            cache_file = os.path.join(DOWNLOAD_DIR, "aggregation_cache", f"{cache_key}.csv")
            cached_rows = _load_cached_rows(cache_file)
            cached_by_key = {(str(r["orgUnit"]), str(r["period"])): r for r in cached_rows}

            rows: list[dict[str, Any]] = []
            computed_rows: list[dict[str, Any]] = []
            for item in inputs.valid_features:
                org_unit_id = str(item["orgUnit"])
                geometry = item["geometry"]
                missing_periods = [period for period in target_periods if (org_unit_id, period) not in cached_by_key]
                if not missing_periods:
                    continue
                series = _clip_spatial_series(data_array, geometry, inputs.spatial_reducer)
                if series.empty:
                    continue
                period_values = _apply_temporal_aggregation(
                    series,
                    inputs.temporal_resolution,
                    inputs.temporal_reducer,
                    start_dt,
                    end_dt,
                )
                for period, value in period_values:
                    if pd.isna(value):
                        continue
                    if period in target_period_set and period in missing_periods:
                        computed_rows.append(
                            {
                                "orgUnit": org_unit_id,
                                "period": period,
                                "value": round(value, inputs.value_rounding),
                            }
                        )
        finally:
            dataset.close()

        merged_rows_map = dict(cached_by_key)
        for row in computed_rows:
            merged_rows_map[(str(row["orgUnit"]), str(row["period"]))] = row

        rows = [
            row
            for (org_unit, period), row in merged_rows_map.items()
            if org_unit in target_org_units and period in target_period_set
        ]
        if not rows:
            raise ProcessorExecuteError("No non-empty aggregated values were produced for the selected features")

        _write_cached_rows(cache_file, list(merged_rows_map.values()))

        return "application/json", {
            "rows": sorted(rows, key=lambda item: (str(item["orgUnit"]), str(item["period"]))),
            "cache": {
                "key": cache_key,
                "file": cache_file,
                "cached_rows_reused": len(rows) - len(computed_rows),
                "computed_rows_delta": len(computed_rows),
            },
        }

    def __repr__(self) -> str:
        return "<DataAggregateProcessor>"
