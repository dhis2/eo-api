"""File-based storage for openEO user-defined processes (UDPs)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import portalocker

from open_climate_service import config as api_config
from open_climate_service.openeo.schemas import UDPListResponse, UDPRecord

_T = TypeVar("_T")


def _resolve_udp_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "process_graphs"
    import os

    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-service" / "process_graphs"


UDP_DIR = _resolve_udp_dir()
UDP_INDEX_PATH = UDP_DIR / "process_graphs.json"


def _ensure_store() -> None:
    UDP_DIR.mkdir(parents=True, exist_ok=True)
    if not UDP_INDEX_PATH.exists():
        UDP_INDEX_PATH.write_text("[]\n", encoding="utf-8")


def list_udps() -> UDPListResponse:
    """Return all stored user-defined processes."""
    records = [UDPRecord.model_validate(raw) for raw in _load_records()]
    return UDPListResponse(
        processes=records,
        links=[{"rel": "self", "href": "/process_graphs", "type": "application/json"}],
    )


def get_udp(process_graph_id: str) -> UDPRecord | None:
    """Return one UDP by id, or None if not found."""
    for raw in _load_records():
        if raw.get("id") == process_graph_id:
            return UDPRecord.model_validate(raw)
    return None


def put_udp(process_graph_id: str, body: dict[str, object]) -> UDPRecord:
    """Store (create or replace) a user-defined process."""
    record = UDPRecord.model_validate({**body, "id": process_graph_id})

    def _mutation(records: list[dict[str, object]]) -> UDPRecord:
        payload = record.model_dump(mode="json")
        for index, existing in enumerate(records):
            if existing.get("id") == process_graph_id:
                records[index] = payload
                return record
        records.append(payload)
        return record

    return _mutate_records(_mutation)


def delete_udp(process_graph_id: str) -> bool:
    """Delete a UDP; returns True if it existed."""

    def _mutation(records: list[dict[str, object]]) -> bool:
        for index, existing in enumerate(records):
            if existing.get("id") == process_graph_id:
                records.pop(index)
                return True
        return False

    return _mutate_records(_mutation)


def _load_records() -> list[dict[str, object]]:
    _ensure_store()
    return _read_records_from_disk()


def _read_records_from_disk() -> list[dict[str, object]]:
    with open(UDP_INDEX_PATH, encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_SH)
        try:
            payload = json.load(handle)
        finally:
            portalocker.unlock(handle)
    if not isinstance(payload, list):
        raise ValueError("process_graphs.json must contain a list")
    return payload


def _mutate_records(mutation: Callable[[list[dict[str, object]]], _T]) -> _T:
    _ensure_store()
    with open(UDP_INDEX_PATH, "r+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX)
        try:
            payload = json.load(handle)
            records: list[dict[str, object]] = payload if isinstance(payload, list) else []
            result = mutation(records)
            handle.seek(0)
            json.dump(records, handle, indent=2)
            handle.write("\n")
            handle.truncate()
            return result
        finally:
            portalocker.unlock(handle)
