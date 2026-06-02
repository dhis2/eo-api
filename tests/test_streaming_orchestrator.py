from __future__ import annotations

from pathlib import Path
from typing import Any

import icechunk
import numpy as np
import pytest
import xarray as xr

import open_climate_service.streaming.orchestrator as streaming_orchestrator
import open_climate_service.transforms.pipeline as transform_pipeline
from open_climate_service.streaming.orchestrator import run_streaming_ingest_sync
from open_climate_service.streaming.protocol import GridSpec


class _FakePlugin:
    max_concurrency = 2
    commit_batch_size = 2

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        _ = bbox, params
        return GridSpec(shape=(1, 1), crs=4326, dtype=np.dtype("float32"))

    async def periods(self, start: str, end: str) -> list[str]:
        _ = start, end
        return ["2026-01-01", "2026-01-02", "2026-01-03"]

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = bbox, params
        value = float(period_id[-2:])
        return xr.Dataset(
            {"precip": (("t", "y", "x"), np.array([[[value]]], dtype=np.float32))},
            coords={"t": [np.datetime64(period_id, "D")], "y": [0.0], "x": [1.0]},
        )


class _ShapeMismatchPlugin(_FakePlugin):
    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = bbox, params
        if period_id == "2026-01-02":
            return xr.Dataset(
                {"precip": (("t", "y", "x"), np.array([[[1.0], [2.0]]], dtype=np.float32))},
                coords={"t": [np.datetime64(period_id, "D")], "y": [0.0, 1.0], "x": [1.0]},
            )
        return await super().fetch_period(period_id, bbox, **params)


class _CustomTimeDimPlugin(_FakePlugin):
    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        _ = bbox, params
        # Use a non-default time dimension to exercise the custom-dim code path;
        # production plugins standardise on "t" but this test must verify the
        # orchestrator honours an overridden spec.time_dim.
        return GridSpec(shape=(1, 1), crs=4326, dtype=np.dtype("float32"), time_dim="valid_time")

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = bbox, params
        value = float(period_id[-2:])
        return xr.Dataset(
            {"precip": (("valid_time", "y", "x"), np.array([[[value]]], dtype=np.float32))},
            coords={"valid_time": [np.datetime64(period_id, "D")], "y": [0.0], "x": [1.0]},
        )


class _FakeSession:
    def __init__(self, store: str) -> None:
        self.store = store
        self.messages: list[str] = []

    def commit(self, message: str) -> None:
        self.messages.append(message)


class _FakeRepo:
    def __init__(self, store: str) -> None:
        self.store = store
        self.sessions: list[_FakeSession] = []

    def writable_session(self, branch: str) -> _FakeSession:
        assert branch == "main"
        session = _FakeSession(self.store)
        self.sessions.append(session)
        return session


def _read_committed_periods_from_zarr(store_path: Path, period_type: str, *, time_dim: str = "t") -> set[str]:
    _ = period_type
    if not store_path.exists():
        return set()
    ds = xr.open_zarr(store_path, consolidated=None)
    try:
        if time_dim not in ds.coords:
            return set()
        return {str(item)[:10] for item in ds[time_dim].values}
    finally:
        ds.close()


def test_orchestrator_uses_store_state_as_resume_truth(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store_path = tmp_path / "streaming-store.zarr"
    repo = _FakeRepo(str(store_path))
    progress_updates: list[tuple[int | None, int | None, str | None]] = []
    cursor_saves: list[dict[str, Any]] = []

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator,
        "read_committed_period_ids",
        lambda path, period_type, time_dim="t": _read_committed_periods_from_zarr(path, period_type, time_dim=time_dim),
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: not path.exists())

    result = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        on_progress=lambda done, total, message: progress_updates.append((done, total, message)),
        save_cursor=lambda cursor: cursor_saves.append(cursor),
    )

    assert result.periods_written == 3
    assert store_path.exists()
    first = xr.open_zarr(store_path, consolidated=None)
    try:
        assert first["precip"].values[:, 0, 0].tolist() == [1.0, 2.0, 3.0]
    finally:
        first.close()
    import zarr

    root = zarr.open_group(store_path, mode="r")
    assert root.attrs["proj:code"] == "EPSG:4326"
    assert root.attrs["spatial:dimensions"] == ["x", "y"]
    assert root.attrs["spatial:shape"] == [1, 1]

    run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        on_progress=lambda done, total, message: progress_updates.append((done, total, message)),
        save_cursor=lambda cursor: cursor_saves.append(cursor),
    )

    second = xr.open_zarr(store_path, consolidated=None)
    try:
        assert second["precip"].values[:, 0, 0].tolist() == [1.0, 2.0, 3.0]
    finally:
        second.close()

    assert any(update[2] == "Wrote 2026-01-03" for update in progress_updates)
    assert cursor_saves[-1] == {"last_committed": "2026-01-03"}


def test_orchestrator_refuses_destructive_first_write_when_existing_store_is_not_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "streaming-store.zarr"
    store_path.mkdir()
    repo = _FakeRepo(str(store_path))

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator, "read_committed_period_ids", lambda path, period_type, time_dim="t": set()
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: False)

    with pytest.raises(RuntimeError, match="committed periods could not be determined safely"):
        run_streaming_ingest_sync(
            plugin=_FakePlugin(),
            params={},
            bbox=[0.0, 0.0, 1.0, 1.0],
            start="2026-01-01",
            end="2026-01-03",
            store_path=store_path,
            period_type="daily",
        )


def test_orchestrator_normalizes_invalid_plugin_batching_and_concurrency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _InvalidPlugin(_FakePlugin):
        max_concurrency = 0
        commit_batch_size = 0

    store_path = tmp_path / "streaming-store.zarr"
    repo = _FakeRepo(str(store_path))
    cursor_saves: list[dict[str, Any]] = []

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator,
        "read_committed_period_ids",
        lambda path, period_type, time_dim="t": _read_committed_periods_from_zarr(path, period_type, time_dim=time_dim),
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: not path.exists())

    result = run_streaming_ingest_sync(
        plugin=_InvalidPlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        save_cursor=lambda cursor: cursor_saves.append(cursor),
    )

    assert result.periods_written == 3
    assert cursor_saves[-1] == {"last_committed": "2026-01-03"}


def test_orchestrator_does_not_skip_uncommitted_gaps_before_cursor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "streaming-store.zarr"
    repo = _FakeRepo(str(store_path))

    existing = xr.Dataset(
        {"precip": (("t", "y", "x"), np.array([[[2.0]]], dtype=np.float32))},
        coords={"t": [np.datetime64("2026-01-02", "D")], "y": [0.0], "x": [1.0]},
    )
    existing.to_zarr(store_path, mode="w", zarr_format=3)

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator,
        "read_committed_period_ids",
        lambda path, period_type, time_dim="t": {"2026-01-02"},
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: False)

    result = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )

    assert result.periods_written == 2
    ds = xr.open_zarr(store_path, consolidated=None)
    try:
        assert {str(item)[:10] for item in ds["t"].values} == {"2026-01-01", "2026-01-02", "2026-01-03"}
    finally:
        ds.close()


def test_orchestrator_rejects_spatial_shape_changes_across_appends(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "streaming-store.zarr"
    repo = _FakeRepo(str(store_path))

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator, "read_committed_period_ids", lambda path, period_type, time_dim="t": set()
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: not path.exists())

    with pytest.raises(RuntimeError, match="has spatial shape"):
        run_streaming_ingest_sync(
            plugin=_ShapeMismatchPlugin(),
            params={},
            bbox=[0.0, 0.0, 1.0, 1.0],
            start="2026-01-01",
            end="2026-01-03",
            store_path=store_path,
            period_type="daily",
        )


def test_orchestrator_writes_and_reads_through_real_icechunk_repo(tmp_path: Path) -> None:
    store_path = tmp_path / "streaming-store.icechunk"

    result = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )

    assert result.periods_written == 3
    assert store_path.exists()

    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store)
    try:
        assert ds["precip"].values[:, 0, 0].tolist() == [1.0, 2.0, 3.0]
        assert {str(item)[:10] for item in ds["t"].values} == {"2026-01-01", "2026-01-02", "2026-01-03"}
    finally:
        ds.close()

    rerun = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )

    assert rerun.periods_written == 0


def test_orchestrator_resume_supports_custom_time_dimension(tmp_path: Path) -> None:
    store_path = tmp_path / "streaming-store-custom-time.icechunk"

    first = run_streaming_ingest_sync(
        plugin=_CustomTimeDimPlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )
    assert first.periods_written == 3

    rerun = run_streaming_ingest_sync(
        plugin=_CustomTimeDimPlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )
    assert rerun.periods_written == 0


def test_orchestrator_applies_dataset_transforms_before_write(tmp_path: Path) -> None:
    store_path = tmp_path / "streaming-store-transformed.icechunk"
    dataset = {
        "id": "era5land_precipitation_hourly",
        "variable": "precip",
        "transforms": ["open_climate_service.transforms.metres_to_mm"],
    }

    result = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        dataset=dataset,
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )

    assert result.periods_written == 3

    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store)
    try:
        np.testing.assert_allclose(ds["precip"].values[:, 0, 0], [1000.0, 2000.0, 3000.0])
        assert ds["precip"].attrs["units"] == "mm"
    finally:
        ds.close()


def test_transform_pipeline_raises_clear_error_for_missing_attribute() -> None:
    with pytest.raises(ValueError, match="does not exist"):
        transform_pipeline._get_dynamic_function("open_climate_service.transforms.missing_function")


def test_transform_pipeline_raises_clear_error_for_non_callable_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transform_pipeline, "logger", object())

    with pytest.raises(ValueError, match="is not callable"):
        transform_pipeline._get_dynamic_function("open_climate_service.transforms.pipeline.logger")
