from collections.abc import Callable, Coroutine

import pytest
from starlette.datastructures import URL
from starlette.responses import StreamingResponse

from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.system import routes as system_routes


class _FakeRequest:
    def __init__(self, form_data: dict[str, str]) -> None:
        self._form_data = form_data
        self.base_url = URL("http://testserver/")

    async def form(self) -> dict[str, str]:
        return self._form_data


@pytest.mark.anyio  # pyright: ignore[reportUntypedFunctionDecorator]
async def test_manage_sync_forwards_provided_end(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    scheduled: list[Coroutine[object, object, None]] = []

    def fake_sync_dataset(
        *,
        dataset_id: str,
        end: str | None,
        publish: bool,
        on_progress: Callable[[int | None, int | None, str | None], None],
    ) -> None:
        captured["dataset_id"] = dataset_id
        captured["end"] = end
        captured["publish"] = publish
        on_progress(1, 1, "done")

    async def fake_to_thread(func: Callable[[], None]) -> None:
        func()

    def fake_create_task(coro: Coroutine[object, object, None]) -> None:
        scheduled.append(coro)
        return None

    monkeypatch.setattr(ingestion_services, "sync_dataset", fake_sync_dataset)
    monkeypatch.setattr(system_routes.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(system_routes.asyncio, "create_task", fake_create_task)

    response = await system_routes.manage_sync(
        _FakeRequest({"dataset_id": "chirps3_precipitation_daily", "end": "2026-02-10", "publish": "on"})
    )

    assert isinstance(response, StreamingResponse)
    assert len(scheduled) == 1
    await scheduled[0]
    assert captured == {
        "dataset_id": "chirps3_precipitation_daily",
        "end": "2026-02-10",
        "publish": True,
    }


@pytest.mark.anyio  # pyright: ignore[reportUntypedFunctionDecorator]
async def test_manage_sync_treats_blank_end_as_none(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    scheduled: list[Coroutine[object, object, None]] = []

    def fake_sync_dataset(
        *,
        dataset_id: str,
        end: str | None,
        publish: bool,
        on_progress: Callable[[int | None, int | None, str | None], None],
    ) -> None:
        captured["dataset_id"] = dataset_id
        captured["end"] = end
        captured["publish"] = publish
        on_progress(1, 1, "done")

    async def fake_to_thread(func: Callable[[], None]) -> None:
        func()

    def fake_create_task(coro: Coroutine[object, object, None]) -> None:
        scheduled.append(coro)
        return None

    monkeypatch.setattr(ingestion_services, "sync_dataset", fake_sync_dataset)
    monkeypatch.setattr(system_routes.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(system_routes.asyncio, "create_task", fake_create_task)

    response = await system_routes.manage_sync(
        _FakeRequest({"dataset_id": "chirps3_precipitation_daily", "end": "", "publish": "on"})
    )

    assert isinstance(response, StreamingResponse)
    assert len(scheduled) == 1
    await scheduled[0]
    assert captured == {
        "dataset_id": "chirps3_precipitation_daily",
        "end": None,
        "publish": True,
    }
