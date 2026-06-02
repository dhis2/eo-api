from collections.abc import Callable, Coroutine
from typing import cast

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from starlette.datastructures import URL
from starlette.responses import StreamingResponse

from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.system import routes as system_routes
from open_climate_service.system import templates as system_templates


class _FakeRequest:
    def __init__(self, form_data: dict[str, str]) -> None:
        self._form_data = form_data
        self.base_url = URL("http://testserver/")

    async def form(self) -> dict[str, str]:
        return self._form_data


@pytest.fixture(autouse=True)
def _clear_template_cache() -> None:
    system_templates._cache.clear()


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
        cast(Request, _FakeRequest({"dataset_id": "chirps3_precipitation_daily", "end": "2026-02-10", "publish": "on"}))
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
        cast(Request, _FakeRequest({"dataset_id": "chirps3_precipitation_daily", "end": "", "publish": "on"}))
    )

    assert isinstance(response, StreamingResponse)
    assert len(scheduled) == 1
    await scheduled[0]
    assert captured == {
        "dataset_id": "chirps3_precipitation_daily",
        "end": None,
        "publish": True,
    }


def test_manage_page_shows_split_publication_and_sync_columns(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(system_templates, "_load_templates", lambda: [])
    monkeypatch.setattr(system_templates, "_load_extent", lambda: {"id": "sle", "name": "Sierra Leone", "bbox": []})
    monkeypatch.setattr(
        system_templates,
        "_load_datasets",
        lambda: [
            type(
                "Dataset",
                (),
                {
                    "dataset_id": "chirps3_precipitation_daily'quoted",
                    "dataset_name": "CHIRPS3 precipitation",
                    "period_type": "daily",
                    "extent": type(
                        "Extent",
                        (),
                        {"temporal": type("Temporal", (), {"start": "2026-01-01", "end": "2026-01-10"})()},
                    )(),
                    "publication": type("Publication", (), {"status": "published"})(),
                },
            )()
        ],
    )

    response = client.get("/manage")

    assert response.status_code == 200
    assert "<th>Publication</th>" in response.text
    assert "<th>Sync</th>" in response.text
    assert "Start sync" in response.text
    assert "Cutoff end" in response.text
    assert 'data-dataset-id="chirps3_precipitation_daily&#39;quoted"' in response.text
    assert 'onclick="openSyncPanel(this.dataset.datasetId)"' in response.text
    assert 'onclick="closeSyncPanel(this.dataset.datasetId)"' in response.text


def test_map_viewer_initializes_at_latest_timestep(client: TestClient) -> None:
    response = client.get("/map")

    assert response.status_code == 200
    assert "function initialTimeIndex()" in response.text
    assert "{ [timeDimKey]: initialTimeIndex() }" in response.text
    assert "timeSlider.value = initialIndex;" in response.text
