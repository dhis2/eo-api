from __future__ import annotations

import pytest
from fastapi import FastAPI

import open_climate_service.main as main


@pytest.mark.anyio
async def test_lifespan_recovers_jobs_and_shuts_down(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class FakeJobService:
        def recover_pending_jobs(self) -> None:
            calls.append("recover")

        def shutdown(self) -> None:
            calls.append("shutdown")

    class FakeSchedulerService:
        def start(self) -> None:
            calls.append("scheduler-start")

        def shutdown(self) -> None:
            calls.append("scheduler-shutdown")

    monkeypatch.setattr(main, "get_job_service", lambda: FakeJobService())
    monkeypatch.setattr(main, "get_scheduler_service", lambda: FakeSchedulerService())

    async with main._lifespan(FastAPI()):
        assert calls == ["recover", "scheduler-start"]

    assert calls == ["recover", "scheduler-start", "scheduler-shutdown", "shutdown"]
