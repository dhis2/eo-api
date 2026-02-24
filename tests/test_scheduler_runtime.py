from datetime import UTC, datetime

from eoapi.scheduler_runtime import poll_scheduler_once


def test_poll_scheduler_once_triggers_due_schedule(monkeypatch) -> None:
    monkeypatch.setenv("EOAPI_INTERNAL_SCHEDULER_ENABLED", "true")

    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "eoapi.scheduler_runtime.list_schedules",
        lambda: [
            {
                "scheduleId": "sch-1",
                "enabled": True,
                "cron": "* * * * *",
                "timezone": "UTC",
                "created": "2026-01-01T00:00:00Z",
                "lastRunAt": "2026-01-01T00:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "eoapi.scheduler_runtime.execute_schedule_target",
        lambda schedule_id, trigger: calls.append((schedule_id, trigger)),
    )

    poll_scheduler_once(now_utc=datetime(2026, 1, 1, 0, 2, tzinfo=UTC))

    assert calls == [("sch-1", "internal-cron")]


def test_poll_scheduler_once_noop_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("EOAPI_INTERNAL_SCHEDULER_ENABLED", "false")

    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "eoapi.scheduler_runtime.execute_schedule_target",
        lambda schedule_id, trigger: calls.append((schedule_id, trigger)),
    )

    poll_scheduler_once(now_utc=datetime(2026, 1, 1, 0, 2, tzinfo=UTC))

    assert calls == []
