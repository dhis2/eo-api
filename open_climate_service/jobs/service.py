"""Runtime service for native asynchronous process jobs."""

from __future__ import annotations

import inspect
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Protocol
from uuid import uuid4

from fastapi import HTTPException

from open_climate_service.data_registry.services import processes as process_registry
from open_climate_service.jobs import store
from open_climate_service.jobs.models import (
    JobCancelledError,
    JobError,
    JobLink,
    JobListResponse,
    JobProgress,
    JobRecord,
    JobStatus,
)
from open_climate_service.shared.time import utc_now

logger = logging.getLogger(__name__)


def _retry_delay_seconds(attempt: int) -> int:
    """Return the retry delay in seconds for a given failed attempt count."""
    exponent = attempt - 1 if attempt > 1 else 0
    return int(min(240, 60 * (2**exponent)))


def _job_links(job_id: str) -> list[JobLink]:
    return [JobLink(href=f"/jobs/{job_id}", rel="self", title="Job detail")]


def _catalog_links() -> list[JobLink]:
    return [JobLink(href="/jobs", rel="self", title="Jobs")]


def _supports_argument(func: Any, name: str) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    if name in signature.parameters:
        return True
    return any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())


def _is_pre_execution_cancellation(record: JobRecord) -> bool:
    return record.cancel_requested and record.status in {JobStatus.ACCEPTED, JobStatus.RETRYING}


class JobExecutionContext:
    """Callbacks and state helpers exposed to one running job."""

    def __init__(self, service: "JobService", job_id: str) -> None:
        self._service = service
        self.job_id = job_id

    def report_progress(self, done: int | None = None, total: int | None = None, message: str | None = None) -> None:
        self._service.update_progress(self.job_id, done=done, total=total, message=message)

    def is_cancel_requested(self) -> bool:
        record = store.get_job_record(self.job_id)
        return bool(record and record.cancel_requested)

    def save_cursor(self, cursor: dict[str, Any]) -> None:
        self._service.save_cursor(self.job_id, cursor)

    def load_cursor(self) -> dict[str, Any] | None:
        record = store.get_job_record(self.job_id)
        return None if record is None else record.cursor


class ProcessExecutor(Protocol):
    """Execution backend contract for native jobs."""

    kind: str

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Future[None]:
        """Submit one callable for asynchronous execution."""
        ...

    def shutdown(self) -> None:
        """Release executor resources."""
        ...


class ThreadProcessExecutor:
    """Default in-process thread-backed job executor."""

    kind = "thread"

    def __init__(self, *, max_workers: int = 4) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="climate-service-job")

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Future[None]:
        """Submit one callable to the thread pool."""
        return self._pool.submit(fn, *args, **kwargs)

    def shutdown(self) -> None:
        """Stop the thread pool without waiting for queued work to finish."""
        self._pool.shutdown(wait=False, cancel_futures=True)


class JobService:
    """Persisted job store plus in-process executor runtime."""

    def __init__(self, *, executor: ProcessExecutor | None = None, max_workers: int = 4) -> None:
        self._executor = executor or ThreadProcessExecutor(max_workers=max_workers)
        self._futures: dict[str, Future[None]] = {}
        self._lock = threading.Lock()

    def shutdown(self) -> None:
        """Stop the executor without waiting for outstanding work."""
        self._executor.shutdown()

    def list_jobs(self) -> JobListResponse:
        """Return all persisted jobs ordered by creation time descending."""
        records = sorted(store.list_job_records(), key=lambda record: record.created_at, reverse=True)
        return JobListResponse(jobs=records, links=_catalog_links())

    def get_job_or_404(self, job_id: str) -> JobRecord:
        """Return one persisted job or raise 404."""
        record = store.get_job_record(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return record

    def submit_callable_job(
        self,
        *,
        func: Any,
        label: str,
        request: dict[str, Any],
        max_attempts: int = 1,
    ) -> JobRecord:
        """Submit a callable directly as a background job — no YAML registry needed.

        ``func`` must be a module-level function so its dotted path can be stored
        in the job record and re-imported on restart.  ``label`` is the human-readable
        process name shown in GET /jobs/{id} as ``processID``.
        """
        qualname = getattr(func, "__qualname__", "")
        if "<locals>" in qualname:
            raise ValueError(f"submit_callable_job requires a module-level function; got {qualname!r}")
        fn_path = f"{func.__module__}.{qualname}"
        # Strip any caller-supplied __fn_path__ to prevent function-path injection,
        # then set the reserved key so it cannot be overridden.
        safe_request = {k: v for k, v in request.items() if k != "__fn_path__"}
        safe_request["__fn_path__"] = fn_path
        return self._create_and_enqueue(
            process_id=label,
            request=safe_request,
            max_attempts=max_attempts,
        )

    def submit_process_job(
        self,
        *,
        process_id: str,
        request: dict[str, Any],
        max_attempts: int = 1,
    ) -> JobRecord:
        """Submit a YAML-registered process as a background job."""
        process = process_registry.get_process(process_id)
        if process is None or not process["expose"]:
            raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
        # Strip the reserved key so a caller-supplied __fn_path__ cannot hijack execution.
        safe_request = {k: v for k, v in request.items() if k != "__fn_path__"}
        return self._create_and_enqueue(process_id=process_id, request=safe_request, max_attempts=max_attempts)

    def _create_and_enqueue(
        self,
        *,
        process_id: str,
        request: dict[str, Any],
        max_attempts: int,
    ) -> JobRecord:
        job_id = str(uuid4())
        record = JobRecord(
            job_id=job_id,
            process_id=process_id,
            status=JobStatus.ACCEPTED,
            created_at=utc_now(),
            max_attempts=max_attempts,
            executor_kind=self._executor.kind,
            request=request,
            links=_job_links(job_id),
        )
        store.create_job_record(record)
        self._enqueue_job(record.job_id)
        return self.get_job_or_404(record.job_id)

    def request_cancellation(self, job_id: str) -> JobRecord:
        """Request cooperative cancellation for a job."""
        self.get_job_or_404(job_id)
        record = store.mutate_job_record(
            job_id,
            lambda current: (
                current
                if current.status in {JobStatus.SUCCESSFUL, JobStatus.FAILED, JobStatus.CANCELLED}
                else current.model_copy(update={"cancel_requested": True})
            ),
        )

        if record.status == JobStatus.ACCEPTED:
            with self._lock:
                future = self._futures.get(job_id)
            if future is not None and future.cancel():
                record = store.mutate_job_record(
                    job_id,
                    lambda current: current.model_copy(
                        update={
                            "status": JobStatus.CANCELLED,
                            "finished_at": utc_now(),
                            "progress": JobProgress(message="Cancellation accepted before execution started"),
                        }
                    ),
                )
        return record

    def recover_pending_jobs(self) -> None:
        """Requeue interrupted jobs on startup."""
        for record in store.list_job_records():
            if record.status not in {JobStatus.ACCEPTED, JobStatus.RUNNING, JobStatus.RETRYING}:
                continue
            if record.cancel_requested:
                store.mutate_job_record(
                    record.job_id,
                    lambda current: current.model_copy(
                        update={
                            "status": JobStatus.CANCELLED,
                            "finished_at": utc_now(),
                            "progress": JobProgress(message="Cancelled before recovery requeue"),
                        }
                    ),
                )
                continue
            if record.status == JobStatus.RUNNING:
                store.mutate_job_record(
                    record.job_id,
                    lambda current: current.model_copy(
                        update={
                            "status": JobStatus.ACCEPTED,
                            "attempt": max(0, current.attempt - 1),
                            "finished_at": None,
                            "retry_after": None,
                            "error": None,
                            "progress": JobProgress(message="Requeued after restart during execution"),
                        }
                    ),
                )
            self._enqueue_job(record.job_id)

    def update_progress(
        self,
        job_id: str,
        *,
        done: int | None = None,
        total: int | None = None,
        message: str | None = None,
    ) -> JobRecord:
        """Persist progress details for one job."""

        def _mutation(current: JobRecord) -> JobRecord:
            current_done = done if done is not None else current.progress.done
            current_total = total if total is not None else current.progress.total
            percent: float | None = current.progress.percent
            if current_done is not None and current_total is not None and current_total != 0:
                percent = round((current_done / current_total) * 100.0, 2)
            progress = JobProgress(
                done=current_done,
                total=current_total,
                percent=percent,
                message=message if message is not None else current.progress.message,
            )
            return current.model_copy(update={"progress": progress})

        return store.mutate_job_record(job_id, _mutation)

    def save_cursor(self, job_id: str, cursor: dict[str, Any]) -> JobRecord:
        """Persist a lightweight checkpoint cursor for one job."""
        return store.mutate_job_record(job_id, lambda current: current.model_copy(update={"cursor": dict(cursor)}))

    def _enqueue_job(self, job_id: str) -> None:
        with self._lock:
            existing = self._futures.get(job_id)
            if existing is not None and not existing.done():
                return
            future = self._executor.submit(self._run_job, job_id)
            self._futures[job_id] = future

    def _sleep_for_retry(self, job_id: str, seconds: int) -> bool:
        """Sleep in short intervals so retry wait remains cancellation-aware."""
        remaining = float(seconds)
        while remaining > 0:
            record = store.get_job_record(job_id)
            if record is not None and record.cancel_requested:
                return False
            interval = min(1.0, remaining)
            time.sleep(interval)
            remaining -= interval
        return True

    def _run_job(self, job_id: str) -> None:
        try:
            self._execute_job(job_id)
        finally:
            with self._lock:
                self._futures.pop(job_id, None)

    def _execute_job(self, job_id: str) -> None:
        while True:
            record = self.get_job_or_404(job_id)
            if _is_pre_execution_cancellation(record):
                message = (
                    "Cancellation accepted before execution started"
                    if record.status == JobStatus.ACCEPTED
                    else "Cancelled before retry execution resumed"
                )
                store.mutate_job_record(
                    job_id,
                    lambda current: current.model_copy(
                        update={
                            "status": JobStatus.CANCELLED,
                            "finished_at": utc_now(),
                            "progress": JobProgress(message=message),
                        }
                    ),
                )
                return

            started = store.mutate_job_record(
                job_id,
                lambda current: current.model_copy(
                    update={
                        "status": JobStatus.RUNNING,
                        "started_at": current.started_at or utc_now(),
                        "finished_at": None,
                        "attempt": current.attempt + 1,
                        "retry_after": None,
                        "error": None,
                    }
                ),
            )

            try:
                result = self._invoke_process(started)
                store.mutate_job_record(
                    job_id,
                    lambda current: current.model_copy(
                        update={
                            "status": JobStatus.SUCCESSFUL,
                            "finished_at": utc_now(),
                            "result": result,
                            "progress": JobProgress(
                                done=current.progress.done,
                                total=current.progress.total,
                                percent=current.progress.percent,
                                message="Completed",
                            ),
                        }
                    ),
                )
                return
            except JobCancelledError:
                store.mutate_job_record(
                    job_id,
                    lambda current: current.model_copy(
                        update={
                            "status": JobStatus.CANCELLED,
                            "finished_at": utc_now(),
                            "progress": JobProgress(
                                done=current.progress.done,
                                total=current.progress.total,
                                percent=current.progress.percent,
                                message="Cancelled",
                            ),
                        }
                    ),
                )
                return
            except Exception as exc:
                logger.exception("Job %s failed", job_id)
                error = JobError(type=type(exc).__name__, message=str(exc))
                if started.attempt < started.max_attempts:
                    retry_after = _retry_delay_seconds(started.attempt)
                    store.mutate_job_record(
                        job_id,
                        lambda latest: latest.model_copy(
                            update={
                                "status": JobStatus.RETRYING,
                                "retry_after": retry_after,
                                "error": error,
                                "progress": JobProgress(message="Retry scheduled"),
                            }
                        ),
                    )
                    if not self._sleep_for_retry(job_id, retry_after):
                        continue
                    continue

                store.mutate_job_record(
                    job_id,
                    lambda latest: latest.model_copy(
                        update={
                            "status": JobStatus.FAILED,
                            "finished_at": utc_now(),
                            "error": error,
                            "progress": JobProgress(
                                done=latest.progress.done,
                                total=latest.progress.total,
                                percent=latest.progress.percent,
                                message="Failed",
                            ),
                        }
                    ),
                )
                return

    def _invoke_process(self, record: JobRecord) -> Any:
        fn_path = record.request.get("__fn_path__")
        if fn_path and isinstance(fn_path, str):
            func = process_registry._get_dynamic_function(fn_path)
        else:
            process = process_registry.get_process(record.process_id)
            if process is None or not process["expose"]:
                raise ValueError(f"Unknown process '{record.process_id}'")
            func = process_registry.get_process_function(record.process_id)
        context = JobExecutionContext(self, record.job_id)
        kwargs = {k: v for k, v in record.request.items() if k != "__fn_path__"}
        if _supports_argument(func, "on_progress"):
            kwargs["on_progress"] = context.report_progress
        if _supports_argument(func, "is_cancel_requested"):
            kwargs["is_cancel_requested"] = context.is_cancel_requested
        if _supports_argument(func, "load_cursor"):
            kwargs["load_cursor"] = context.load_cursor
        if _supports_argument(func, "save_cursor"):
            kwargs["save_cursor"] = context.save_cursor
        return func(**kwargs)


_job_service: JobService | None = None


def get_job_service() -> JobService:
    """Return the singleton native job runtime."""
    global _job_service
    if _job_service is None:
        _job_service = JobService()
    return _job_service


def reset_job_service() -> None:
    """Reset the singleton runtime for tests."""
    global _job_service
    if _job_service is not None:
        _job_service.shutdown()
    _job_service = None
