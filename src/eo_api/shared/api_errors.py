"""Shared typed API error helpers."""

from __future__ import annotations

from typing import NoReturn

from fastapi import HTTPException
from pydantic import BaseModel


class ApiErrorResponse(BaseModel):
    """Stable API error envelope."""

    error: str
    error_code: str
    message: str
    resource_id: str | None = None
    process_id: str | None = None
    job_id: str | None = None
    run_id: str | None = None
    schedule_id: str | None = None
    status: str | None = None
    failed_component: str | None = None
    failed_component_version: str | None = None


def api_error(
    *,
    error: str,
    error_code: str,
    message: str,
    resource_id: str | None = None,
    process_id: str | None = None,
    job_id: str | None = None,
    run_id: str | None = None,
    schedule_id: str | None = None,
    status: str | None = None,
    failed_component: str | None = None,
    failed_component_version: str | None = None,
) -> dict[str, str]:
    """Build a stable API error envelope."""
    return ApiErrorResponse(
        error=error,
        error_code=error_code,
        message=message,
        resource_id=resource_id,
        process_id=process_id,
        job_id=job_id,
        run_id=run_id,
        schedule_id=schedule_id,
        status=status,
        failed_component=failed_component,
        failed_component_version=failed_component_version,
    ).model_dump(exclude_none=True)


def raise_api_error(
    status_code: int,
    *,
    error: str,
    error_code: str,
    message: str,
    resource_id: str | None = None,
    process_id: str | None = None,
    job_id: str | None = None,
    run_id: str | None = None,
    status: str | None = None,
    failed_component: str | None = None,
    failed_component_version: str | None = None,
) -> NoReturn:
    """Raise an HTTPException using the shared typed error envelope."""
    raise HTTPException(
        status_code=status_code,
        detail=api_error(
            error=error,
            error_code=error_code,
            message=message,
            resource_id=resource_id,
            process_id=process_id,
            job_id=job_id,
            run_id=run_id,
            status=status,
            failed_component=failed_component,
            failed_component_version=failed_component_version,
        ),
    )
