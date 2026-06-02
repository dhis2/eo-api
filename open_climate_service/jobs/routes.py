"""HTTP routes for native job status and cancellation."""

from fastapi import APIRouter

from open_climate_service.jobs.models import JobListResponse, JobRecord
from open_climate_service.jobs.service import get_job_service

router = APIRouter()


@router.get("", response_model=JobListResponse)
def list_jobs() -> JobListResponse:
    """Return all persisted native jobs."""
    return get_job_service().list_jobs()


@router.get("/{job_id}", response_model=JobRecord)
def get_job(job_id: str) -> JobRecord:
    """Return one native job."""
    return get_job_service().get_job_or_404(job_id)


@router.delete("/{job_id}", response_model=JobRecord)
def cancel_job(job_id: str) -> JobRecord:
    """Request cooperative cancellation for one native job."""
    return get_job_service().request_cancellation(job_id)
