"""User-facing management routes for per-dataset schedules."""

from fastapi import APIRouter, Response, status

from open_climate_service.scheduler.dispatcher import CheckResult
from open_climate_service.scheduler.models import SchedulePatch, SchedulePut
from open_climate_service.scheduler.schemas import ScheduleListResponse, ScheduleStatus
from open_climate_service.scheduler.service import get_scheduler_service

router = APIRouter()


@router.get("", response_model=ScheduleListResponse)
def list_schedules() -> ScheduleListResponse:
    """Return all persisted schedules with their runtime status."""
    return get_scheduler_service().status()


@router.get("/{dataset_id}", response_model=ScheduleStatus)
def get_schedule(dataset_id: str) -> ScheduleStatus:
    """Return the schedule for one managed dataset."""
    return get_scheduler_service().get_status(dataset_id)


@router.put("/{dataset_id}", response_model=ScheduleStatus)
def put_schedule(dataset_id: str, request: SchedulePut) -> ScheduleStatus:
    """Create or replace the only schedule for a managed dataset."""
    return get_scheduler_service().put(dataset_id, request)


@router.patch("/{dataset_id}", response_model=ScheduleStatus)
def patch_schedule(dataset_id: str, request: SchedulePatch) -> ScheduleStatus:
    """Change timing, enablement, publication, or retry settings."""
    return get_scheduler_service().patch(dataset_id, request)


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_schedule(dataset_id: str) -> Response:
    """Delete a schedule without deleting its dataset or job history."""
    get_scheduler_service().delete(dataset_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{dataset_id}/run", response_model=CheckResult, status_code=status.HTTP_202_ACCEPTED)
def run_schedule_now(dataset_id: str) -> CheckResult:
    """Enqueue the scheduled sync immediately without changing its cadence."""
    return get_scheduler_service().run_now(dataset_id)
