"""Read-only operator routes for dataset schedules."""

from fastapi import APIRouter

from open_climate_service.scheduler.schemas import ScheduleListResponse
from open_climate_service.scheduler.service import get_scheduler_service

router = APIRouter()


@router.get("", response_model=ScheduleListResponse)
def list_schedules() -> ScheduleListResponse:
    """Return configured schedules with process-local runtime status."""
    return get_scheduler_service().status()
