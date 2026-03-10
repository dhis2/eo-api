"""Pydantic schemas for OGC API process contracts."""

from eo_api.routers.ogcapi.plugins.processes.schemas.base import CHIRPS3Input, ERA5LandInput
from eo_api.routers.ogcapi.plugins.processes.schemas.generic import (
    GENERIC_DHIS2_WORKFLOW_INPUT_ADAPTER,
    GenericChirps3WorkflowInput,
    GenericDhis2WorkflowInput,
    GenericWorldPopWorkflowInput,
)
from eo_api.routers.ogcapi.plugins.processes.schemas.workflow import (
    ClimateDhis2WorkflowInput,
    DataAggregateInput,
    DataValueBuildInput,
    FeatureFetchInput,
    ProcessOutput,
)
from eo_api.routers.ogcapi.plugins.processes.schemas.worldpop import WorldPopDhis2WorkflowInput, WorldPopSyncInput
from eo_api.routers.ogcapi.plugins.processes.schemas.zonal import ZonalStatisticsInput

__all__ = [
    "CHIRPS3Input",
    "ERA5LandInput",
    "WorldPopSyncInput",
    "WorldPopDhis2WorkflowInput",
    "ZonalStatisticsInput",
    "ClimateDhis2WorkflowInput",
    "FeatureFetchInput",
    "DataValueBuildInput",
    "DataAggregateInput",
    "ProcessOutput",
    "GenericDhis2WorkflowInput",
    "GenericChirps3WorkflowInput",
    "GenericWorldPopWorkflowInput",
    "GENERIC_DHIS2_WORKFLOW_INPUT_ADAPTER",
]
