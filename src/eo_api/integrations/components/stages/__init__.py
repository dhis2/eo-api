"""Workflow stage wrappers used by orchestration."""

from eo_api.integrations.components.stages.dhis2_payload_builder_stage import run_dhis2_payload_builder_stage
from eo_api.integrations.components.stages.download_stage import run_download_stage
from eo_api.integrations.components.stages.feature_stage import run_feature_stage
from eo_api.integrations.components.stages.spatial_aggregation_stage import run_spatial_aggregation_stage
from eo_api.integrations.components.stages.temporal_aggregation_stage import run_temporal_aggregation_stage

__all__ = [
    "run_feature_stage",
    "run_download_stage",
    "run_temporal_aggregation_stage",
    "run_spatial_aggregation_stage",
    "run_dhis2_payload_builder_stage",
]
