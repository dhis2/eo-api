"""Template workflow specs (scaffold) for CHIRPS and WorldPop."""

from __future__ import annotations

from eo_api.integrations.workflow_spec import WorkflowNodeSpec, WorkflowSpec


def chirps3_dhis2_template() -> WorkflowSpec:
    """Return scaffold CHIRPS3 workflow template."""
    return WorkflowSpec(
        workflow_id="chirps3-dhis2-template",
        nodes=[
            WorkflowNodeSpec(
                id="feature_fetch",
                component="feature.resolve",
                params={
                    "features_geojson": "{{input.features_geojson}}",
                    "org_unit_level": "{{input.org_unit_level}}",
                    "parent_org_unit": "{{input.parent_org_unit}}",
                    "org_unit_ids": "{{input.org_unit_ids}}",
                    "org_unit_id_property": "{{input.org_unit_id_property}}",
                    "bbox": "{{input.bbox}}",
                },
            ),
            WorkflowNodeSpec(
                id="chirps_download",
                component="chirps.fetch",
                params={
                    "start": "{{input.start_month}}",
                    "end": "{{input.end_month}}",
                    "bbox": "{{feature_fetch.effective_bbox}}",
                    "stage": "{{input.stage}}",
                    "flavor": "{{input.flavor}}",
                },
            ),
            WorkflowNodeSpec(
                id="aggregate",
                component="chirps.aggregate",
                params={
                    "start_date": "{{input.start_date}}",
                    "end_date": "{{input.end_date}}",
                    "files": "{{chirps_download.files}}",
                    "valid_features": "{{feature_fetch.valid_features}}",
                    "spatial_reducer": "{{input.spatial_reducer}}",
                    "temporal_resolution": "{{input.temporal_resolution}}",
                    "temporal_reducer": "{{input.temporal_reducer}}",
                    "value_rounding": "{{input.value_rounding}}",
                    "stage": "{{input.stage}}",
                    "flavor": "{{input.flavor}}",
                },
            ),
            WorkflowNodeSpec(
                id="datavalues_build",
                component="dhis2.datavalues.build",
                params={
                    "rows": "{{aggregate.rows}}",
                    "data_element": "{{input.data_element}}",
                    "category_option_combo": "{{input.category_option_combo}}",
                    "attribute_option_combo": "{{input.attribute_option_combo}}",
                    "data_set": "{{input.data_set}}",
                },
            ),
        ],
    )


def worldpop_dhis2_template() -> WorkflowSpec:
    """Return scaffold WorldPop workflow template."""
    return WorkflowSpec(
        workflow_id="worldpop-dhis2-template",
        nodes=[
            WorkflowNodeSpec(
                id="feature_fetch",
                component="feature.resolve",
                params={
                    "features_geojson": "{{input.features_geojson}}",
                    "org_unit_level": "{{input.org_unit_level}}",
                    "parent_org_unit": "{{input.parent_org_unit}}",
                    "org_unit_ids": "{{input.org_unit_ids}}",
                    "org_unit_id_property": "{{input.org_unit_id_property}}",
                    "bbox": "{{input.bbox}}",
                },
            ),
            WorkflowNodeSpec(
                id="worldpop_sync",
                component="worldpop.sync",
                params={
                    "country_code": "{{input.country_code}}",
                    "bbox": "{{input.bbox}}",
                    "start_year": "{{input.start_year}}",
                    "end_year": "{{input.end_year}}",
                    "output_format": "{{input.output_format}}",
                    "dry_run": "{{input.dry_run}}",
                },
            ),
        ],
    )
