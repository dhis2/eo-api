from eo_api.integrations.orchestration.capabilities import (
    build_generic_workflow_capabilities_document,
    list_supported_datasets,
    load_dataset_capabilities,
)
from eo_api.routers.ogcapi.plugins.processes.generic_dhis2_workflow import PROCESS_METADATA


def test_dataset_capability_catalog_loads_expected_datasets() -> None:
    catalog = load_dataset_capabilities()
    assert catalog.version == "0.2.0"
    assert set(catalog.datasets.keys()) == {"chirps3", "worldpop"}
    assert "zarr" in catalog.datasets["chirps3"].provider_capabilities.supported_output_formats
    assert "zarr" not in catalog.datasets["chirps3"].integration_capabilities.supported_output_formats
    assert "geotiff" in catalog.datasets["worldpop"].provider_capabilities.supported_output_formats
    assert "geotiff" not in catalog.datasets["worldpop"].integration_capabilities.supported_output_formats


def test_supported_dataset_list_matches_generic_process_enum() -> None:
    datasets = list_supported_datasets()
    process_enum = PROCESS_METADATA["inputs"]["dataset_type"]["schema"]["enum"]  # type: ignore[index]
    assert datasets == process_enum


def test_generic_capabilities_document_has_datasets_components_and_workflows() -> None:
    doc = build_generic_workflow_capabilities_document()
    assert doc["processId"] == "generic-dhis2-workflow"
    assert "chirps3" in doc["datasets"]
    assert "worldpop" in doc["datasets"]
    assert "provider_capabilities" in doc["datasets"]["chirps3"]
    assert "integration_capabilities" in doc["datasets"]["chirps3"]
    assert "collections" in doc["datasets"]["chirps3"]
    assert doc["datasets"]["chirps3"]["collections"]["source"][0]["id"] == "generic-chirps3-source"
    assert "collections" in doc
    assert any(item["id"] == "generic-chirps3-source" for item in doc["collections"])
    assert "dataset_capabilities" not in doc["workflowDefinitions"]
    assert any(item["id"] == "workflow.features" for item in doc["components"])
    assert "chirps3-dhis2-template" in doc["workflowDefinitions"]
    assert "worldpop-dhis2-template" in doc["workflowDefinitions"]
