import pytest

from eo_api.integrations.workflow_executor import execute_workflow_spec
from eo_api.integrations.workflow_registry import (
    ComponentDescriptor,
    ComponentRegistry,
    build_default_component_registry,
)
from eo_api.integrations.workflow_spec import WorkflowNodeSpec, WorkflowSpec
from eo_api.integrations.workflow_templates import chirps3_dhis2_template, worldpop_dhis2_template


def test_workflow_spec_rejects_duplicate_node_ids() -> None:
    with pytest.raises(ValueError, match="Duplicate workflow node ids"):
        WorkflowSpec(
            workflow_id="dup",
            nodes=[
                WorkflowNodeSpec(id="a", component="x"),
                WorkflowNodeSpec(id="a", component="y"),
            ],
        )


def test_default_registry_contains_core_components() -> None:
    registry = build_default_component_registry()
    ids = registry.list_ids()
    assert "feature.resolve" in ids
    assert "chirps.fetch" in ids
    assert "worldpop.sync" in ids


def test_execute_workflow_spec_resolves_refs() -> None:
    registry = ComponentRegistry()
    registry.register(
        ComponentDescriptor(
            component_id="mock.a",
            description="a",
            fn=lambda params, context: {"value": params["x"] + context["base"]},
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="mock.b",
            description="b",
            fn=lambda params, context: {"value": params["from_a"] * 2 + context["base"]},
        )
    )

    spec = WorkflowSpec(
        workflow_id="demo",
        nodes=[
            WorkflowNodeSpec(id="a", component="mock.a", params={"x": "{{input.seed}}"}),
            WorkflowNodeSpec(id="b", component="mock.b", params={"from_a": "{{a.value}}"}),
        ],
    )
    result = execute_workflow_spec(spec=spec, registry=registry, context={"base": 3, "input": {"seed": 4}})

    assert result["status"] == "completed"
    assert result["outputs"]["a"]["value"] == 7
    assert result["outputs"]["b"]["value"] == 17
    assert len(result["workflowTrace"]) == 2


def test_templates_present_for_chirps_and_worldpop() -> None:
    chirps = chirps3_dhis2_template()
    worldpop = worldpop_dhis2_template()
    assert chirps.workflow_id == "chirps3-dhis2-template"
    assert worldpop.workflow_id == "worldpop-dhis2-template"
    assert len(chirps.nodes) >= 3
    assert len(worldpop.nodes) >= 2
