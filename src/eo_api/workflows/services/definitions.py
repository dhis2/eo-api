"""Declarative workflow definition loading and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Final, Literal

import yaml
from pydantic import BaseModel, model_validator

ComponentName = Literal[
    "feature_source",
    "download_dataset",
    "temporal_aggregation",
    "spatial_aggregation",
    "build_datavalueset",
]

SUPPORTED_COMPONENTS: Final[set[str]] = set(ComponentName.__args__)  # type: ignore[attr-defined]

COMPONENT_INPUTS: Final[dict[str, set[str]]] = {
    "feature_source": set(),
    "download_dataset": {"bbox"},
    "temporal_aggregation": {"bbox"},
    "spatial_aggregation": {"bbox", "features"},
    "build_datavalueset": {"records"},
}

COMPONENT_OUTPUTS: Final[dict[str, set[str]]] = {
    "feature_source": {"features", "bbox"},
    "download_dataset": set(),
    "temporal_aggregation": {"temporal_dataset"},
    "spatial_aggregation": {"records"},
    "build_datavalueset": {"data_value_set", "output_file"},
}

SCRIPT_DIR = Path(__file__).parent.resolve()
WORKFLOWS_DIR = SCRIPT_DIR.parent.parent.parent.parent / "data" / "workflows"
DEFAULT_WORKFLOW_ID = "dhis2_datavalue_set_v1"


class WorkflowStep(BaseModel):
    """One component step in a declarative workflow definition."""

    component: ComponentName


class WorkflowDefinition(BaseModel):
    """Declarative workflow definition."""

    workflow_id: str
    version: int = 1
    steps: list[WorkflowStep]

    @model_validator(mode="after")
    def validate_steps(self) -> "WorkflowDefinition":
        """Require terminal DataValueSet step and validate component compatibility."""
        if not self.steps:
            raise ValueError("Workflow steps cannot be empty")
        if self.steps[-1].component != "build_datavalueset":
            raise ValueError("The last workflow step must be 'build_datavalueset'")
        available_context: set[str] = set()
        for step in self.steps:
            required_inputs = COMPONENT_INPUTS[step.component]
            missing_inputs = required_inputs - available_context
            if missing_inputs:
                missing = ", ".join(sorted(missing_inputs))
                raise ValueError(f"Component '{step.component}' is missing required upstream outputs: {missing}")
            available_context.update(COMPONENT_OUTPUTS[step.component])
        return self


def load_workflow_definition(
    workflow_id: str = DEFAULT_WORKFLOW_ID,
    *,
    path: Path | None = None,
) -> WorkflowDefinition:
    """Load and validate workflow definition from discovered YAML files."""
    if path is not None:
        workflow_file = path
    else:
        workflow_files = _discover_workflow_files()
        workflow_file_or_none = workflow_files.get(workflow_id)
        if workflow_file_or_none is None:
            known = ", ".join(sorted(workflow_files))
            raise ValueError(f"Unknown workflow_id '{workflow_id}'. Allowed values: {known}")
        workflow_file = workflow_file_or_none

    if not workflow_file.exists():
        raise ValueError(f"Workflow definition file not found: {workflow_file}")
    with open(workflow_file, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        raise ValueError(f"Workflow definition file is empty: {workflow_file}")
    definition = WorkflowDefinition.model_validate(raw)
    if path is None and definition.workflow_id != workflow_id:
        raise ValueError(
            f"workflow_id mismatch: requested '{workflow_id}' but definition declares '{definition.workflow_id}'"
        )
    return definition


def list_workflow_definitions() -> list[WorkflowDefinition]:
    """Load and return all discovered workflow definitions."""
    workflow_files = _discover_workflow_files()
    return [load_workflow_definition(workflow_id) for workflow_id in sorted(workflow_files)]


def _discover_workflow_files() -> dict[str, Path]:
    """Discover and validate workflow IDs from all YAML files in workflows folder."""
    if not WORKFLOWS_DIR.is_dir():
        raise ValueError(f"Workflow directory not found: {WORKFLOWS_DIR}")

    discovered: dict[str, Path] = {}
    for workflow_file in sorted(WORKFLOWS_DIR.glob("*.y*ml")):
        with open(workflow_file, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        if raw is None:
            raise ValueError(f"Workflow definition file is empty: {workflow_file}")
        if not isinstance(raw, dict):
            raise ValueError(f"Workflow definition must be a mapping/object: {workflow_file}")

        workflow_id = raw.get("workflow_id")
        if not isinstance(workflow_id, str) or not workflow_id:
            raise ValueError(f"Missing/invalid workflow_id in: {workflow_file}")

        existing = discovered.get(workflow_id)
        if existing is not None:
            raise ValueError(f"Duplicate workflow_id '{workflow_id}' in files: {existing.name}, {workflow_file.name}")
        discovered[workflow_id] = workflow_file

    return discovered
