"""Declarative workflow definition loading and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Final, Literal

import yaml
from pydantic import AliasChoices, BaseModel, Field, model_validator

from ...publications.schemas import PublishedResourceExposure, PublishedResourceKind

ComponentName = Literal[
    "feature_source",
    "download_dataset",
    "temporal_aggregation",
    "spatial_aggregation",
    "build_datavalueset",
]

SUPPORTED_COMPONENTS: Final[set[str]] = set(ComponentName.__args__)  # type: ignore[attr-defined]
SUPPORTED_COMPONENT_VERSIONS: Final[dict[str, set[str]]] = {component: {"v1"} for component in SUPPORTED_COMPONENTS}

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
    version: str = "v1"
    config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_component_version(self) -> "WorkflowStep":
        """Ensure component@version exists in the registered component catalog."""
        supported_versions = SUPPORTED_COMPONENT_VERSIONS.get(self.component, set())
        if self.version not in supported_versions:
            known = ", ".join(sorted(supported_versions)) or "<none>"
            raise ValueError(
                f"Unsupported component version '{self.component}@{self.version}'. Supported versions: {known}"
            )
        return self


class WorkflowPublicationPolicy(BaseModel):
    """Publication policy for workflow outputs."""

    publishable: bool = Field(default=False, validation_alias=AliasChoices("publishable", "enabled"))
    strategy: Literal["on_success", "manual"] = Field(
        default="on_success",
        validation_alias=AliasChoices("strategy", "publish_strategy"),
    )
    intent: PublishedResourceKind = Field(
        default=PublishedResourceKind.FEATURE_COLLECTION,
        validation_alias=AliasChoices("intent", "resource_kind"),
    )
    exposure: PublishedResourceExposure = PublishedResourceExposure.REGISTRY_ONLY
    required_output_file_suffixes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_publication_policy(self) -> "WorkflowPublicationPolicy":
        """Restrict workflow-driven publication to currently supported resource types."""
        if self.publishable and self.intent != PublishedResourceKind.FEATURE_COLLECTION:
            raise ValueError("Workflow publication currently supports only intent='feature_collection'")
        normalized_suffixes = []
        for suffix in self.required_output_file_suffixes:
            normalized_suffixes.append(suffix if suffix.startswith(".") else f".{suffix}")
        self.required_output_file_suffixes = normalized_suffixes
        return self


class WorkflowDefinition(BaseModel):
    """Declarative workflow definition."""

    workflow_id: str
    version: int = 1
    publication: WorkflowPublicationPolicy = Field(default_factory=WorkflowPublicationPolicy)
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
