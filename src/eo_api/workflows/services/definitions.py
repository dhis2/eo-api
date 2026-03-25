"""Declarative workflow definition loading and validation."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import AliasChoices, BaseModel, Field, model_validator

from ...publications.capabilities import evaluate_publication_serving
from ...publications.schemas import PublishedResourceExposure, PublishedResourceKind

SCRIPT_DIR = Path(__file__).parent.resolve()
WORKFLOWS_DIR = SCRIPT_DIR.parent.parent.parent.parent / "data" / "workflows"
DEFAULT_WORKFLOW_ID = "dhis2_datavalue_set_v1"


class WorkflowStep(BaseModel):
    """One component step in a declarative workflow definition."""

    id: str | None = None
    component: str
    version: str = "v1"
    config: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, "WorkflowStepInput"] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_component_version(self) -> "WorkflowStep":
        """Ensure component@version exists in the registered component catalog."""
        supported_versions = _supported_component_versions(self.component)
        if self.version not in supported_versions:
            known = ", ".join(sorted(supported_versions)) or "<none>"
            raise ValueError(
                f"Unsupported component version '{self.component}@{self.version}'. Supported versions: {known}"
            )
        return self


class WorkflowStepInput(BaseModel):
    """Reference one named output from a prior workflow step."""

    from_step: str
    output: str = Field(validation_alias=AliasChoices("output", "output_key"))


class WorkflowOutputBinding(BaseModel):
    """Expose one named workflow output from a step output."""

    from_step: str
    output: str = Field(validation_alias=AliasChoices("output", "output_key"))
    include_in_response: bool = True


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
    asset: WorkflowStepInput | None = None
    asset_format: str | None = None
    inputs: dict[str, WorkflowStepInput] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_publication_policy(self) -> "WorkflowPublicationPolicy":
        """Normalize workflow publication policy."""
        normalized_suffixes = []
        for suffix in self.required_output_file_suffixes:
            normalized_suffixes.append(suffix if suffix.startswith(".") else f".{suffix}")
        self.required_output_file_suffixes = normalized_suffixes
        if self.asset_format is not None:
            self.asset_format = self.asset_format.strip().lower() or None
        return self


class WorkflowDefinition(BaseModel):
    """Declarative workflow definition."""

    workflow_id: str
    version: int = 1
    publication: WorkflowPublicationPolicy = Field(default_factory=WorkflowPublicationPolicy)
    steps: list[WorkflowStep]
    outputs: dict[str, WorkflowOutputBinding] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_steps(self) -> "WorkflowDefinition":
        """Validate component compatibility and exported workflow outputs."""
        if not self.steps:
            raise ValueError("Workflow steps cannot be empty")
        _assign_step_ids(self.steps)
        available_outputs: dict[str, set[str]] = {}
        latest_producer_for_output: dict[str, str] = {}
        for step in self.steps:
            if step.id is None:
                raise ValueError(f"Workflow step '{step.component}' is missing an id")

            resolved_inputs = _normalize_step_inputs(
                step=step,
                available_outputs=available_outputs,
                latest_producer_for_output=latest_producer_for_output,
            )
            step.inputs = resolved_inputs

            outputs = _component_outputs(step.component, step.version)
            available_outputs[step.id] = outputs
            for output_name in outputs:
                latest_producer_for_output[output_name] = step.id

        if not self.outputs:
            raise ValueError("Workflow must declare at least one exported output")

        _validate_workflow_outputs(bindings=self.outputs, available_outputs=available_outputs, owner="Workflow outputs")
        if self.publication.publishable:
            if self.publication.asset is None and not self.publication.inputs:
                raise ValueError("Publishable workflows must declare a publication asset or publication inputs")
            if self.publication.asset is not None:
                _validate_workflow_outputs(
                    bindings={"asset": self.publication.asset},
                    available_outputs=available_outputs,
                    owner="Workflow publication asset",
                )
            if self.publication.inputs:
                _validate_workflow_outputs(
                    bindings=self.publication.inputs,
                    available_outputs=available_outputs,
                    owner="Workflow publication",
                )
            capability = evaluate_publication_serving(
                kind=self.publication.intent,
                exposure=self.publication.exposure,
                asset_format=self.publication.asset_format,
            )
            if not capability.supported:
                raise ValueError(capability.error or "Unsupported publication serving contract")
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


def _assign_step_ids(steps: list[WorkflowStep]) -> None:
    seen_ids: set[str] = set()
    component_counts: dict[str, int] = {}
    for step in steps:
        if step.id is None:
            count = component_counts.get(step.component, 0) + 1
            component_counts[step.component] = count
            step.id = step.component if count == 1 else f"{step.component}_{count}"
        if step.id in seen_ids:
            raise ValueError(f"Duplicate workflow step id '{step.id}'")
        seen_ids.add(step.id)


def _normalize_step_inputs(
    *,
    step: WorkflowStep,
    available_outputs: dict[str, set[str]],
    latest_producer_for_output: dict[str, str],
) -> dict[str, WorkflowStepInput]:
    declared_inputs = dict(step.inputs)
    required_inputs = _component_required_inputs(step.component, step.version)
    optional_inputs = _component_optional_inputs(step.component, step.version)

    if not declared_inputs:
        for input_name in sorted(required_inputs | optional_inputs):
            producer = latest_producer_for_output.get(input_name)
            if producer is None:
                continue
            declared_inputs[input_name] = WorkflowStepInput(from_step=producer, output=input_name)

    missing_required = required_inputs - set(declared_inputs)
    if missing_required:
        missing = ", ".join(sorted(missing_required))
        raise ValueError(f"Component '{step.component}' is missing required upstream outputs: {missing}")

    allowed_inputs = required_inputs | optional_inputs
    unexpected_inputs = set(declared_inputs) - allowed_inputs
    if unexpected_inputs:
        unexpected = ", ".join(sorted(unexpected_inputs))
        raise ValueError(f"Component '{step.component}' declares unsupported inputs: {unexpected}")

    for input_name, ref in declared_inputs.items():
        available_for_step = available_outputs.get(ref.from_step)
        if available_for_step is None:
            raise ValueError(
                f"Component '{step.component}' references unknown upstream "
                f"step '{ref.from_step}' for input '{input_name}'"
            )
        if ref.output not in available_for_step:
            raise ValueError(
                f"Component '{step.component}' input '{input_name}' references "
                f"missing output '{ref.output}' from step '{ref.from_step}'"
            )

    return declared_inputs


def _validate_workflow_outputs(
    *,
    bindings: Mapping[str, WorkflowStepInput | WorkflowOutputBinding],
    available_outputs: dict[str, set[str]],
    owner: str,
) -> None:
    if not bindings:
        raise ValueError(f"{owner} cannot be empty")
    for output_name, ref in bindings.items():
        available_for_step = available_outputs.get(ref.from_step)
        if available_for_step is None:
            raise ValueError(f"{owner} reference '{output_name}' points to unknown step '{ref.from_step}'")
        if ref.output not in available_for_step:
            raise ValueError(
                f"{owner} reference '{output_name}' points to missing output '{ref.output}' from step '{ref.from_step}'"
            )


def _component_definition(component: str, version: str) -> tuple[set[str], set[str], set[str]]:
    from ...components import services as component_services

    definition = component_services.component_registry().get(f"{component}@{version}")
    if definition is None:
        raise ValueError(f"Unsupported component version '{component}@{version}'. Supported versions: <none>")
    return (
        set(definition.workflow_inputs_required),
        set(definition.workflow_inputs_optional),
        set(definition.outputs),
    )


def _supported_component_versions(component: str) -> set[str]:
    from ...components import services as component_services

    versions: set[str] = set()
    for key in component_services.component_registry():
        name, _, version = key.partition("@")
        if name == component and version:
            versions.add(version)
    return versions


def _component_required_inputs(component: str, version: str) -> set[str]:
    required, _, _ = _component_definition(component, version)
    return required


def _component_optional_inputs(component: str, version: str) -> set[str]:
    _, optional, _ = _component_definition(component, version)
    return optional


def _component_outputs(component: str, version: str) -> set[str]:
    _, _, outputs = _component_definition(component, version)
    return outputs
