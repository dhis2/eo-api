"""Declarative workflow definition loading and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Final, Literal

import yaml
from pydantic import BaseModel, Field, model_validator


SCRIPT_DIR = Path(__file__).parent.resolve()
WORKFLOWS_DIR = SCRIPT_DIR.parent.parent.parent.parent / "data" / "workflows"
DEFAULT_WORKFLOW_ID = "dhis2_datavalue_set_v1"


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
