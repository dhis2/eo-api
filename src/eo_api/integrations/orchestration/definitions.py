"""YAML workflow-definition loader for scaffolded workflow specs."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import yaml
from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.orchestration.spec import WorkflowSpec

WORKFLOW_DEFINITION_DIR = Path(__file__).resolve().parents[1] / "workflow_definitions"
WORKFLOW_TEMPLATE_DIR = WORKFLOW_DEFINITION_DIR / "workflows"


def _definition_path(workflow_id: str) -> Path:
    return WORKFLOW_TEMPLATE_DIR / f"{workflow_id}.yaml"


@lru_cache(maxsize=32)
def load_workflow_definition(workflow_id: str) -> WorkflowSpec:
    """Load a workflow definition YAML file and validate as WorkflowSpec."""
    path = _definition_path(workflow_id)
    if not path.exists():
        raise ProcessorExecuteError(f"Workflow definition not found: {path}")
    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ProcessorExecuteError(f"Workflow definition must be a YAML object: {path}")
    try:
        spec = WorkflowSpec.model_validate(cast(dict[str, Any], data))
    except Exception as exc:
        raise ProcessorExecuteError(f"Invalid workflow definition '{workflow_id}': {exc}") from exc
    if spec.workflow_id != workflow_id:
        raise ProcessorExecuteError(
            f"Workflow definition id mismatch: expected '{workflow_id}', got '{spec.workflow_id}'"
        )
    return spec


def available_workflow_definitions() -> list[str]:
    """List available workflow template IDs from YAML files."""
    if not WORKFLOW_TEMPLATE_DIR.exists():
        return []
    return sorted(path.stem for path in WORKFLOW_TEMPLATE_DIR.glob("*-template.yaml"))
