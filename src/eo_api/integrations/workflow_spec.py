"""Workflow spec scaffold models for dynamic orchestration."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class WorkflowNodeSpec(BaseModel):
    """One node in a workflow DAG (scaffold: sequential execution)."""

    id: str = Field(..., description="Unique node id in workflow")
    component: str = Field(..., description="Registered component id")
    params: dict[str, Any] = Field(default_factory=dict, description="Node parameters (supports refs)")


class WorkflowSpec(BaseModel):
    """Workflow specification used by the scaffold executor."""

    workflow_id: str = Field(..., description="Workflow identifier")
    version: str = Field(default="0.1.0")
    nodes: list[WorkflowNodeSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_node_ids(self) -> "WorkflowSpec":
        node_ids = [node.id for node in self.nodes]
        duplicates = {node_id for node_id in node_ids if node_ids.count(node_id) > 1}
        if duplicates:
            raise ValueError(f"Duplicate workflow node ids: {sorted(duplicates)}")
        return self
