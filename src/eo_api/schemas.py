"""Pydantic response models."""

from pydantic import BaseModel


class StatusMessage(BaseModel):
    """Simple status message response."""

    message: str
