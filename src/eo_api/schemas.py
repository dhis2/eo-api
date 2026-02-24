"""Pydantic response models."""

from pydantic import BaseModel


class MessageResponse(BaseModel):
    """Generic message response."""

    message: str
