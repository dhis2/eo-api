"""Command-line entry point for running the Open Climate Service with uvicorn."""

import uvicorn


def main() -> None:
    """Start the Open Climate Service server."""
    uvicorn.run("open_climate_service.main:app", host="0.0.0.0", port=8000)
