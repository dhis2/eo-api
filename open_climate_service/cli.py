"""Command-line entry point for running the Open Climate Service with uvicorn."""

import os

import uvicorn

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9000


def main() -> None:
    """Start the Open Climate Service server.

    Host and port are read from the HOST and PORT environment variables,
    defaulting to 0.0.0.0:9000.
    """
    host = os.environ.get("HOST", DEFAULT_HOST)
    port = int(os.environ.get("PORT", DEFAULT_PORT))
    uvicorn.run("open_climate_service.main:app", host=host, port=port)
