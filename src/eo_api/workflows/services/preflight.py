"""Preflight checks for external data source connectivity."""

from __future__ import annotations

import socket
from urllib.parse import urlparse


def check_upstream_connectivity(dataset: dict[str, object], timeout_seconds: float = 5.0) -> None:
    """Fail fast if a dataset source host is not reachable."""
    source_url = dataset.get("source_url")
    if not isinstance(source_url, str) or not source_url:
        return

    parsed = urlparse(source_url)
    hostname = parsed.hostname
    if not hostname:
        return
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    # Fail quickly on DNS/TCP connectivity issues instead of waiting for long GDAL timeouts.
    with socket.create_connection((hostname, port), timeout=timeout_seconds):
        pass
