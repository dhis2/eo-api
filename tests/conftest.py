from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eo_api.main import app
from eo_api.publications import pygeoapi as publication_pygeoapi
from eo_api.publications import services as publication_services
from eo_api.workflows.services import datavalueset, job_store, publication_assets, run_logs


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def isolate_download_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep workflow/publication tests from writing into the repo download dir."""
    isolated_download_dir = tmp_path / "downloads"
    isolated_download_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", isolated_download_dir)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", isolated_download_dir)
    monkeypatch.setattr(datavalueset, "DOWNLOAD_DIR", isolated_download_dir)
    monkeypatch.setattr(publication_assets, "DOWNLOAD_DIR", isolated_download_dir)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", isolated_download_dir)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", isolated_download_dir)
