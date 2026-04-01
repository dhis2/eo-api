import pytest

from eo_api.publications import services


def test_native_dataset_href_defaults_to_relative_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EO_API_BASE_URL", raising=False)
    monkeypatch.delenv("OGCAPI_BASE_URL", raising=False)

    assert services._native_dataset_href("dataset-1") == "/datasets/dataset-1"


def test_native_dataset_href_uses_ogcapi_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EO_API_BASE_URL", raising=False)
    monkeypatch.setenv("OGCAPI_BASE_URL", "https://example.org/ogcapi")

    assert services._native_dataset_href("dataset-1") == "https://example.org/datasets/dataset-1"
