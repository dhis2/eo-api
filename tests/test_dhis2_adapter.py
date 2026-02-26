from typing import Any

import pytest

from eo_api.integrations import dhis2_adapter


def test_normalized_base_url_strips_api_suffix() -> None:
    assert dhis2_adapter._normalized_base_url("https://example.org/api") == "https://example.org"
    assert dhis2_adapter._normalized_base_url("https://example.org/api/") == "https://example.org"
    assert dhis2_adapter._normalized_base_url("https://example.org") == "https://example.org"


def test_create_client_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DHIS2_BASE_URL", raising=False)
    monkeypatch.delenv("DHIS2_USERNAME", raising=False)
    monkeypatch.delenv("DHIS2_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="DHIS2_BASE_URL, DHIS2_USERNAME and DHIS2_PASSWORD must be set"):
        dhis2_adapter.create_client()


def test_create_client_uses_defaults_and_normalized_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class FakeClient:
        def __init__(self, **kwargs: Any):
            captured.update(kwargs)

    monkeypatch.setenv("DHIS2_BASE_URL", "https://play.example.org/api")
    monkeypatch.setenv("DHIS2_USERNAME", "alice")
    monkeypatch.setenv("DHIS2_PASSWORD", "secret")
    monkeypatch.setattr(dhis2_adapter, "DHIS2Client", FakeClient)

    dhis2_adapter.create_client()

    assert captured["base_url"] == "https://play.example.org"
    assert captured["username"] == "alice"
    assert captured["password"] == "secret"
    assert captured["timeout"] == dhis2_adapter.DEFAULT_DHIS2_TIMEOUT_SECONDS
    assert captured["retries"] == dhis2_adapter.DEFAULT_DHIS2_RETRIES


def test_create_client_allows_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class FakeClient:
        def __init__(self, **kwargs: Any):
            captured.update(kwargs)

    monkeypatch.setenv("DHIS2_BASE_URL", "https://play.example.org")
    monkeypatch.setenv("DHIS2_USERNAME", "alice")
    monkeypatch.setenv("DHIS2_PASSWORD", "secret")
    monkeypatch.setattr(dhis2_adapter, "DHIS2Client", FakeClient)

    dhis2_adapter.create_client(timeout_seconds=90, retries=7)

    assert captured["timeout"] == 90
    assert captured["retries"] == 7
