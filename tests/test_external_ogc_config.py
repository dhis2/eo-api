from urllib.error import URLError

from eoapi.external_ogc import _fetch_json, load_external_providers


class _Response:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_load_external_provider_auth_and_retry_settings(monkeypatch) -> None:
    monkeypatch.setenv(
        "EOAPI_EXTERNAL_OGC_SERVICES",
        '[{"id":"demo","url":"https://example.test","headers":{"X-Custom":"abc"},"apiKeyEnv":"DEMO_API_KEY","authScheme":"Token","timeoutSeconds":7,"retries":2,"operations":["coverage","position"]}]',
    )

    providers = load_external_providers()

    assert len(providers) == 1
    provider = providers[0]
    assert provider.id == "demo"
    assert provider.headers == {"X-Custom": "abc"}
    assert provider.api_key_env == "DEMO_API_KEY"
    assert provider.auth_scheme == "Token"
    assert provider.timeout_seconds == 7.0
    assert provider.retries == 2
    assert provider.operations == ("coverage", "position")


def test_fetch_json_applies_headers_timeout_and_retries(monkeypatch) -> None:
    monkeypatch.setenv("DEMO_API_KEY", "secret-value")

    monkeypatch.setenv(
        "EOAPI_EXTERNAL_OGC_SERVICES",
        '[{"id":"demo","url":"https://example.test","headers":{"X-Custom":"abc"},"apiKeyEnv":"DEMO_API_KEY","authScheme":"Token","timeoutSeconds":3,"retries":1}]',
    )
    provider = load_external_providers()[0]

    calls: list[tuple[object, float]] = []

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        if len(calls) == 1:
            raise URLError("temporary")
        return _Response(b'{"collections": []}')

    monkeypatch.setattr("eoapi.external_ogc.urlopen", fake_urlopen)

    payload = _fetch_json("https://example.test/collections", provider=provider)

    assert payload == {"collections": []}
    assert len(calls) == 2
    request, timeout = calls[0]
    assert timeout == 3.0
    assert request.headers["X-custom"] == "abc"
    assert request.headers["Authorization"] == "Token secret-value"


def test_load_external_provider_rejects_unknown_operations(monkeypatch) -> None:
    monkeypatch.setenv(
        "EOAPI_EXTERNAL_OGC_SERVICES",
        '[{"id":"demo","url":"https://example.test","operations":["coverage","timeseries"]}]',
    )

    providers = load_external_providers()

    assert providers == []
