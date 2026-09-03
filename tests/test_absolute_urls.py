"""Absolute URLs must carry the configured public scheme (CLIM-974).

Behind a TLS-terminating proxy the request arrives over plain HTTP, so anything built from
`request.base_url` or `request.url` emits `http://` on an HTTPS deployment. In a browser that
is active mixed content: the fetches are blocked and the map viewer renders an empty
catalogue, while curl against every endpoint still returns 200. HSTS hides it entirely, so
the deployment where it was found looked fine.
"""

import pytest
from fastapi.testclient import TestClient

from open_climate_service.shared.urls import BASE_URL_ENV

_CONFIGURED = "https://ocs-demo-nepal.dhis2.org"


@pytest.fixture
def https_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """A client whose requests arrive over HTTP while the public origin is HTTPS.

    This is exactly the proxied deployment: TestClient's own base URL is `http://testserver`,
    standing in for what uvicorn sees behind the proxy.
    """
    from open_climate_service.main import app

    monkeypatch.setenv(BASE_URL_ENV, _CONFIGURED)
    return TestClient(app)


# -- the helper -----------------------------------------------------------------------------


def test_the_configured_origin_wins_over_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    from open_climate_service.shared.urls import absolute_base

    monkeypatch.setenv(BASE_URL_ENV, _CONFIGURED)
    request = _fake_request("http://internal:9000/", "/map")
    assert absolute_base(request) == _CONFIGURED


def test_the_request_is_the_fallback_when_nothing_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local development, served directly with no proxy and no configuration."""
    from open_climate_service.shared.urls import absolute_base

    monkeypatch.delenv(BASE_URL_ENV, raising=False)
    request = _fake_request("http://localhost:9000/", "/map")
    assert absolute_base(request) == "http://localhost:9000"


@pytest.mark.parametrize("configured", [f"{_CONFIGURED}/", f"  {_CONFIGURED}  ", f"{_CONFIGURED}///"])
def test_a_sloppy_base_url_is_normalised(monkeypatch: pytest.MonkeyPatch, configured: str) -> None:
    """A trailing slash or stray whitespace in the deployment config must not double up in
    every link on the page."""
    from open_climate_service.shared.urls import absolute_base, absolute_url

    monkeypatch.setenv(BASE_URL_ENV, configured)
    request = _fake_request("http://internal:9000/", "/map")
    assert absolute_base(request) == _CONFIGURED
    assert absolute_url(request, "/collections") == f"{_CONFIGURED}/collections"


def test_a_blank_base_url_is_not_a_configured_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty environment variable is how a compose file spells "unset"; treating it as an
    origin would emit links with no host at all."""
    from open_climate_service.shared.urls import absolute_base

    monkeypatch.setenv(BASE_URL_ENV, "   ")
    request = _fake_request("http://localhost:9000/", "/map")
    assert absolute_base(request) == "http://localhost:9000"


def test_the_self_url_keeps_the_requested_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """`/stac` and `/stac/catalog.json` are one document, and each must name itself."""
    from open_climate_service.shared.urls import self_url

    monkeypatch.setenv(BASE_URL_ENV, _CONFIGURED)
    assert self_url(_fake_request("http://internal:9000/", "/stac")) == f"{_CONFIGURED}/stac"
    assert self_url(_fake_request("http://internal:9000/", "/stac/catalog.json")) == f"{_CONFIGURED}/stac/catalog.json"


def test_the_self_url_drops_the_query_string(monkeypatch: pytest.MonkeyPatch) -> None:
    """No OCS document varies by query string, so reflecting client input into a served
    document buys nothing."""
    from open_climate_service.shared.urls import self_url

    monkeypatch.setenv(BASE_URL_ENV, _CONFIGURED)
    request = _fake_request("http://internal:9000/", "/stac", query="a=1&b=2")
    assert self_url(request) == f"{_CONFIGURED}/stac"


def _fake_request(base: str, path: str, query: str = ""):
    """A Request with just enough scope for the URL helpers."""
    from fastapi import Request

    host = base.split("://", 1)[1].rstrip("/")
    hostname, _, port = host.partition(":")
    return Request(
        {
            "type": "http",
            "scheme": base.split("://", 1)[0],
            "server": (hostname, int(port) if port else 80),
            "path": path,
            "query_string": query.encode(),
            "headers": [(b"host", host.encode())],
            "root_path": "",
        }
    )


# -- the two reported endpoints -------------------------------------------------------------


def test_the_map_viewer_never_emits_our_own_host_over_http(https_client: TestClient) -> None:
    """The reported defect. Every fetch target and the header link came from the request, so
    they carried `http` while the page itself was served over `https`."""
    body = https_client.get("/map").text

    assert "http://testserver" not in body
    # The specific URLs named in the report.
    for path in ("/extent", "/collections"):
        assert f'"{_CONFIGURED}{path}"' in body
    assert f'href="{_CONFIGURED}/"' in body


def test_the_stac_self_link_uses_the_configured_scheme(https_client: TestClient) -> None:
    """`self` was built from the raw request URL while its sibling links already used the
    configured base — so one link in the document disagreed with the rest."""
    links = {link["rel"]: link["href"] for link in https_client.get("/stac").json()["links"]}

    assert links["self"] == f"{_CONFIGURED}/stac"
    assert links["root"] == f"{_CONFIGURED}/stac/catalog.json"


def test_the_catalog_json_self_link_names_its_own_path(https_client: TestClient) -> None:
    links = {link["rel"]: link["href"] for link in https_client.get("/stac/catalog.json").json()["links"]}

    assert links["self"] == f"{_CONFIGURED}/stac/catalog.json"


def test_no_served_document_leaks_the_internal_origin(https_client: TestClient) -> None:
    """A sweep rather than a list, so a new absolute URL built from the request is caught here
    instead of on a deployment. Non-browser STAC clients do not implement HSTS, so an `http`
    link is followed as written."""
    for path in ("/", "/map", "/stac", "/stac/catalog.json", "/collections", "/processes"):
        response = https_client.get(path, headers={"Accept": "application/json"} if path == "/" else {})
        assert response.status_code == 200, path
        assert "http://testserver" not in response.text, path
