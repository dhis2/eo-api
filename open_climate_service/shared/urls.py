"""The public origin for absolute URLs OCS puts into documents it serves.

Behind a TLS-terminating reverse proxy, the request uvicorn sees is plain HTTP: the proxy
speaks HTTPS to the client and forwards over HTTP. So `request.base_url` reports the `http`
scheme even though every client reached the service over `https`, and any absolute URL built
from the request carries a scheme that is wrong for the outside world (CLIM-974).

`CLIMATE_SERVICE_BASE_URL` is the operator's statement of the public origin, so it wins.
The request is the fallback for a direct deployment where nothing is configured, which is
the local-development case.

The failure this prevents is easy to misread. An HTTPS page fetching `http://` URLs is
active mixed content, so the browser blocks the requests and the map viewer renders an empty
catalogue — while curl against every endpoint still returns 200, because the endpoints were
never the problem. HSTS masks it completely: the browser rewrites the scheme before the
request leaves, so a deployment with HSTS at the edge looks correct and only clients that
have not yet seen the HSTS header (and non-browser STAC clients, which do not implement it
at all) see the defect.

Use these helpers rather than `request.base_url` or `request.url` for anything that leaves
the process. Reading the environment variable at each call site is the same one-line
expression repeated, and the sites that forgot it are exactly the bug.
"""

import os

from fastapi import Request

BASE_URL_ENV = "CLIMATE_SERVICE_BASE_URL"


def absolute_base(request: Request) -> str:
    """The configured public origin, or the request's own, without a trailing slash."""
    configured = os.getenv(BASE_URL_ENV, "").strip()
    if configured:
        return configured.rstrip("/")
    return str(request.base_url).rstrip("/")


def absolute_url(request: Request, path: str) -> str:
    """`path` resolved against the public origin."""
    base = absolute_base(request)
    if not path:
        return base
    return f"{base}/{path.lstrip('/')}"


def self_url(request: Request) -> str:
    """The public URL of the document being requested.

    The path comes from the request — `/stac` and `/stac/catalog.json` are the same document
    and each must name itself — while the origin comes from the configuration. The query
    string is deliberately dropped: no OCS document varies by it, and reflecting arbitrary
    client input back into a served document is not worth the trouble it invites.
    """
    return absolute_url(request, request.url.path)
