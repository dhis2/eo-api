"""Thin OGC API adapter routes over the native workflow engine."""

from __future__ import annotations

import uuid
from html import escape
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

from ..publications.schemas import PublishedResourceExposure
from ..publications.services import collection_id_for_resource, get_published_resource
from ..shared.api_errors import api_error
from ..workflows.schemas import WorkflowExecuteEnvelopeRequest, WorkflowJobStatus
from ..workflows.services.definitions import load_workflow_definition
from ..workflows.services.engine import execute_workflow
from ..workflows.services.job_store import get_job, get_job_result, initialize_job, list_jobs
from ..workflows.services.simple_mapper import normalize_simple_request

router = APIRouter()

_PROCESS_ID = "generic-dhis2-workflow"
_PROCESS_TITLE = "Generic DHIS2 workflow"


@router.get("", response_model=None)
def get_ogc_root(request: Request, f: str | None = None) -> dict[str, Any] | HTMLResponse:
    """Return a native OGC landing page for processes and jobs."""
    base_url = str(request.base_url).rstrip("/")
    body = {
        "title": "DHIS2 EO API",
        "description": (
            "Native OGC API landing page for workflow processes and jobs. "
            "Collections and items are served by the mounted geospatial publication layer."
        ),
        "links": [
            {"rel": "self", "type": "application/json", "href": _request_href(request, f="json")},
            {"rel": "alternate", "type": "text/html", "href": _request_href(request, f="html")},
            {"rel": "service-desc", "type": "application/vnd.oai.openapi+json;version=3.0", "href": "/ogcapi/openapi"},
            {"rel": "conformance", "type": "application/json", "href": f"{base_url}/pygeoapi/conformance"},
            {"rel": "data", "type": "application/json", "href": f"{base_url}/pygeoapi/collections"},
            {"rel": "processes", "type": "application/json", "href": f"{base_url}/ogcapi/processes"},
            {"rel": "jobs", "type": "application/json", "href": f"{base_url}/ogcapi/jobs"},
        ],
        "navigation": [
            {
                "title": "Browse Collections",
                "description": "Open the OGC publication surface for collections and items.",
                "href": f"{base_url}/pygeoapi/collections?f=html",
            },
            {
                "title": "List Processes",
                "description": "View the exposed OGC process catalog backed by the native workflow engine.",
                "href": f"{base_url}/ogcapi/processes",
            },
            {
                "title": "List Jobs",
                "description": "Inspect OGC job records backed by the native job store.",
                "href": f"{base_url}/ogcapi/jobs",
            },
            {
                "title": "Conformance",
                "description": "See the standards conformance declarations for the mounted OGC publication layer.",
                "href": f"{base_url}/ogcapi/conformance",
            },
        ],
    }
    if _wants_html(request, f):
        return HTMLResponse(_render_ogc_root_html(body))
    return body


@router.get("/processes")
def list_processes(request: Request) -> dict[str, Any]:
    """List exposed OGC processes."""
    return {
        "processes": [
            {
                "id": _PROCESS_ID,
                "title": _PROCESS_TITLE,
                "description": "Execute the generic DHIS2 EO workflow and persist a native job record.",
                "jobControlOptions": ["sync-execute", "async-execute"],
                "outputTransmission": ["value", "reference"],
                "links": [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": str(request.url_for("describe_ogc_process", process_id=_PROCESS_ID)),
                    }
                ],
            }
        ]
    }


@router.get("/processes/{process_id}", name="describe_ogc_process")
def describe_process(process_id: str, request: Request) -> dict[str, Any]:
    """Describe the single exposed generic workflow process."""
    _require_process(process_id)
    return {
        "id": _PROCESS_ID,
        "title": _PROCESS_TITLE,
        "description": "OGC-facing adapter over the reusable native workflow engine.",
        "jobControlOptions": ["sync-execute", "async-execute"],
        "outputTransmission": ["value", "reference"],
        "links": [
            {
                "rel": "execute",
                "type": "application/json",
                "href": str(request.url_for("execute_ogc_process", process_id=_PROCESS_ID)),
            }
        ],
    }


@router.post("/processes/{process_id}/execution", name="execute_ogc_process")
def execute_process(
    process_id: str,
    payload: WorkflowExecuteEnvelopeRequest,
    request: Request,
    response: Response,
    background_tasks: BackgroundTasks,
    prefer: str | None = Header(default=None),
) -> dict[str, Any]:
    """Execute the generic workflow synchronously or submit it asynchronously."""
    _require_process(process_id)
    normalized, _warnings = normalize_simple_request(payload.request)

    if prefer is not None and "respond-async" in prefer.lower():
        job_id = str(uuid.uuid4())
        workflow = load_workflow_definition(payload.request.workflow_id)
        initialize_job(
            job_id=job_id,
            request=normalized,
            request_payload=payload.request.model_dump(exclude_none=True),
            workflow=workflow,
            workflow_definition_source="catalog",
            workflow_id=payload.request.workflow_id,
            workflow_version=workflow.version,
            status=WorkflowJobStatus.ACCEPTED,
            process_id=_PROCESS_ID,
        )
        background_tasks.add_task(
            _run_async_workflow_job,
            job_id,
            normalized,
            payload.request.workflow_id,
            payload.request.model_dump(exclude_none=True),
            payload.request.include_component_run_details,
        )
        job_url = str(request.url_for("get_ogc_job", job_id=job_id))
        results_url = str(request.url_for("get_ogc_job_results", job_id=job_id))
        response.status_code = 202
        response.headers["Location"] = job_url
        return {
            "jobID": job_id,
            "status": WorkflowJobStatus.ACCEPTED,
            "location": job_url,
            "jobUrl": job_url,
            "resultsUrl": results_url,
        }

    result = execute_workflow(
        normalized,
        workflow_id=payload.request.workflow_id,
        request_params=payload.request.model_dump(exclude_none=True),
        include_component_run_details=payload.request.include_component_run_details,
        workflow_definition_source="catalog",
    )
    job_url = str(request.url_for("get_ogc_job", job_id=result.run_id))
    results_url = str(request.url_for("get_ogc_job_results", job_id=result.run_id))
    publication = get_published_resource(f"workflow-output-{result.run_id}")
    links: list[dict[str, Any]] = [
        {"rel": "monitor", "type": "application/json", "href": job_url},
        {"rel": "results", "type": "application/json", "href": results_url},
    ]
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id_for_resource(publication)),
            }
        )
    return {
        "jobID": result.run_id,
        "processID": _PROCESS_ID,
        "status": WorkflowJobStatus.SUCCESSFUL,
        "outputs": result.model_dump(mode="json"),
        "links": links,
    }


@router.get("/jobs")
def list_ogc_jobs(process_id: str | None = None) -> dict[str, Any]:
    """List OGC-visible jobs backed by the native job store."""
    jobs = list_jobs(process_id=process_id, status=None)
    return {"jobs": [job.model_dump(mode="json") for job in jobs]}


@router.get("/jobs/{job_id}", name="get_ogc_job")
def get_ogc_job(job_id: str, request: Request) -> dict[str, Any]:
    """Fetch one OGC job view from the native job store."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    publication = get_published_resource(f"workflow-output-{job.job_id}")
    links: list[dict[str, Any]] = [
        {
            "rel": "self",
            "type": "application/json",
            "href": str(request.url_for("get_ogc_job", job_id=job.job_id)),
        },
        {
            "rel": "results",
            "type": "application/json",
            "href": str(request.url_for("get_ogc_job_results", job_id=job.job_id)),
        },
    ]
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id_for_resource(publication)),
            }
        )
    return {
        "jobID": job.job_id,
        "processID": job.process_id,
        "status": job.status,
        "created": job.created_at,
        "updated": job.updated_at,
        "links": links,
    }


@router.get("/jobs/{job_id}/results", name="get_ogc_job_results")
def get_ogc_job_results(job_id: str) -> dict[str, Any]:
    """Return persisted results for a completed OGC job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    result = get_job_result(job_id)
    if result is None:
        raise HTTPException(
            status_code=409,
            detail=api_error(
                error="job_result_unavailable",
                error_code="JOB_RESULT_UNAVAILABLE",
                message=f"Result is not available for job '{job_id}'",
                job_id=job_id,
                status=str(job.status),
            ),
        )
    return result


def _require_process(process_id: str) -> None:
    if process_id != _PROCESS_ID:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="process_not_found",
                error_code="PROCESS_NOT_FOUND",
                message=f"Unknown process_id '{process_id}'",
                process_id=process_id,
            ),
        )


def _run_async_workflow_job(
    job_id: str,
    normalized_request: Any,
    workflow_id: str,
    request_params: dict[str, Any],
    include_component_run_details: bool,
) -> None:
    try:
        execute_workflow(
            normalized_request,
            workflow_id=workflow_id,
            request_params=request_params,
            include_component_run_details=include_component_run_details,
            run_id=job_id,
        )
    except HTTPException:
        return


def _collection_href(request: Request, collection_id: str) -> str:
    return str(request.base_url).rstrip("/") + f"/pygeoapi/collections/{collection_id}"


def _request_href(request: Request, **updates: Any) -> str:
    params = dict(request.query_params)
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = str(value)
    query = "&".join(f"{key}={value}" for key, value in params.items())
    suffix = f"?{query}" if query else ""
    return f"{request.url.path}{suffix}"


def _wants_html(request: Request, f: str | None) -> bool:
    if f is not None:
        return f.lower() == "html"
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept


def _render_ogc_root_html(body: dict[str, Any]) -> str:
    # Map icon SVGs to navigation items by title  # noqa: E501
    icons_map = {  # noqa: E501
        "Browse Collections": (  # noqa: E501
            '<svg class="card-icon" viewBox="0 0 24 24" fill="none" '
            'stroke="currentColor" stroke-width="1.5"><circle cx="10" cy="12" '
            'r="8"></circle><path d="M21 21l-4.35-4.35"></path></svg>'
        ),
        "List Processes": (  # noqa: E501
            '<svg class="card-icon" viewBox="0 0 24 24" fill="none" '
            'stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" '
            'r="1"></circle><circle cx="19" cy="12" r="1"></circle><circle '
            'cx="5" cy="12" r="1"></circle><path d="M12 2v20M2 12h20">'
            "</path></svg>"
        ),
        "List Jobs": (  # noqa: E501
            '<svg class="card-icon" viewBox="0 0 24 24" fill="none" '
            'stroke="currentColor" stroke-width="1.5"><path d="M9 5H7a2 2 0 '
            "00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 "
            '2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"></path>'
            '<path d="M12 12v6M12 12h3M12 12H9"></path></svg>'
        ),
        "Conformance": (  # noqa: E501
            '<svg class="card-icon" viewBox="0 0 24 24" fill="none" '
            'stroke="currentColor" stroke-width="1.5"><path d="M9 12l2 2 '
            '4-4"></path><path d="M21 12a9 9 0 11-18 0 9 9 0 0118 '
            '0z"></path></svg>'
        ),
    }

    nav_cards = "".join(
        (
            '<a class="nav-card" href="{href}">'
            '<div class="card-icon-wrapper">{icon}</div>'
            '<strong class="card-title">{title}</strong>'
            '<span class="card-desc">{description}</span>'
            '<div class="card-arrow">→</div>'
            "</a>"
        ).format(
            href=escape(item["href"]),
            title=escape(item["title"]),
            description=escape(item["description"]),
            icon=icons_map.get(
                item["title"],
                (  # noqa: E501
                    '<svg class="card-icon" viewBox="0 0 24 24" '
                    'fill="none" stroke="currentColor" stroke-width="1.5">'
                    '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12">'
                    "</polyline></svg>"
                ),
            ),
        )
        for item in body.get("navigation", [])
    )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(body["title"])}</title>
    <style>
      :root {{
        --primary: #1976d2;
        --primary-light: #42a5f5;
        --primary-dark: #1565c0;
        --accent: #f57c00;
        --success: #388e3c;
        --bg-light: #fafbfc;
        --bg: #f1f3f5;
        --fg: #ffffff;
        --text: #1a202c;
        --text-muted: #718096;
        --border: #e2e8f0;
        --shadow-xs: 0 1px 2px rgba(0, 0, 0, 0.05);
        --shadow-sm: 0 4px 6px rgba(0, 0, 0, 0.1);
        --shadow-md: 0 10px 15px rgba(0, 0, 0, 0.1);
        --shadow-lg: 0 20px 25px rgba(0, 0, 0, 0.1);
      }}

      * {{ box-sizing: border-box; }}

      body {{
        margin: 0;
        padding: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto",
          "Oxygen", "Ubuntu", "Cantarell", "Fira Sans", "Droid Sans",
          "Helvetica Neue", sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        color: var(--text);
        background: linear-gradient(135deg, var(--bg-light) 0%, var(--bg) 100%);
        min-height: 100vh;
      }}

      main {{
        max-width: 1200px;
        margin: 0 auto;
        padding: 60px 24px;
      }}

      .eyebrow {{
        display: inline-block;
        padding: 8px 14px;
        border-radius: 8px;
        background: linear-gradient(135deg, rgba(25, 118, 210, 0.1) 0%, rgba(79, 195, 247, 0.05) 100%);
        color: var(--primary-dark);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        margin-bottom: 16px;
      }}

      h1 {{
        margin: 0 0 12px;
        font-size: clamp(2.4rem, 6vw, 3.6rem);
        font-weight: 800;
        line-height: 1.1;
        letter-spacing: -0.02em;
        background: linear-gradient(135deg, var(--text) 0%, var(--primary) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
      }}

      .subtitle {{
        max-width: 720px;
        margin: 0 0 48px;
        color: var(--text-muted);
        font-size: 1.125rem;
        line-height: 1.6;
        font-weight: 400;
      }}

      /* Navigation Grid */
      .nav-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap: 20px;
        margin-bottom: 56px;
      }}

      .nav-card {{
        position: relative;
        display: flex;
        flex-direction: column;
        padding: 28px;
        border: 1px solid var(--border);
        border-radius: 16px;
        background: var(--fg);
        box-shadow: var(--shadow-sm);
        transition: all 280ms cubic-bezier(0.34, 1.56, 0.64, 1);
        text-decoration: none;
        color: inherit;
        overflow: hidden;
      }}

      .nav-card::before {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--primary) 0%, var(--accent) 100%);
        transform: scaleX(0);
        transform-origin: left;
        transition: transform 280ms ease;
      }}

      .nav-card:hover {{
        border-color: var(--primary);
        box-shadow: var(--shadow-md);
        transform: translateY(-6px);
      }}

      .nav-card:hover::before {{
        transform: scaleX(1);
      }}

      .nav-card:hover .card-icon {{
        color: var(--primary);
        transform: scale(1.1) rotate(5deg);
      }}

      .nav-card:hover .card-arrow {{
        transform: translateX(4px);
      }}

      .card-icon-wrapper {{
        margin-bottom: 16px;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 48px;
        height: 48px;
        border-radius: 12px;
        background: linear-gradient(135deg, rgba(25, 118, 210, 0.1) 0%, rgba(79, 195, 247, 0.05) 100%);
      }}

      .card-icon {{
        width: 28px;
        height: 28px;
        color: var(--primary);
        flex-shrink: 0;
        transition: all 280ms ease;
      }}

      .card-title {{
        margin: 0 0 8px;
        font-size: 1.125rem;
        font-weight: 700;
        line-height: 1.3;
      }}

      .card-desc {{
        flex-grow: 1;
        margin: 0 0 16px;
        color: var(--text-muted);
        font-size: 0.875rem;
        line-height: 1.5;
      }}

      .card-arrow {{
        color: var(--primary);
        font-weight: 700;
        font-size: 1.25rem;
        transition: transform 280ms ease;
        margin-top: auto;
      }}

      /* Responsive */
      @media (max-width: 768px) {{
        main {{
          padding: 40px 16px;
        }}

        h1 {{
          font-size: clamp(2rem, 5vw, 2.8rem);
        }}

        .nav-grid {{
          grid-template-columns: 1fr;
        }}
      }}

      /* Print styles */
      @media print {{
        body {{
          background: white;
        }}

        .nav-card, .link-row {{
          box-shadow: none;
        }}
      }}
    </style>
  </head>
  <body>
    <main>
      <div class="eyebrow">OGC API</div>
      <h1>{escape(body["title"])}</h1>
      <p class="subtitle">{escape(body["description"])}</p>

      <section class="nav-grid">
        {nav_cards}
      </section>

      <footer>
        <p>🌍 DHIS2 Earth Observation API • <a href="https://github.com/dhis2/eo-api">GitHub</a></p>
      </footer>
    </main>
  </body>
</html>"""
