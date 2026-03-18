"""Thin OGC API adapter routes over the native workflow engine."""

from __future__ import annotations

import json
import uuid
from html import escape
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

from ..publications.schemas import PublishedResourceExposure
from ..publications.services import (
    collection_id_for_resource,
    ensure_source_dataset_publications,
    get_published_resource,
    get_published_resource_by_collection_id,
    list_published_resources,
)
from ..workflows.schemas import ApiErrorResponse, WorkflowExecuteEnvelopeRequest, WorkflowJobStatus
from ..workflows.services.definitions import load_workflow_definition
from ..workflows.services.engine import execute_workflow
from ..workflows.services.job_store import get_job, get_job_result, initialize_job, list_jobs
from ..workflows.services.simple_mapper import normalize_simple_request

router = APIRouter()

_PROCESS_ID = "generic-dhis2-workflow"
_PROCESS_TITLE = "Generic DHIS2 workflow"


def _api_error(
    *,
    error: str,
    error_code: str,
    message: str,
    resource_id: str | None = None,
    process_id: str | None = None,
    job_id: str | None = None,
    status: str | None = None,
) -> dict[str, str]:
    return ApiErrorResponse(
        error=error,
        error_code=error_code,
        message=message,
        resource_id=resource_id,
        process_id=process_id,
        job_id=job_id,
        status=status,
    ).model_dump(exclude_none=True)


@router.get("/collections", response_model=None)
def list_collections(request: Request, f: str | None = None) -> dict[str, Any] | HTMLResponse:
    """List OGC collections directly from live publication state."""
    collections = [_collection_summary(resource, request) for resource in _ogc_resources()]
    body = {
        "collections": collections,
        "links": [
            {"rel": "self", "type": "application/json", "href": _request_href(request, f="json")},
            {"rel": "alternate", "type": "text/html", "href": _request_href(request, f="html")},
            {"rel": "root", "type": "application/json", "href": str(request.base_url).rstrip("/") + "/ogcapi"},
        ],
    }
    if _wants_html(request, f):
        return HTMLResponse(_render_collections_html(collections))
    return body


@router.get("/collections/{collection_id}", response_model=None)
def get_collection(collection_id: str, request: Request, f: str | None = None) -> dict[str, Any] | HTMLResponse:
    """Return one dynamic collection document backed by publication truth."""
    resource = get_published_resource_by_collection_id(collection_id)
    if resource is None or resource.exposure != PublishedResourceExposure.OGC:
        raise HTTPException(
            status_code=404,
            detail=_api_error(
                error="collection_not_found",
                error_code="COLLECTION_NOT_FOUND",
                message=f"Unknown collection_id '{collection_id}'",
                resource_id=collection_id,
            ),
        )
    body = _collection_detail(resource, request)
    if _wants_html(request, f):
        return HTMLResponse(_render_collection_html(body))
    return body


@router.get("/collections/{collection_id}/items", response_model=None)
def get_collection_items(
    collection_id: str,
    request: Request,
    limit: int = 20,
    offset: int = 0,
    period: str | None = None,
    view: str | None = None,
    f: str | None = None,
) -> dict[str, Any] | HTMLResponse:
    """Return dynamic feature items for a GeoJSON-backed published collection."""
    resource = get_published_resource_by_collection_id(collection_id)
    if resource is None or resource.exposure != PublishedResourceExposure.OGC:
        raise HTTPException(
            status_code=404,
            detail=_api_error(
                error="collection_not_found",
                error_code="COLLECTION_NOT_FOUND",
                message=f"Unknown collection_id '{collection_id}'",
                resource_id=collection_id,
            ),
        )
    if resource.path is None or Path(resource.path).suffix.lower() != ".geojson":
        raise HTTPException(
            status_code=409,
            detail=_api_error(
                error="collection_items_unavailable",
                error_code="COLLECTION_ITEMS_UNAVAILABLE",
                message=f"Collection '{collection_id}' does not expose OGC items",
                resource_id=collection_id,
            ),
        )
    items = _load_feature_collection(resource.path)
    features = items.get("features", [])
    if period is not None:
        features = [feature for feature in features if feature.get("properties", {}).get("period") == period]
    matched = len(features)
    page = features[offset : offset + max(limit, 0)]
    body = {
        "type": "FeatureCollection",
        "id": collection_id,
        "title": resource.title,
        "numberMatched": matched,
        "numberReturned": len(page),
        "features": page,
        "links": _item_links(request, resource, limit=limit, offset=offset, matched=matched, period=period),
    }
    if _wants_html(request, f):
        return HTMLResponse(
            _render_items_html(
                resource,
                page,
                limit=limit,
                offset=offset,
                matched=matched,
                selected_period=period,
                view_mode=view or "browse",
            )
        )
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
        collection_id = collection_id_for_resource(publication)
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id),
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
            detail=_api_error(
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
            detail=_api_error(
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
            detail=_api_error(
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
            detail=_api_error(
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
    return str(request.base_url).rstrip("/") + f"/ogcapi/collections/{collection_id}"


def _ogc_resources() -> list[Any]:
    ensure_source_dataset_publications()
    return list_published_resources(exposure=PublishedResourceExposure.OGC)


def _collection_summary(resource: Any, request: Request) -> dict[str, Any]:
    collection_id = collection_id_for_resource(resource)
    item_type = "feature" if resource.path and Path(resource.path).suffix.lower() == ".geojson" else "coverage"
    representation_type = "Feature Collection" if item_type == "feature" else "Coverage"
    links = [
        {"rel": "self", "type": "application/json", "href": _collection_href(request, collection_id)},
        {"rel": "alternate", "type": "text/html", "href": _collection_href(request, collection_id) + "?f=html"},
    ]
    if item_type == "feature":
        links.extend(
            [
                {
                    "rel": "items",
                    "type": "application/geo+json",
                    "href": _collection_href(request, collection_id) + "/items",
                },
                {
                    "rel": "items-html",
                    "type": "text/html",
                    "href": _collection_href(request, collection_id) + "/items?f=html",
                },
            ]
        )
    for link in resource.links:
        href = str(link.get("href", ""))
        if href:
            links.append(
                {
                    "rel": str(link.get("rel", "related")),
                    "type": "text/html" if link.get("rel") == "analytics" else "application/json",
                    "href": _absolute_href(request, href),
                }
            )
    return {
        "id": collection_id,
        "title": resource.title,
        "description": resource.description,
        "itemType": item_type,
        "representationType": representation_type,
        "extent": {"spatial": {"bbox": [_bbox_for_resource(resource)]}},
        "links": links,
    }


def _collection_detail(resource: Any, request: Request) -> dict[str, Any]:
    collection = _collection_summary(resource, request)
    collection["crs"] = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]
    collection["storageCrs"] = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    collection["keywords"] = _keywords_for_resource(resource)
    collection["metadata"] = {
        "resource_id": resource.resource_id,
        "resource_class": str(resource.resource_class),
        "dataset_id": resource.dataset_id,
        "workflow_id": resource.workflow_id,
        "job_id": resource.job_id,
    }
    return collection


def _item_links(
    request: Request, resource: Any, *, limit: int, offset: int, matched: int, period: str | None
) -> list[dict[str, str]]:
    collection_id = collection_id_for_resource(resource)
    base_href = _collection_href(request, collection_id) + "/items"
    period_query = f"&period={period}" if period is not None else ""
    links = [
        {
            "rel": "self",
            "type": "application/geo+json",
            "href": f"{base_href}?limit={limit}&offset={offset}{period_query}",
        },
        {"rel": "collection", "type": "application/json", "href": _collection_href(request, collection_id)},
        {
            "rel": "alternate",
            "type": "text/html",
            "href": f"{base_href}?limit={limit}&offset={offset}{period_query}&f=html",
        },
    ]
    if offset + limit < matched:
        links.append(
            {
                "rel": "next",
                "type": "application/geo+json",
                "href": f"{base_href}?limit={limit}&offset={offset + limit}{period_query}",
            }
        )
    if offset > 0:
        links.append(
            {
                "rel": "prev",
                "type": "application/geo+json",
                "href": f"{base_href}?limit={limit}&offset={max(0, offset - limit)}{period_query}",
            }
        )
    return links


def _load_feature_collection(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=_api_error(
                error="published_asset_not_found",
                error_code="PUBLISHED_ASSET_NOT_FOUND",
                message=f"Published feature asset does not exist: {path_value}",
            ),
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=409,
            detail=_api_error(
                error="published_asset_invalid",
                error_code="PUBLISHED_ASSET_INVALID",
                message="Published feature asset is not a GeoJSON object",
            ),
        )
    return payload


def _wants_html(request: Request, f: str | None) -> bool:
    if f == "html":
        return True
    if f in {"json", "jsonld"}:
        return False
    accept = request.headers.get("accept", "")
    return "text/html" in accept.lower()


def _request_href(request: Request, *, f: str | None = None) -> str:
    href = str(request.url).split("?")[0]
    return f"{href}?f={f}" if f is not None else href


def _absolute_href(request: Request, href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return str(request.base_url).rstrip("/") + href


def _bbox_for_resource(resource: Any) -> list[float]:
    bbox = resource.metadata.get("bbox")
    if isinstance(bbox, list) and len(bbox) == 4:
        return [float(value) for value in bbox]
    return [-180.0, -90.0, 180.0, 90.0]


def _keywords_for_resource(resource: Any) -> list[str]:
    keywords = ["EO", "DHIS2", str(resource.resource_class), str(resource.kind)]
    if resource.dataset_id is not None:
        keywords.append(resource.dataset_id)
    if resource.workflow_id is not None:
        keywords.append(resource.workflow_id)
    return keywords


def _render_collections_html(collections: list[dict[str, Any]]) -> str:
    rows = []
    for collection in collections:
        links = {link["rel"]: link["href"] for link in collection["links"]}
        analytics = links.get("analytics")
        analytics_html = f'<a class="chip" href="{escape(analytics)}">Analytics</a>' if analytics else ""
        title = escape(collection["title"])
        description = escape(collection["description"])
        rows.append(
            f"""
            <tr>
              <td class="title-cell">
                <a class="title-link" href="{escape(links["alternate"])}">{title}</a>
                <div class="subtle">{description}</div>
                <div class="dataset-note">{escape(_dataset_note_for_collection(collection))}</div>
              </td>
              <td><code>{escape(collection["id"])}</code></td>
              <td><span class="type-pill">{escape(collection["representationType"]).upper()}</span></td>
              <td class="actions">
                <a class="chip" href="{escape(links["alternate"])}">Browse</a>
                <a class="chip" href="{escape(links["self"])}">JSON</a>
                {analytics_html}
              </td>
            </tr>
            """
        )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>OGC Collections</title>
    <style>
      :root {{
        --ink: #172033;
        --muted: #637087;
        --line: rgba(23, 32, 51, 0.12);
        --accent: #ab3b1f;
        --accent-soft: #f2c46f;
        --panel: rgba(255, 251, 246, 0.92);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(242, 196, 111, 0.24), transparent 28%),
          radial-gradient(circle at right, rgba(69, 113, 181, 0.14), transparent 24%),
          linear-gradient(180deg, #fcfaf6 0%, #f3ede2 100%);
      }}
      main {{ max-width: 1260px; margin: 0 auto; padding: 42px 24px 56px; }}
      .hero {{
        display: grid;
        grid-template-columns: minmax(0, 1.2fr) minmax(260px, 0.6fr);
        gap: 18px;
        align-items: end;
      }}
      .eyebrow {{
        display: inline-flex;
        padding: 6px 12px;
        border-radius: 999px;
        background: rgba(171, 59, 31, 0.08);
        color: var(--accent);
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}
      h1 {{
        margin: 14px 0 10px;
        font-size: clamp(2.7rem, 5vw, 4.4rem);
        line-height: 0.96;
        letter-spacing: -0.05em;
      }}
      p.lead {{ color: var(--muted); max-width: 780px; font-size: 1.05rem; }}
      .hero-side {{
        background: rgba(255, 255, 255, 0.7);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 18px 20px;
        box-shadow: 0 18px 60px rgba(23, 32, 51, 0.08);
      }}
      .hero-side strong {{
        display: block;
        font-size: 2rem;
        line-height: 1;
      }}
      .hero-side span {{ color: var(--muted); display: block; margin-top: 8px; }}
      .crumbs {{
        display: flex;
        gap: 10px;
        align-items: center;
        color: var(--muted);
        font-size: 0.94rem;
        margin-bottom: 18px;
      }}
      .crumbs a {{ color: var(--accent); text-decoration: none; }}
      .table-shell {{
        margin-top: 28px;
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        box-shadow: 0 18px 58px rgba(23, 32, 51, 0.08);
        overflow: hidden;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        padding: 16px 18px;
        text-align: left;
        vertical-align: top;
        border-bottom: 1px solid var(--line);
      }}
      th {{
        background: rgba(255, 255, 255, 0.72);
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.8rem;
      }}
      tr:last-child td {{ border-bottom: none; }}
      .title-cell {{ min-width: 280px; }}
      .title-link {{
        color: var(--ink);
        text-decoration: none;
        font-size: 1.05rem;
        font-weight: 700;
      }}
      .subtle {{
        margin-top: 6px;
        color: var(--muted);
        font-size: 0.94rem;
      }}
      .dataset-note {{
        margin-top: 6px;
        color: #8b5e34;
        font-size: 0.9rem;
      }}
      code {{
        font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
        font-size: 0.86rem;
      }}
      .type-pill {{
        display: inline-flex;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(52, 120, 246, 0.1);
        color: #2755aa;
        font-size: 0.74rem;
        font-weight: 700;
        letter-spacing: 0.08em;
      }}
      .actions {{ display: flex; flex-wrap: wrap; gap: 10px; }}
      .chip {{
        display: inline-flex;
        align-items: center;
        padding: 9px 13px;
        border-radius: 999px;
        background: rgba(171, 59, 31, 0.08);
        color: var(--accent);
        font-weight: 600;
      }}
      @media (max-width: 960px) {{
        .hero {{ grid-template-columns: 1fr; }}
      }}
      @media (max-width: 720px) {{
        main {{ padding: 30px 16px 44px; }}
        th:nth-child(2), td:nth-child(2) {{ display: none; }}
      }}
    </style>
  </head>
  <body>
    <main>
      <nav class="crumbs">
        <a href="/ogcapi">OGC Home</a><span>/</span><strong>Collections</strong>
      </nav>
      <section class="hero">
        <div>
          <span class="eyebrow">Live OGC Surface</span>
          <h1>Collections</h1>
          <p class="lead">
            Live collection discovery from backend publication truth. New publications and deletions appear
            here immediately without restarting the OGC surface.
          </p>
          <div class="actions" style="margin-top:16px;">
            <a class="chip" href="/ogcapi">OGC Home</a>
          </div>
        </div>
        <aside class="hero-side">
          <strong>{len(collections)}</strong>
          <span>Collections currently exposed through the dynamic OGC collection surface.</span>
        </aside>
      </section>
      <section class="table-shell">
        <table>
          <thead>
            <tr>
              <th>Collection</th>
              <th>Identifier</th>
              <th>Type</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {"".join(rows)}
          </tbody>
        </table>
      </section>
    </main>
  </body>
</html>"""


def _dataset_note_for_collection(collection: dict[str, Any]) -> str:
    collection_id = str(collection.get("id", ""))
    title = str(collection.get("title", ""))
    if collection_id.startswith("workflow-output-"):
        dataset_name = title.split(" output for ", 1)[-1] if " output for " in title else collection_id
        return f"Source dataset: {dataset_name}"
    return ""


def _render_collection_html(collection: dict[str, Any]) -> str:
    links = {link["rel"]: link["href"] for link in collection["links"]}
    analytics = links.get("analytics")
    browse_items = links.get("items-html")
    analytics_html = (
        f'<a class="action" href="{escape(analytics)}">Open Analytics Viewer</a>' if analytics is not None else ""
    )
    browse_html = f'<a class="action" href="{escape(browse_items)}">Browse Items</a>' if browse_items else ""
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(collection["title"])}</title>
    <style>
      :root {{
        --ink: #172033;
        --muted: #637087;
        --line: rgba(23, 32, 51, 0.12);
        --accent: #ab3b1f;
      }}
      body {{
        font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
        margin: 0;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(242, 196, 111, 0.18), transparent 28%),
          linear-gradient(180deg, #fcfaf6 0%, #f5efe5 100%);
      }}
      main {{ max-width: 1120px; margin: 0 auto; padding: 40px 24px 52px; }}
      .crumbs {{
        display: flex;
        gap: 10px;
        align-items: center;
        color: var(--muted);
        font-size: 0.94rem;
        margin-bottom: 18px;
      }}
      .crumbs a {{ color: var(--accent); text-decoration: none; }}
      .eyebrow {{
        display: inline-flex;
        padding: 6px 12px;
        border-radius: 999px;
        background: rgba(171, 59, 31, 0.08);
        color: var(--accent);
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}
      h1 {{ margin: 14px 0 10px; font-size: clamp(2.4rem, 4vw, 3.6rem); line-height: 1.02; }}
      p.subhead {{ color: var(--muted); max-width: 760px; }}
      .deck {{ display: grid; gap: 18px; grid-template-columns: 1.4fr 0.9fr; margin-top: 24px; }}
      .panel {{
        background: rgba(255, 251, 246, 0.94);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 22px;
        box-shadow: 0 16px 52px rgba(23,32,51,0.07);
      }}
      .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 18px; }}
      .action {{
        display: inline-flex;
        padding: 10px 14px;
        border-radius: 999px;
        text-decoration: none;
        background: rgba(171,59,31,0.08);
        color: var(--accent);
        font-weight: 600;
      }}
      dl {{ display: grid; grid-template-columns: max-content 1fr; gap: 10px 14px; margin: 0; }}
      dt {{ color: var(--muted); }}
      dd {{ margin: 0; }}
      pre {{ overflow: auto; background: #fff7ef; padding: 14px; border-radius: 16px; }}
      @media (max-width: 900px) {{ .deck {{ grid-template-columns: 1fr; }} }}
    </style>
  </head>
  <body>
    <main>
      <nav class="crumbs">
        <a href="/ogcapi">OGC Home</a><span>/</span><a href="/ogcapi/collections?f=html">Collections</a><span>/</span>
        <strong>{escape(collection["title"])}</strong>
      </nav>
      <span class="eyebrow">Collection Detail</span>
      <h1>{escape(collection["title"])}</h1>
      <p class="subhead">{escape(collection["description"])}</p>
      <div class="actions">
        <a class="action" href="/ogcapi">OGC Home</a>
        <a class="action" href="/ogcapi/collections?f=html">Back to Collections</a>
        <a class="action" href="{escape(links["self"])}">Collection JSON</a>
        {browse_html}
        {analytics_html}
      </div>
      <section class="deck">
        <article class="panel">
          <h2>Collection Info</h2>
          <dl>
            <dt>Identifier</dt><dd>{escape(collection["id"])}</dd>
            <dt>Item type</dt><dd>{escape(collection["itemType"])}</dd>
            <dt>Storage CRS</dt><dd>{escape(collection["storageCrs"])}</dd>
          </dl>
        </article>
        <article class="panel">
          <h2>Metadata</h2>
          <pre>{escape(json.dumps(collection["metadata"], indent=2))}</pre>
        </article>
      </section>
    </main>
  </body>
</html>"""


def _render_items_html(
    resource: Any,
    features: list[dict[str, Any]],
    *,
    limit: int,
    offset: int,
    matched: int,
    selected_period: str | None,
    view_mode: str,
) -> str:
    properties = [feature.get("properties", {}) for feature in features]
    columns = []
    for props in properties:
        for key in props:
            if key not in columns:
                columns.append(key)
    header_html = "".join(f"<th>{escape(column)}</th>" for column in columns)
    collection_id = collection_id_for_resource(resource)
    analytics = next((link["href"] for link in resource.links if link.get("rel") == "analytics"), None)
    page_geojson = json.dumps({"type": "FeatureCollection", "features": features})
    selected_period_json = json.dumps(selected_period)
    period_query = f"&period={selected_period}" if selected_period is not None else ""
    next_href = (
        f"/ogcapi/collections/{collection_id}/items?"
        f"limit={limit}&offset={offset + limit}{period_query}&f=html&view={escape(view_mode)}"
        if offset + limit < matched
        else None
    )
    prev_href = (
        f"/ogcapi/collections/{collection_id}/items?"
        f"limit={limit}&offset={max(0, offset - limit)}{period_query}&f=html&view={escape(view_mode)}"
        if offset > 0
        else None
    )
    analytics_html = f'<a class="action" href="{escape(analytics)}">Open Analytics Viewer</a>' if analytics else ""
    browse_active = view_mode != "analytics"
    browse_href = (
        f"/ogcapi/collections/{collection_id}/items?limit={limit}&offset={offset}{period_query}&f=html&view=browse"
    )
    analytics_href = (
        f"/ogcapi/collections/{collection_id}/items?limit={limit}&offset={offset}{period_query}&f=html&view=analytics"
    )
    embedded_analytics_html = (
        f'<iframe class="analytics-frame" src="/analytics/publications/{escape(resource.resource_id)}/viewer?embed=1" '
        'title="Embedded analytics viewer" loading="lazy"></iframe>'
        if analytics
        else '<div class="empty" style="padding: 20px;">Analytics viewer unavailable for this collection.</div>'
    )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(resource.title)} Items</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin="">
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
    <style>
      :root {{
        --ink: #172033;
        --muted: #637087;
        --line: rgba(23, 32, 51, 0.12);
        --accent: #ab3b1f;
      }}
      body {{
        font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
        margin: 0;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(242, 196, 111, 0.18), transparent 26%),
          linear-gradient(180deg, #fcfaf6 0%, #f5efe5 100%);
      }}
      main {{ max-width: 1440px; margin: 0 auto; padding: 34px 24px 44px; }}
      .crumbs {{
        display: flex;
        gap: 10px;
        align-items: center;
        color: var(--muted);
        font-size: 0.94rem;
        margin-bottom: 18px;
      }}
      .crumbs a {{ color: var(--accent); text-decoration: none; }}
      .eyebrow {{
        display: inline-flex;
        padding: 6px 12px;
        border-radius: 999px;
        background: rgba(171, 59, 31, 0.08);
        color: var(--accent);
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}
      h1 {{ margin: 14px 0 10px; font-size: clamp(2.2rem, 4vw, 3.4rem); line-height: 1.02; }}
      p.subhead {{ color: var(--muted); max-width: 820px; }}
      .layout {{ display: grid; grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.95fr); gap: 18px; }}
      .controls {{
        display: flex;
        flex-wrap: wrap;
        align-items: end;
        gap: 12px;
        margin: 16px 0 14px;
      }}
      .controls label {{
        display: grid;
        gap: 6px;
        color: var(--muted);
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .controls select {{
        min-width: 180px;
        padding: 10px 12px;
        border-radius: 12px;
        border: 1px solid var(--line);
        background: rgba(255, 251, 246, 0.98);
        color: var(--ink);
        font: inherit;
      }}
      .period-pill {{
        display: inline-flex;
        align-items: center;
        min-height: 44px;
        padding: 0 14px;
        border-radius: 999px;
        background: rgba(52, 120, 246, 0.08);
        color: #2755aa;
        font-weight: 700;
      }}
      .panel {{
        background: rgba(255, 251, 246, 0.94);
        border: 1px solid var(--line);
        border-radius: 24px;
        overflow: hidden;
        box-shadow: 0 16px 52px rgba(23,32,51,0.07);
      }}
      .map-panel {{
        display: grid;
        grid-template-rows: 1fr auto;
      }}
      .mode-toggle {{
        display: flex;
        gap: 10px;
        margin: 16px 0 6px;
      }}
      .mode-chip {{
        display: inline-flex;
        align-items: center;
        padding: 10px 14px;
        border-radius: 999px;
        text-decoration: none;
        background: rgba(52, 120, 246, 0.08);
        color: #2755aa;
        font-weight: 700;
      }}
      .mode-chip.inactive {{
        background: rgba(23, 32, 51, 0.05);
        color: var(--muted);
      }}
      .head {{ padding: 18px 20px; border-bottom: 1px solid var(--line); }}
      .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; }}
      .action {{
        display: inline-flex;
        padding: 10px 14px;
        border-radius: 999px;
        text-decoration: none;
        background: rgba(171,59,31,0.08);
        color: var(--accent);
        font-weight: 600;
      }}
      #map {{ min-height: 620px; }}
      .legend {{
        padding: 14px 18px 18px;
        border-top: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.58);
      }}
      .legend-title {{
        color: var(--muted);
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .legend-bar {{
        height: 14px;
        margin-top: 10px;
        border-radius: 999px;
        background: linear-gradient(90deg, #eef6ff 0%, #b8dcff 35%, #5fa6f2 68%, #124fa3 100%);
        border: 1px solid rgba(23,32,51,0.1);
      }}
      .legend-scale {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        margin-top: 8px;
        color: var(--muted);
        font-size: 0.88rem;
      }}
      .table-wrap {{ max-height: 760px; overflow: auto; }}
      .analytics-frame {{
        width: 100%;
        min-height: 1180px;
        border: 0;
        border-radius: 24px;
        background: transparent;
      }}
      table {{ width: 100%; border-collapse: collapse; }}
      th, td {{
        padding: 10px 14px;
        border-bottom: 1px solid rgba(23,32,51,0.08);
        text-align: left;
        vertical-align: top;
      }}
      th {{ position: sticky; top: 0; background: rgba(255, 251, 246, 0.98); }}
      .pager {{ display: flex; gap: 10px; margin-top: 10px; }}
      .empty {{ color: var(--muted); font-style: italic; }}
      @media (max-width: 1080px) {{ .layout {{ grid-template-columns: 1fr; }} #map {{ min-height: 420px; }} }}
    </style>
  </head>
  <body>
    <main>
      <nav class="crumbs">
        <a href="/ogcapi">OGC Home</a>
        <span>/</span>
        <a href="/ogcapi/collections?f=html">Collections</a>
        <span>/</span>
        <a href="/ogcapi/collections/{escape(collection_id)}?f=html">Collection</a>
        <span>/</span>
        <strong>Items</strong>
      </nav>
      <span class="eyebrow">Collection Items</span>
      <h1>{escape(resource.title)}</h1>
      <p class="subhead">
        Dynamic items page over live publication state. For time-aware exploration, use the analytics viewer.
      </p>
      <div class="actions">
        <a class="action" href="/ogcapi">OGC Home</a>
        <a class="action" href="/ogcapi/collections?f=html">Back to Collections</a>
        <a class="action" href="/ogcapi/collections/{escape(collection_id)}?f=html">Back to Collection</a>
        <a
          class="action"
          href="/ogcapi/collections/{escape(collection_id)}/items?limit={limit}&offset={offset}"
        >JSON</a>
        {analytics_html}
      </div>
      <div class="mode-toggle">
        <a class="mode-chip{" inactive" if not browse_active else ""}" href="{escape(browse_href)}">Browse</a>
        <a class="mode-chip{" inactive" if browse_active else ""}" href="{escape(analytics_href)}">Analytics</a>
      </div>
      <div class="pager">
        {f'<a class="action" href="{escape(prev_href)}">Previous</a>' if prev_href else ""}
        {f'<a class="action" href="{escape(next_href)}">Next</a>' if next_href else ""}
      </div>
      <p>Showing {offset + 1 if matched else 0} to {offset + len(features)} of {matched} items.</p>
      {'<div class="controls">' if browse_active else '<div class="controls" style="margin-bottom:18px;">'}
        <label>
          Select Period
          <select id="period-select"></select>
        </label>
        <div class="period-pill" id="period-pill">All Periods</div>
      </div>
      {
        (
            f'''<section class="layout">
        <article class="panel map-panel">
          <div id="map"></div>
          <div class="legend">
            <div class="legend-title">Value Scale</div>
            <div class="legend-bar"></div>
            <div class="legend-scale">
              <span id="legend-min">0</span>
              <span id="legend-max">0</span>
            </div>
          </div>
        </article>
        <article class="panel">
          <div class="head"><strong>Current Page</strong></div>
          <div class="table-wrap">
            <table>
              <thead><tr>{header_html}</tr></thead>
              <tbody id="items-body"></tbody>
            </table>
            <p class="empty" id="items-empty" hidden>No records for the selected period.</p>
          </div>
        </article>
      </section>'''
            if browse_active
            else f'''<section>
        <article class="panel">
          {embedded_analytics_html}
        </article>
      </section>'''
        )
    }
    </main>
    <script>
      const featureCollection = {page_geojson};
      const columns = {json.dumps(columns)};
      const selectedPeriod = {selected_period_json};
      const allFeatures = featureCollection.features || [];
      const periodSelect = document.getElementById('period-select');
      const periodPill = document.getElementById('period-pill');
      const itemsBody = document.getElementById('items-body');
      const emptyState = document.getElementById('items-empty');
      const legendMin = document.getElementById('legend-min');
      const legendMax = document.getElementById('legend-max');
      const periods = Array.from(new Set(
        allFeatures
          .map((feature) => feature?.properties?.period)
          .filter((value) => typeof value === 'string' && value.length > 0)
      )).sort();
      const map = {str(browse_active).lower()} ? L.map('map') : null;
      if (map) {{
        L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
          attribution: '&copy; OpenStreetMap contributors'
        }}).addTo(map);
      }}
      let currentMin = 0;
      let currentMax = 0;

      function colorForValue(value) {{
        if (!Number.isFinite(value)) {{
          return '#d9e2f2';
        }}
        if (currentMax <= currentMin) {{
          return '#5fa6f2';
        }}
        const ratio = Math.max(0, Math.min(1, (value - currentMin) / (currentMax - currentMin)));
        if (ratio < 0.2) return '#eef6ff';
        if (ratio < 0.4) return '#b8dcff';
        if (ratio < 0.6) return '#7fc0ff';
        if (ratio < 0.8) return '#5fa6f2';
        return '#124fa3';
      }}

      function updateLegend(features) {{
        const values = features
          .map((feature) => Number(feature?.properties?.value))
          .filter((value) => Number.isFinite(value));
        currentMin = values.length > 0 ? Math.min(...values) : 0;
        currentMax = values.length > 0 ? Math.max(...values) : 0;
        legendMin.textContent = currentMin.toFixed(2);
        legendMax.textContent = currentMax.toFixed(2);
      }}

      const layer = map
        ? L.geoJSON(undefined, {{
            style: (feature) => ({{
              color: '#3478f6',
              weight: 1.5,
              fillColor: colorForValue(Number(feature?.properties?.value)),
              fillOpacity: 0.72
            }}),
            onEachFeature: (feature, target) => {{
              const props = feature.properties || {{}};
              const lines = Object.entries(props).map(([key, value]) => `<strong>${{key}}</strong>: ${{value}}`);
              target.bindPopup(lines.join('<br>'));
            }}
          }}).addTo(map)
        : null;

      function renderTable(features) {{
        itemsBody.innerHTML = '';
        if (features.length === 0) {{
          emptyState.hidden = false;
          return;
        }}
        emptyState.hidden = true;
        const rows = features.map((feature) => {{
          const props = feature.properties || {{}};
          const cells = columns.map((column) => `<td>${{props[column] ?? ''}}</td>`).join('');
          return `<tr>${{cells}}</tr>`;
        }});
        itemsBody.innerHTML = rows.join('');
      }}

      function filteredFeatures(period) {{
        return !period ? allFeatures : allFeatures.filter((feature) => feature?.properties?.period === period);
      }}

      function applyPeriod(period) {{
        const url = new URL(window.location.href);
        if (period) {{
          url.searchParams.set('period', period);
        }} else {{
          url.searchParams.delete('period');
        }}
        url.searchParams.set('f', 'html');
        window.location.href = url.toString();
      }}

      periodSelect.innerHTML = [
        '<option value="">All Periods</option>',
        ...periods.map((period) => `<option value="${{period}}">${{period}}</option>`)
      ].join('');
      periodSelect.value = selectedPeriod ?? '';
      periodPill.textContent = selectedPeriod ? `Period ${{selectedPeriod}}` : 'All Periods';
      periodSelect.addEventListener('change', (event) => applyPeriod(event.target.value));
      if ({str(browse_active).lower()}) {{
        const visibleFeatures = filteredFeatures(selectedPeriod);
        updateLegend(visibleFeatures);
        layer.addData({{ type: 'FeatureCollection', features: visibleFeatures }});
        renderTable(visibleFeatures);

        if (layer.getLayers().length > 0 && layer.getBounds().isValid()) {{
          map.fitBounds(layer.getBounds(), {{ padding: [20, 20] }});
        }} else {{
          map.setView([8.5, -11.8], 6);
        }}
      }}
    </script>
  </body>
</html>"""
