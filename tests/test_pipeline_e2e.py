"""End-to-end tests for the collection → process → job discovery pipeline.

These tests exercise the full OGC pattern:
  GET /collections/{id}              — discover dataset + embedded process links
  GET /processes/{id}                — confirm linked process is valid
  POST /processes/{id}/execution     — execute, receive full job inline (sync-execute)
  GET /jobs/{jobId}                  — re-fetch the same job by ID
  GET /jobs                          — list all jobs; our job must appear

The raster operation tests use a tiny in-memory NetCDF file so they run without
any network access or external data.
"""

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.collections import router as collections_router
from eoapi.endpoints.processes import router as processes_router
from eoapi.processing.providers.base import RasterFetchResult

try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:
    xr = None
    HAS_XARRAY = False


# ---------------------------------------------------------------------------
# Shared test client
# ---------------------------------------------------------------------------


def _make_client() -> TestClient:
    app = FastAPI()
    app.include_router(collections_router)
    app.include_router(processes_router)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Discovery tests (no raster files needed)
# ---------------------------------------------------------------------------


def test_collection_embeds_process_and_execute_links() -> None:
    """GET /collections/{id} must embed rel=process and rel=process-execute links
    for every registered process, so a client can discover execution endpoints
    from the collection without prior knowledge of the process catalog.
    """
    client = _make_client()

    response = client.get("/collections/chirps-daily")

    assert response.status_code == 200
    links = response.json()["links"]
    rels = {link["rel"] for link in links}
    assert "process" in rels, "No rel=process link found"
    assert "process-execute" in rels, "No rel=process-execute link found"

    execute_hrefs = [link["href"] for link in links if link["rel"] == "process-execute"]
    for process_id in ("raster.zonal_stats", "raster.point_timeseries", "data.temporal_aggregate", "dhis2.pipeline"):
        assert any(process_id in href for href in execute_hrefs), (
            f"process-execute link for '{process_id}' missing from collection links"
        )


def test_process_links_resolve_to_valid_definitions() -> None:
    """Each rel=process href embedded in a collection should return a valid
    process definition (200) with matching id and a rel=collection back-link.
    """
    client = _make_client()

    collection = client.get("/collections/chirps-daily").json()
    process_links = [link for link in collection["links"] if link["rel"] == "process"]
    assert process_links, "No rel=process links in collection"

    for link in process_links:
        # hrefs are absolute (http://testserver/processes/…); strip host
        path = "/" + link["href"].split("testserver/", 1)[-1]
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} returned {resp.status_code}"
        body = resp.json()
        assert body["id"] in {"raster.zonal_stats", "raster.point_timeseries", "data.temporal_aggregate", "dhis2.pipeline"}
        # Process definition must link back to /collections (OGC cross-link)
        back_rels = {lnk["rel"] for lnk in body["links"]}
        assert "collection" in back_rels, f"Process {body['id']} has no rel=collection back-link"


def test_execute_link_in_collection_points_to_real_endpoint() -> None:
    """The process-execute hrefs from a collection should return 405 (not 404)
    when called with GET — confirming the endpoint is registered and only
    the HTTP method is wrong, not the path.
    """
    client = _make_client()

    collection = client.get("/collections/chirps-daily").json()
    execute_link = next(
        (link for link in collection["links"] if link["rel"] == "process-execute"),
        None,
    )
    assert execute_link is not None

    path = "/" + execute_link["href"].split("testserver/", 1)[-1]
    # GET on an execute endpoint should be 405 Method Not Allowed, not 404 Not Found
    resp = client.get(path)
    assert resp.status_code == 405, f"Expected 405 (method not allowed) for GET {path}, got {resp.status_code}"


# ---------------------------------------------------------------------------
# Full pipeline test (requires xarray for raster computation)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_XARRAY, reason="xarray not available")
def test_full_pipeline_discover_execute_retrieve(monkeypatch, tmp_path: Path) -> None:
    """Full pipeline: collection discovery → process execution → job retrieval.

    Steps verified:
    1. GET /collections/chirps-daily  — find process-execute link for raster.zonal_stats
    2. POST /processes/raster.zonal_stats/execution  — execute; receive full job inline
    3. Inline result assertions:
         - status == "succeeded", processId, jobId present
         - rows[0].operation == "zonal_stats", status == "computed"
         - rows[0].value == 2.5 (mean of 2×2 grid [[1,2],[3,4]])
         - csv has header row; dhis2 stub present
    4. GET /jobs/{jobId}  — re-fetch by ID, assert identical computed value
    5. GET /jobs          — list; our job appears newest-first
    """
    # Build a minimal 2×2 NetCDF raster covering the AOI used in the execute call.
    nc_path = tmp_path / "precip_e2e.nc"
    ds = xr.Dataset(
        {"precip": (("lat", "lon"), [[1.0, 2.0], [3.0, 4.0]])},
        coords={"lat": [-9.75, -9.25], "lon": [30.25, 30.75]},
    )
    ds.to_netcdf(nc_path, engine="scipy")

    class LocalFileProvider:
        provider_id = "local-file"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[str(nc_path)],
                from_cache=False,
            )

        def implementation_details(self):
            return {"adapter": "test.local_file"}

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: LocalFileProvider())

    client = _make_client()

    # ── Step 1: discover ────────────────────────────────────────────────────
    collection_resp = client.get("/collections/chirps-daily")
    assert collection_resp.status_code == 200
    collection = collection_resp.json()

    execute_links = [link for link in collection["links"] if link["rel"] == "process-execute"]
    zonal_link = next(
        (link for link in execute_links if "raster.zonal_stats" in link["href"]),
        None,
    )
    assert zonal_link is not None, "raster.zonal_stats execute link not found in collection"
    execute_path = "/" + zonal_link["href"].split("testserver/", 1)[-1]

    # ── Step 2: execute ─────────────────────────────────────────────────────
    execute_resp = client.post(
        execute_path,
        json={
            "inputs": {
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-31",
                "aoi": [30.0, -10.0, 31.0, -9.0],
                "aggregation": "mean",
            }
        },
    )
    assert execute_resp.status_code == 200

    # ── Step 3: assert inline result ────────────────────────────────────────
    inline = execute_resp.json()
    assert inline["processId"] == "raster.zonal_stats"
    assert inline["status"] == "succeeded"
    job_id = inline["jobId"]
    assert job_id

    rows = inline["outputs"]["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["operation"] == "zonal_stats"
    assert row["parameter"] == "precip"
    assert row["status"] == "computed"
    assert row["value"] == pytest.approx(2.5, abs=1e-6)

    csv_lines = inline["outputs"]["csv"].splitlines()
    assert "parameter" in csv_lines[0]  # header row present

    assert inline["outputs"]["dhis2"]["status"] == "stub"
    assert inline["outputs"]["implementation"]["provider"]["id"] == "local-file"

    # self link in job response must point to /jobs/{jobId}
    job_self_links = [lnk for lnk in inline.get("links", []) if lnk["rel"] == "self"]
    assert any(job_id in lnk["href"] for lnk in job_self_links)

    # ── Step 4: re-fetch by jobId ───────────────────────────────────────────
    job_resp = client.get(f"/jobs/{job_id}")
    assert job_resp.status_code == 200
    job = job_resp.json()
    assert job["jobId"] == job_id
    assert job["status"] == "succeeded"
    assert job["outputs"]["rows"][0]["value"] == pytest.approx(2.5, abs=1e-6)

    # ── Step 5: list all jobs ───────────────────────────────────────────────
    jobs_resp = client.get("/jobs")
    assert jobs_resp.status_code == 200
    jobs_payload = jobs_resp.json()
    assert "jobs" in jobs_payload
    job_ids = [j["jobId"] for j in jobs_payload["jobs"]]
    assert job_id in job_ids
    # Newest-first: our job should be at position 0 (it was just created)
    assert jobs_payload["jobs"][0]["jobId"] == job_id


@pytest.mark.skipif(not HAS_XARRAY, reason="xarray not available")
def test_pipeline_point_timeseries(monkeypatch, tmp_path: Path) -> None:
    """collection → raster.point_timeseries → job with centroid lookup.

    Verifies that the point value returned is the raster cell nearest to the
    AOI centroid [31.0, -9.0] and that the job is retrievable by ID.
    """
    nc_path = tmp_path / "precip_pt.nc"
    ds = xr.Dataset(
        # 3×3 grid; centroid of aoi [30, -10, 32, -8] → lon=31, lat=-9
        # Place value 7.0 at (lon=31.0, lat=-9.0)
        {"precip": (("lat", "lon"), [[1.0, 2.0, 3.0], [4.0, 7.0, 6.0], [7.0, 8.0, 9.0]])},
        coords={"lat": [-9.5, -9.0, -8.5], "lon": [30.5, 31.0, 31.5]},
    )
    ds.to_netcdf(nc_path, engine="scipy")

    class LocalFileProvider:
        provider_id = "local-file"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[str(nc_path)],
                from_cache=False,
            )

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: LocalFileProvider())

    client = _make_client()

    resp = client.post(
        "/processes/raster.point_timeseries/execution",
        json={
            "inputs": {
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-31",
                "aoi": [30.0, -10.0, 32.0, -8.0],
            }
        },
    )
    assert resp.status_code == 200
    inline = resp.json()
    assert inline["status"] == "succeeded"

    row = inline["outputs"]["rows"][0]
    assert row["operation"] == "point_timeseries"
    assert row["point"] == [31.0, -9.0]  # centroid of aoi bbox
    assert row["status"] == "computed"
    assert row["value"] == pytest.approx(7.0, abs=1e-6)

    # Job retrievable by ID
    job_resp = client.get(f"/jobs/{inline['jobId']}")
    assert job_resp.status_code == 200
    assert job_resp.json()["outputs"]["rows"][0]["value"] == pytest.approx(7.0, abs=1e-6)


@pytest.mark.skipif(not HAS_XARRAY, reason="xarray not available")
def test_pipeline_temporal_aggregate_multi_file(monkeypatch, tmp_path: Path) -> None:
    """collection → data.temporal_aggregate → job with multi-file spatial mean.

    Two raster files simulate two daily files in a monthly window.
    Spatial mean of file 1 = 1.5; spatial mean of file 2 = 3.5.
    Temporal sum across files = 5.0.
    """
    nc1 = tmp_path / "precip_day1.nc"
    nc2 = tmp_path / "precip_day2.nc"

    for nc, vals in [(nc1, [[1.0, 2.0]]), (nc2, [[3.0, 4.0]])]:
        ds = xr.Dataset(
            {"precip": (("lat", "lon"), vals)},
            coords={"lat": [-9.5], "lon": [30.25, 30.75]},
        )
        ds.to_netcdf(nc, engine="scipy")

    class TwoFileProvider:
        provider_id = "two-file"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[str(nc1), str(nc2)],
                from_cache=False,
            )

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: TwoFileProvider())

    client = _make_client()

    resp = client.post(
        "/processes/data.temporal_aggregate/execution",
        json={
            "inputs": {
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-01",
                "aoi": [30.0, -10.0, 31.0, -9.0],
                "frequency": "P1M",
                "aggregation": "sum",
            }
        },
    )
    assert resp.status_code == 200
    inline = resp.json()
    assert inline["status"] == "succeeded"

    row = inline["outputs"]["rows"][0]
    assert row["operation"] == "temporal_aggregate"
    assert row["aggregation"] == "sum"
    assert row["sample_count"] == 2
    # spatial mean of file1 = 1.5, file2 = 3.5; temporal sum = 5.0
    assert row["value"] == pytest.approx(5.0, abs=1e-6)
    assert row["status"] == "computed"
