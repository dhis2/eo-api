---
name: eo-process-api-design
description: Design OGC API - Processes style EO execution endpoints and request/response contracts for DHIS2 EO API.
---

# EO Process API Design

## Use this skill when
- Adding or modifying process execution endpoints
- Designing dataset/process discovery and execution contracts
- Defining long-running execution behavior and status tracking

## Required endpoint baseline
- `GET /processes`
- `GET /processes/{process-id}`
- `POST /processes/{process-id}/execution`

## Design rules
- Keep resource names consistent and stable
- Use explicit IDs for process, execution, dataset
- Return structured validation errors with actionable messages
- Prefer async execution with status polling for long jobs
- Keep response schemas backward compatible

## EO/DHIS2 checks
- Document CRS assumptions and aggregation method semantics
- Include preview/dry-run support where feasible
- Use `dhis2-python-client` for DHIS2 Web API operations
- Use `dhis2eo` for EO/climate extraction and aggregation primitives
- Separate DHIS2 mapping from core EO process logic
- Identify API/library gaps that should be implemented upstream in `dhis2-python-client` or `dhis2eo`
- Validate response contract compatibility for `maps-app` and `climate-app` integration paths
- Prioritize process capabilities that replace current Google Earth Engine-backed functionality

## Output checklist
- Endpoint contract
- Example request/response
- Error model
- Job lifecycle model (`queued`, `running`, `succeeded`, `failed`)
