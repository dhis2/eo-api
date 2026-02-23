---
name: prd-authoring
description: Draft or refine PRDs for DHIS2 EO API features with clear MVP scope, requirements, and acceptance criteria.
---

# EO API PRD Authoring

## Use this skill when

- Defining new EO API features or integration flows
- Turning discovery notes into implementation-ready product requirements
- Aligning engineering work with hackathon or MVP objectives

## Output template

1. Problem statement
2. Goals and non-goals
3. User personas and user stories
4. Functional requirements
5. Non-functional requirements
6. MVP scope and out-of-scope
7. Success metrics
8. Risks/open questions
9. Acceptance criteria

## EO API-specific checks

- Includes org unit aggregation requirements
- Includes preview before import requirement where relevant
- Defines scheduled pipeline behavior clearly
- States DHIS2 integration boundary (direct push vs handoff)
- Specifies `dhis2-python-client` for DHIS2 Web API integration
- Specifies `dhis2eo` for EO/climate processing integration
- States whether any requirement needs upstream changes in `dhis2-python-client` or `dhis2eo`
- Includes compatibility and migration implications for `maps-app` and `climate-app`
- Explicitly identifies which Google Earth Engine-dependent behavior is replaced by `eo-api`
- Calls out standards alignment (OGC/STAC) when applicable
- Reflects current implemented baseline where relevant:
  - `/collections`, `/collections/{collectionId}`
  - `/collections/{collectionId}/coverage`
  - File-driven dataset metadata in `eoapi/datasets/<dataset-id>/<dataset-id>.yaml` validated by Pydantic
  - Validation/test workflow via `make validate-datasets` and `make test`

## Quality bar

- Requirements are testable and unambiguous
- MVP remains small and demoable end-to-end
- Risks and unresolved decisions are explicit
