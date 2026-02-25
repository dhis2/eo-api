# API Examples

Base URL (local):

http://127.0.0.1:8000

OGC landing page:

http://127.0.0.1:8000/

```bash
curl "http://127.0.0.1:8000/"
```

## Collection → Process → Job (Recommended Pipeline)

This is the primary use pattern. Every collection advertises which processes
can be run against it via embedded `rel=process` and `rel=process-execute`
links. A client can therefore:

1. **Discover** — fetch a collection and read its process links
2. **Execute** — POST to the linked execution endpoint; receive the full job
   result inline (sync-execute: status 200, no polling needed)
3. **Retrieve** — re-fetch the stored job at any time via `GET /jobs/{jobId}`
4. **List** — browse all past jobs via `GET /jobs`

```text
GET /collections/{id}              →  discover dataset + process-execute links
POST /processes/{id}/execution     →  run; full result returned inline (200)
GET /jobs/{jobId}                  →  re-fetch stored result at any time
GET /jobs                          →  list all past jobs, newest first
```

---

### Annotated step-by-step (CHIRPS, East Africa)

#### Step 1 — discover

```bash
curl -s "http://127.0.0.1:8000/collections/chirps-daily" | jq '{
  id, title,
  process_links: [.links[] | select(.rel=="process-execute") | {rel, href}]
}'
```

Expected (abbreviated):

```json
{
  "id": "chirps-daily",
  "title": "CHIRPS v3.0 Daily Precipitation",
  "process_links": [
    { "rel": "process-execute", "href": "http://…/processes/raster.zonal_stats/execution" },
    { "rel": "process-execute", "href": "http://…/processes/raster.point_timeseries/execution" },
    { "rel": "process-execute", "href": "http://…/processes/data.temporal_aggregate/execution" }
  ]
}
```

#### Step 2 — execute (zonal stats, synchronous)

```bash
curl -s -X POST "http://127.0.0.1:8000/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "chirps-daily",
      "params":     ["precip"],
      "time":       "2026-01-31",
      "aoi":        [30.0, -10.0, 31.0, -9.0],
      "aggregation":"mean"
    }
  }' | jq '{jobId, processId, status, rows: .outputs.rows}'
```

Expected (abbreviated — result is inline, no polling):

```json
{
  "jobId":     "3fa85f64-…",
  "processId": "raster.zonal_stats",
  "status":    "succeeded",
  "rows": [{
    "dataset_id": "chirps-daily",
    "parameter":  "precip",
    "operation":  "zonal_stats",
    "time":       "2026-01-31",
    "aoi_bbox":   [30.0, -10.0, 31.0, -9.0],
    "stat":       "mean",
    "value":      12.4,
    "status":     "computed"
  }]
}
```

> **Note:** `value` will be `null` and `status` will be `"missing_assets"` if
> the provider has not cached the raster file locally yet. The CHIRPS3 provider
> downloads on first fetch and caches; subsequent calls return the cached file.

#### Step 3 — re-fetch stored job

```bash
# Capture the jobId from Step 2
JOB_ID="3fa85f64-…"

curl -s "http://127.0.0.1:8000/jobs/$JOB_ID" | jq '{jobId, status, rows: .outputs.rows}'
```

#### Step 4 — list all past jobs

```bash
# All jobs, newest first
curl -s "http://127.0.0.1:8000/jobs" | jq '.jobs[] | {jobId, processId, status, created}'

# Only succeeded jobs
curl -s "http://127.0.0.1:8000/jobs" | jq '[.jobs[] | select(.status=="succeeded")]'
```

---

### One-liner pipeline script (bash)

Copy-paste into a terminal with the server running to run the full pipeline:

```bash
BASE="http://127.0.0.1:8000"

# 1. Discover chirps-daily → grab the zonal_stats execute href
EXEC=$(curl -s "$BASE/collections/chirps-daily" \
  | jq -r '.links[] | select(.rel=="process-execute" and (.href | contains("zonal_stats"))) | .href')

# 2. Execute — capture jobId from inline result
JOB_ID=$(curl -s -X POST "$EXEC" \
  -H "Content-Type: application/json" \
  -d '{"inputs":{"dataset_id":"chirps-daily","params":["precip"],"time":"2026-01-31","aoi":[30,-10,31,-9]}}' \
  | jq -r '.jobId')

echo "Job: $JOB_ID"

# 3. Re-fetch stored result
curl -s "$BASE/jobs/$JOB_ID" | jq '.outputs.rows[0] | {value, status, stat}'

# 4. List all jobs
curl -s "$BASE/jobs" | jq '[.jobs[] | {jobId, processId, status}]'
```

---

### CHIRPS precipitation — zonal stats

```bash
BASE="http://127.0.0.1:8000"

# 1) Discover collection — process links are embedded in the collection response
curl -s "$BASE/collections/chirps-daily" | jq '{
  id,
  links: [.links[] | select(.rel=="process" or .rel=="process-execute") | {rel, href}]
}'

# 2) Execute zonal stats — full job result returned inline
curl -s -X POST "$BASE/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "chirps-daily",
      "params": ["precip"],
      "time": "2026-01-31",
      "aoi": [30.0, -10.0, 31.0, -9.0],
      "aggregation": "mean"
    }
  }' | jq '{jobId, status, rows: .outputs.rows}'

# 3) Re-fetch the same job by ID at any time
JOB_ID=$(curl -s -X POST "$BASE/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d '{"inputs":{"dataset_id":"chirps-daily","params":["precip"],"time":"2026-01-31","aoi":[30.0,-10.0,31.0,-9.0]}}' \
  | jq -r '.jobId')

curl -s "$BASE/jobs/$JOB_ID" | jq '{processId, status, rows: .outputs.rows}'
```

### ERA5-Land temperature — zonal stats + temporal aggregation

```bash
BASE="http://127.0.0.1:8000"

# 1) Discover ERA5-Land collection
curl -s "$BASE/collections/era5-land-daily" | jq '{id, title, keywords}'

# 2) Zonal stats over East Africa bbox
curl -s -X POST "$BASE/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "era5-land-daily",
      "params": ["2m_temperature"],
      "time": "2026-01-15",
      "aoi": [33.0, -5.0, 42.0, 5.0],
      "aggregation": "mean"
    }
  }' | jq '{jobId, status, rows: .outputs.rows}'

# 3) Temporal aggregation (daily -> monthly mean) for two parameters
curl -s -X POST "$BASE/processes/data.temporal_aggregate/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "era5-land-daily",
      "params": ["2m_temperature", "total_precipitation"],
      "time": "2026-01-01",
      "frequency": "P1M",
      "aggregation": "mean"
    }
  }' | jq '{jobId, status, rows: .outputs.rows}'
```

## DHIS2 Org Units → CHIRPS Pipeline

This is the primary operational pattern: use DHIS2 administrative boundaries
as the area of interest (AOI) for CHIRPS3 precipitation extraction and get
aggregated values back per district.

```text
GET  /features/dhis2-org-units/items   →  org unit GeoJSON polygons
                                            (live from DHIS2 or static fallback)
→  derive bbox from polygon geometry   →  [minx, miny, maxx, maxy]
POST /processes/raster.zonal_stats/execution   →  CHIRPS3 precip for that bbox
GET  /jobs/{jobId}                     →  result with mm value per district
```

The process `aoi` input accepts a bbox array `[minx, miny, maxx, maxy]`.
Any DHIS2 polygon can be converted to a bbox with a one-line `jq` expression.

> **DHIS2 connected?** Set `EOAPI_DHIS2_BASE_URL` + `EOAPI_DHIS2_TOKEN` (or
> `EOAPI_DHIS2_USERNAME` / `EOAPI_DHIS2_PASSWORD`) and the features endpoint
> returns your live organisation unit tree. Without these vars the API falls
> back to three static Sierra Leone districts.

---

### Step 1 — browse org unit features

```bash
BASE="http://127.0.0.1:8000"

# List Level 2 org units with their bounding boxes
curl -s "$BASE/features/dhis2-org-units/items?level=2" | jq '
  [.features[] | {
    id:   .id,
    name: .properties.name,
    bbox: (
      .geometry.coordinates[0]
      | [(map(.[0]) | min), (map(.[1]) | min),
         (map(.[0]) | max), (map(.[1]) | max)]
    )
  }]
'
```

Expected (static fallback — Sierra Leone Level 2 districts):

```json
[
  { "id": "O6uvpzGd5pu", "name": "Bo",      "bbox": [-11.64, 8.42, -11.50, 8.55] },
  { "id": "fdc6uOvgoji", "name": "Bombali", "bbox": [-13.30, 8.80, -13.10, 9.00] },
  { "id": "lc3eMKXaEfw", "name": "Bonthe",  "bbox": [-12.40, 7.00, -12.10, 7.25] }
]
```

---

### Step 2 — run CHIRPS3 for a single district

```bash
BASE="http://127.0.0.1:8000"

# 1. Fetch the Bo district feature
BO=$(curl -s "$BASE/features/dhis2-org-units/items?level=2" \
  | jq '.features[] | select(.properties.name=="Bo")')

# 2. Derive bbox [minx, miny, maxx, maxy] from the polygon ring
BO_BBOX=$(echo "$BO" | jq '
  .geometry.coordinates[0]
  | [(map(.[0]) | min), (map(.[1]) | min),
     (map(.[0]) | max), (map(.[1]) | max)]
')

echo "Bo bbox: $BO_BBOX"
# → [-11.64, 8.42, -11.50, 8.55]

# 3. Run zonal stats — full result inline, no polling
curl -s -X POST "$BASE/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --argjson aoi "$BO_BBOX" '{
    inputs: {
      dataset_id:  "chirps-daily",
      params:      ["precip"],
      time:        "2026-01-31",
      aoi:         $aoi,
      aggregation: "mean"
    }
  }')" | jq '{
    jobId,
    district: .outputs.rows[0].dataset_id,
    aoi_bbox:  .outputs.rows[0].aoi_bbox,
    value_mm:  .outputs.rows[0].value,
    status:    .outputs.rows[0].status
  }'
```

Expected:

```json
{
  "jobId":    "a1b2c3d4-…",
  "district": "chirps-daily",
  "aoi_bbox": [-11.64, 8.42, -11.50, 8.55],
  "value_mm": 3.2,
  "status":   "computed"
}
```

> `value_mm` is `null` with `status: "missing_assets"` until the CHIRPS3
> provider has downloaded and cached the raster for that date. The file
> downloads automatically on the first call; subsequent calls for the same
> date read from cache.

---

### Batch: all districts, one date

Loop over every Level 2 org unit, run the process, print a table.

```bash
BASE="http://127.0.0.1:8000"
DATE="2026-01-31"

# Fetch all Level 2 org units
ORG_UNITS=$(curl -s "$BASE/features/dhis2-org-units/items?level=2" | jq '.features')

# Loop — one process call per district
echo "$ORG_UNITS" | jq -c '.[]' | while read -r feature; do
  name=$(echo "$feature" | jq -r '.properties.name')
  bbox=$(echo "$feature" | jq '
    .geometry.coordinates[0]
    | [(map(.[0]) | min), (map(.[1]) | min),
       (map(.[0]) | max), (map(.[1]) | max)]
  ')
  value=$(curl -s -X POST "$BASE/processes/raster.zonal_stats/execution" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --argjson aoi "$bbox" --arg date "$DATE" '{
      inputs: {dataset_id:"chirps-daily",params:["precip"],time:$date,aoi:$aoi}
    }')" | jq '.outputs.rows[0].value')
  echo "$name  $value mm"
done
```

Expected:

```text
Bo       3.2 mm
Bombali  5.8 mm
Bonthe   7.1 mm
```

---

### Monthly aggregate per district (P1M)

Instead of a single-day snapshot, request a monthly total. The `data.temporal_aggregate`
process fetches all daily files for the month (Jan 1–31 for `P1M`) and
aggregates them.

```bash
BASE="http://127.0.0.1:8000"

# Bo district bbox
BO_BBOX="[-11.64, 8.42, -11.50, 8.55]"

curl -s -X POST "$BASE/processes/data.temporal_aggregate/execution" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --argjson aoi "$BO_BBOX" '{
    inputs: {
      dataset_id:  "chirps-daily",
      params:      ["precip"],
      time:        "2026-01-01",
      aoi:         $aoi,
      frequency:   "P1M",
      aggregation: "sum"
    }
  }')" | jq '{
    jobId,
    operation:    .outputs.rows[0].operation,
    frequency:    .outputs.rows[0].target_frequency,
    aggregation:  .outputs.rows[0].aggregation,
    sample_count: .outputs.rows[0].sample_count,
    value_mm:     .outputs.rows[0].value,
    status:       .outputs.rows[0].status
  }'
```

Expected:

```json
{
  "jobId":        "b2c3d4e5-…",
  "operation":    "temporal_aggregate",
  "frequency":    "P1M",
  "aggregation":  "sum",
  "sample_count": 31,
  "value_mm":     98.4,
  "status":       "computed"
}
```

---

### What the `dhis2` output contains

Every process job includes a `dhis2` output field with a stub payload shaped
like a DHIS2 `dataValueSets` import request. This is the structure that will
be POSTed back to DHIS2 once the import step is wired:

```bash
curl -s "$BASE/jobs/$JOB_ID" | jq '.outputs.dhis2'
```

```json
{
  "status": "stub",
  "dataValueSets": {
    "dataValues": []
  },
  "note": "DHIS2 import payload formatting is a stub. Wire dhis2eo.integrations to populate dataValues."
}
```

---

## Landing page runtime summary

Operator-focused view:

```bash
curl "http://127.0.0.1:8000/" | jq '.runtime'
```

Example runtime payload:

```json
{
	"cors": {
		"mode": "wildcard",
		"origins": 1
	},
	"apiKeyRequired": false,
	"dhis2": {
		"configured": false,
		"host": "unset",
		"authMode": "none"
	},
	"state": {
		"persistenceEnabled": true,
		"directory": ".cache/state"
	},
	"internalScheduler": {
		"enabled": true
	}
}
```

## Collections (`/collections`)

Conformance declaration:

http://127.0.0.1:8000/conformance

```bash
curl "http://127.0.0.1:8000/conformance"
```

Note: values used in `range-subset` and `parameter-name` must match keys in `eoapi/datasets/<dataset-id>/<dataset-id>.yaml` under `parameters`.

List collections:

http://127.0.0.1:8000/collections

```bash
curl "http://127.0.0.1:8000/collections"
```

Get CHIRPS collection:

http://127.0.0.1:8000/collections/chirps-daily

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily"
```

Get ERA5-Land collection:

http://127.0.0.1:8000/collections/era5-land-daily

Collections in this section correspond to OGC API - Common collection discovery endpoints.

Get CHIRPS coverage (default extent/time):

http://127.0.0.1:8000/collections/chirps-daily/coverage

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/coverage"
```

Get CHIRPS coverage for a specific datetime and bbox:

http://127.0.0.1:8000/collections/chirps-daily/coverage?datetime=2026-01-31T00:00:00Z&bbox=30,-5,35,2

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/coverage?datetime=2026-01-31T00:00:00Z&bbox=30,-5,35,2"
```

Get ERA5-Land coverage for a range-subset parameter:

http://127.0.0.1:8000/collections/era5-land-daily/coverage?range-subset=2m_temperature

```bash
curl "http://127.0.0.1:8000/collections/era5-land-daily/coverage?range-subset=2m_temperature"
```

Get CHIRPS EDR position query:

http://127.0.0.1:8000/collections/chirps-daily/position?coords=POINT(30%20-1)&datetime=2026-01-31T00:00:00Z&parameter-name=precip

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/position?coords=POINT(30%20-1)&datetime=2026-01-31T00:00:00Z&parameter-name=precip"
```

Get ERA5-Land EDR position query:

http://127.0.0.1:8000/collections/era5-land-daily/position?coords=POINT(36.8%20-1.3)&parameter-name=2m_temperature

```bash
curl "http://127.0.0.1:8000/collections/era5-land-daily/position?coords=POINT(36.8%20-1.3)&parameter-name=2m_temperature"
```

Get CHIRPS EDR area query:

http://127.0.0.1:8000/collections/chirps-daily/area?bbox=30,-5,35,2&datetime=2026-01-31T00:00:00Z&parameter-name=precip

```bash
curl "http://127.0.0.1:8000/collections/chirps-daily/area?bbox=30,-5,35,2&datetime=2026-01-31T00:00:00Z&parameter-name=precip"
```

Get ERA5-Land EDR area query:

http://127.0.0.1:8000/collections/era5-land-daily/area?bbox=36,-2,38,0&parameter-name=2m_temperature

```bash
curl "http://127.0.0.1:8000/collections/era5-land-daily/area?bbox=36,-2,38,0&parameter-name=2m_temperature"
```

## Features (`/features`)

List feature collections:

http://127.0.0.1:8000/features

```bash
curl "http://127.0.0.1:8000/features"
```

Get DHIS2 org unit features (level 2):

http://127.0.0.1:8000/features/dhis2-org-units/items?level=2

```bash
curl "http://127.0.0.1:8000/features/dhis2-org-units/items?level=2"
```

Get DHIS2 org unit features filtered by bbox:

http://127.0.0.1:8000/features/dhis2-org-units/items?level=2&bbox=-13,8,-11,9

```bash
curl "http://127.0.0.1:8000/features/dhis2-org-units/items?level=2&bbox=-13,8,-11,9"
```

## Processes (`/processes`)

List processes:

http://127.0.0.1:8000/processes

```bash
curl "http://127.0.0.1:8000/processes"
```

Describe zonal-stats process:

http://127.0.0.1:8000/processes/raster.zonal_stats

```bash
curl "http://127.0.0.1:8000/processes/raster.zonal_stats"
```

Run zonal-stats process:

http://127.0.0.1:8000/processes/raster.zonal_stats/execution

```bash
curl -X POST "http://127.0.0.1:8000/processes/raster.zonal_stats/execution" \
	-H "Content-Type: application/json" \
	-d '{
		"inputs": {
			"dataset_id": "chirps-daily",
			"params": ["precip"],
			"time": "2026-01-31",
			"aoi": [30.0, -10.0, 31.0, -9.0],
			"aggregation": "mean"
		}
	}'
```

Run point-timeseries process:

http://127.0.0.1:8000/processes/raster.point_timeseries/execution

```bash
curl -X POST "http://127.0.0.1:8000/processes/raster.point_timeseries/execution" \
	-H "Content-Type: application/json" \
	-d '{
		"inputs": {
			"dataset_id": "chirps-daily",
			"params": ["precip"],
			"time": "2026-01-31",
			"aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]}
		}
	}'
```

Run temporal-aggregate (harmonization) process:

http://127.0.0.1:8000/processes/data.temporal_aggregate/execution

```bash
curl -X POST "http://127.0.0.1:8000/processes/data.temporal_aggregate/execution" \
	-H "Content-Type: application/json" \
	-d '{
		"inputs": {
			"dataset_id": "chirps-daily",
			"params": ["precip"],
			"time": "2026-01-31",
			"frequency": "P1M",
			"aggregation": "sum"
		}
	}'
```

List all jobs (most recent first):

<http://127.0.0.1:8000/jobs>

```bash
curl "http://127.0.0.1:8000/jobs"
```

Filter job list to see only succeeded jobs:

```bash
curl -s "http://127.0.0.1:8000/jobs" | jq '.jobs[] | select(.status=="succeeded") | {jobId, processId, created}'
```

Check job status (replace `<JOB_ID>`):

http://127.0.0.1:8000/jobs/<JOB_ID>

```bash
curl "http://127.0.0.1:8000/jobs/<JOB_ID>"
```

Get aggregated result features from a job (replace `<JOB_ID>`):

http://127.0.0.1:8000/features/aggregated-results/items?jobId=<JOB_ID>

```bash
curl "http://127.0.0.1:8000/features/aggregated-results/items?jobId=<JOB_ID>"
```

Collection-first, process-next (full-circle) quick check:

```bash
# 1) Discover dataset and process links
curl -s "http://127.0.0.1:8000/collections/chirps-daily" | jq '.links[] | select(.rel=="process" or .rel=="process-execute")'

# 2) Execute process for that dataset
JOB_ID=$(curl -s -X POST "http://127.0.0.1:8000/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d '{"inputs":{"dataset_id":"chirps-daily","params":["precip"],"time":"2026-01-31","aoi":[30.0,-10.0,31.0,-9.0]}}' | jq -r '.jobId')

# 3) Inspect computed output
curl -s "http://127.0.0.1:8000/jobs/$JOB_ID" | jq '.outputs.rows, .outputs.implementation'
```

## Workflows (`/workflows`)

Create a custom workflow with two steps:

http://127.0.0.1:8000/workflows

```bash
curl -X POST "http://127.0.0.1:8000/workflows" \
	-H "Content-Type: application/json" \
	-d '{
		"name": "climate-indicators-workflow",
		"steps": [
			{
				"name": "zonal",
				"processId": "raster.zonal_stats",
				"payload": {
					"inputs": {
						"dataset_id": "chirps-daily",
						"params": ["precip"],
						"time": "2026-01-31",
						"aoi": [30.0, -10.0, 31.0, -9.0],
						"aggregation": "mean",
						"frequency": "P1M"
					}
				}
			},
			{
				"name": "timeseries",
				"processId": "raster.point_timeseries",
				"payload": {
					"inputs": {
						"dataset_id": "chirps-daily",
						"params": ["precip"],
						"time": "2026-01-31",
						"aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]}
					}
				}
			}
		]
	}'
```

List workflows:

http://127.0.0.1:8000/workflows

```bash
curl "http://127.0.0.1:8000/workflows"
```

Run a workflow immediately (replace `<WORKFLOW_ID>`):

http://127.0.0.1:8000/workflows/<WORKFLOW_ID>/run

```bash
curl -X POST "http://127.0.0.1:8000/workflows/<WORKFLOW_ID>/run"
```

## Schedules (`/schedules`)

Schedules allow user-defined recurring execution of a process or workflow.

Create a nightly schedule:

http://127.0.0.1:8000/schedules

```bash
curl -X POST "http://127.0.0.1:8000/schedules" \
	-H "Content-Type: application/json" \
	-d '{
		"name": "nightly-zonal-stats",
		"cron": "0 0 * * *",
		"timezone": "UTC",
		"enabled": true,
		"processId": "raster.zonal_stats",
		"inputs": {
			"dataset_id": "chirps-daily",
			"params": ["precip"],
			"time": "2026-01-31",
			"aoi": [30.0, -10.0, 31.0, -9.0],
			"aggregation": "mean",
			"frequency": "P1M"
		}
	}'
```

Create a workflow-target schedule (replace `<WORKFLOW_ID>`):

http://127.0.0.1:8000/schedules

```bash
curl -X POST "http://127.0.0.1:8000/schedules" \
	-H "Content-Type: application/json" \
	-d '{
		"name": "nightly-workflow-run",
		"cron": "0 0 * * *",
		"timezone": "UTC",
		"enabled": true,
		"workflowId": "<WORKFLOW_ID>"
	}'
```

List schedules:

http://127.0.0.1:8000/schedules

```bash
curl "http://127.0.0.1:8000/schedules"
```

Run a schedule immediately (replace `<SCHEDULE_ID>`):

http://127.0.0.1:8000/schedules/<SCHEDULE_ID>/run

```bash
curl -X POST "http://127.0.0.1:8000/schedules/<SCHEDULE_ID>/run"
```

Trigger schedule from orchestrator callback (replace `<SCHEDULE_ID>`):

http://127.0.0.1:8000/schedules/<SCHEDULE_ID>/callback

Requires server env var `EOAPI_SCHEDULER_TOKEN` and header `X-Scheduler-Token`.

```bash
curl -X POST "http://127.0.0.1:8000/schedules/<SCHEDULE_ID>/callback" \
	-H "X-Scheduler-Token: <EOAPI_SCHEDULER_TOKEN>"
```

## COG (`/cog`)

COG info:

http://127.0.0.1:8000/cog/info?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif

```bash
curl "http://127.0.0.1:8000/cog/info?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif"
```

COG preview:

http://127.0.0.1:8000/cog/preview.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&max_size=2048&colormap_name=delta

```bash
curl -o chirps-preview.png "http://127.0.0.1:8000/cog/preview.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&max_size=2048&colormap_name=delta"
```

Tile:

http://127.0.0.1:8000/cog/tiles/WebMercatorQuad/4/5/5.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&colormap_name=delta

```bash
curl -o chirps-tile.png "http://127.0.0.1:8000/cog/tiles/WebMercatorQuad/4/5/5.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&colormap_name=delta"
```

CHIRPS COG test file:

https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/2026/chirps-v3.0.rnl.2026.01.31.tif
