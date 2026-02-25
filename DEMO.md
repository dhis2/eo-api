# End-to-End Demo: DHIS2 Org Unit → CHIRPS → DHIS2 dataValueSet

A single, runnable walkthrough of the full pipeline that touches every OGC API
building block:

```text
[OGC Features API]   GET /features/dhis2-org-units/items
                         ↓  org unit geometry + bbox
[OGC Collections API] GET /collections/chirps-daily
                         ↓  discover process-execute link
[OGC Processes API]   GET /processes / GET /processes/{id}
                         ↓  inspect inputs schema
[OGC Processes API]   POST /processes/{id}/execution
                         ↓  synchronous job result
[OGC Jobs API]        GET /jobs/{jobId}
                         ↓  retrieve persisted result
[DHIS2]               POST /api/dataValueSets
```

---

## Prerequisites

```bash
# 1. Server running
uvicorn main:app --reload

# 2. (Optional) Connect a real DHIS2 instance
export EOAPI_DHIS2_BASE_URL="https://your-dhis2.org"
export EOAPI_DHIS2_TOKEN="d2pat_xxxx"   # or EOAPI_DHIS2_USERNAME / EOAPI_DHIS2_PASSWORD

# Without DHIS2 env vars the features endpoint falls back to
# three static Sierra Leone districts — the rest of the demo still works.

BASE="http://127.0.0.1:8000"
```

---

## Step 1 — OGC Features API: fetch DHIS2 org unit geometry

```bash
# OGC Features API — GET /features/{collectionId}/items
# Returns GeoJSON FeatureCollection.
# With DHIS2 configured → live org units via dhis2-python-client
# Without              → three static Level-2 Sierra Leone districts

ORG_UNITS=$(curl -s "$BASE/features/dhis2-org-units/items?level=2")

echo "$ORG_UNITS" | jq '[.features[] | {
  id:   .id,
  name: .properties.name,
  bbox: (
    .geometry.coordinates[0]
    | [(map(.[0]) | min), (map(.[1]) | min),
       (map(.[0]) | max), (map(.[1]) | max)]
  )
}]'
```

Expected output:

```json
[
  { "id": "O6uvpzGd5pu", "name": "Bo",      "bbox": [-11.64, 8.42, -11.50, 8.55] },
  { "id": "fdc6uOvgoji", "name": "Bombali", "bbox": [-13.30, 8.80, -13.10, 9.00] },
  { "id": "lc3eMKXaEfw", "name": "Bonthe",  "bbox": [-12.40, 7.00, -12.10, 7.25] }
]
```

Pick the first district:

```bash
OU_ID=$(echo "$ORG_UNITS"   | jq -r '.features[0].id')
OU_NAME=$(echo "$ORG_UNITS" | jq -r '.features[0].properties.name')
OU_BBOX=$(echo "$ORG_UNITS" | jq '
  .features[0].geometry.coordinates[0]
  | [(map(.[0]) | min), (map(.[1]) | min),
     (map(.[0]) | max), (map(.[1]) | max)]
')

echo "District : $OU_NAME"
echo "DHIS2 ID : $OU_ID"
echo "BBox     : $OU_BBOX"
```

```text
District : Bo
DHIS2 ID : O6uvpzGd5pu
BBox     : [-11.64,8.42,-11.50,8.55]
```

---

## Step 2 — OGC Collections API: discover the CHIRPS dataset

```bash
# OGC Collections API — GET /collections
# Lists all available datasets, each with self-describing links.

curl -s "$BASE/collections" | jq '[.collections[] | {id, title}]'
```

```json
[
  { "id": "chirps-daily",   "title": "CHIRPS Daily Precipitation" },
  { "id": "era5-land-daily","title": "ERA5-Land Daily" }
]
```

Inspect the CHIRPS collection — note the embedded `process-execute` links:

```bash
# OGC Collections API — GET /collections/{collectionId}
# Each collection embeds links to the processes that can operate on it.

curl -s "$BASE/collections/chirps-daily" | jq '{
  id,
  title,
  process_links: [.links[] | select(.rel == "process-execute") | {title, href}]
}'
```

```json
{
  "id": "chirps-daily",
  "title": "CHIRPS Daily Precipitation",
  "process_links": [
    { "title": "Zonal statistics",       "href": "http://127.0.0.1:8000/processes/raster.zonal_stats/execution" },
    { "title": "Point time-series",      "href": "http://127.0.0.1:8000/processes/raster.point_timeseries/execution" },
    { "title": "Temporal aggregation",   "href": "http://127.0.0.1:8000/processes/data.temporal_aggregate/execution" }
  ]
}
```

Capture the zonal-stats execute URL — no hardcoding needed:

```bash
EXEC_HREF=$(curl -s "$BASE/collections/chirps-daily" \
  | jq -r '
    .links[]
    | select(.rel == "process-execute" and (.href | contains("zonal_stats")))
    | .href
  ')

echo "$EXEC_HREF"
# → http://127.0.0.1:8000/processes/raster.zonal_stats/execution
```

---

## Step 3 — OGC Processes API: inspect the process definition

```bash
# OGC Processes API — GET /processes
# Lists all registered processes with summary info.

curl -s "$BASE/processes" | jq '[.processes[] | {id, title, version}]'
```

```json
[
  { "id": "raster.zonal_stats",       "title": "Zonal Statistics",      "version": "1.0.0" },
  { "id": "raster.point_timeseries",  "title": "Point Time-Series",     "version": "1.0.0" },
  { "id": "data.temporal_aggregate",  "title": "Temporal Aggregation",  "version": "1.0.0" }
]
```

Read the full input schema for the process we are about to call:

```bash
# OGC Processes API — GET /processes/{processId}
# Returns the OGC Process Description with inputs/outputs schemas.

curl -s "$BASE/processes/raster.zonal_stats" | jq '{
  id,
  title,
  inputs: (.inputs | keys),
  outputs: (.outputs | keys),
  execute_link: (.links[] | select(.rel == "http://www.opengis.net/def/rel/ogc/1.0/execute") | .href)
}'
```

```json
{
  "id": "raster.zonal_stats",
  "title": "Zonal Statistics",
  "inputs": ["aoi", "aggregation", "dataset_id", "params", "time"],
  "outputs": ["csv", "dhis2", "rows"],
  "execute_link": "http://127.0.0.1:8000/processes/raster.zonal_stats/execution"
}
```

---

## Step 4 — OGC Processes API: execute zonal stats for the district

```bash
# OGC Processes API — POST /processes/{processId}/execution
# Synchronous execution (OGC "respond=document", Prefer: return=representation).
# Returns HTTP 200 with the completed job inline — no polling required.

DATE="2026-01-31"

JOB=$(curl -s -X POST "$EXEC_HREF" \
  -H "Content-Type: application/json" \
  -d "$(jq -n \
    --argjson aoi  "$OU_BBOX" \
    --arg     date "$DATE" \
    '{
      inputs: {
        dataset_id:  "chirps-daily",
        params:      ["precip"],
        time:        $date,
        aoi:         $aoi,
        aggregation: "mean"
      }
    }')")

echo "$JOB" | jq '{
  jobId,
  processId,
  status,
  row: .outputs.rows[0] | { parameter, operation, stat, value, status }
}'
```

Expected result (status 200, full result inline):

```json
{
  "jobId":     "a1b2c3d4-…",
  "processId": "raster.zonal_stats",
  "status":    "succeeded",
  "row": {
    "parameter": "precip",
    "operation": "zonal_stats",
    "stat":      "mean",
    "value":     3.2,
    "status":    "computed"
  }
}
```

> **First call?** `value` will be `null` and row `status` will be
> `"missing_assets"` while the CHIRPS3 provider downloads the raster.
> The file is cached after the first call — re-run the same command and
> you get the computed value immediately.

Capture what you need for the next steps:

```bash
JOB_ID=$(echo "$JOB" | jq -r '.jobId')
VALUE=$(echo "$JOB"  | jq -r '.outputs.rows[0].value')
PARAM=$(echo "$JOB"  | jq -r '.outputs.rows[0].parameter')

echo "Job   : $JOB_ID"
echo "Value : $VALUE mm (mean daily precip)"

---

## Final Demo Guide (Collection -> Process -> Job -> DHIS2 dataValueSet)

This is the recommended end-to-end demo for:

**DHIS2 GeoJSON features in -> DHIS2 dataValueSet out**

```bash
BASE="http://127.0.0.1:8000"
```

### 1) Discover dataset via OGC Collections

```bash
curl -s "$BASE/collections/chirps-daily" | jq '{
  id,
  process_links: [.links[] | select(.rel=="process" or .rel=="process-execute") | {rel, href}]
}'
```

This confirms collection discovery and process routing are linked.

### 2) Discover available processes and confirm pipeline definition

```bash
curl -s "$BASE/processes" | jq '[.processes[] | {id, title}]'
curl -s "$BASE/processes/dhis2.pipeline" | jq '{id, title, inputs: (.inputs|keys), outputs: (.outputs|keys)}'
```

### 3) Execute `dhis2.pipeline` with DHIS2 GeoJSON features

```bash
PIPELINE_JOB=$(curl -s -X POST "$BASE/processes/dhis2.pipeline/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "features": {
        "type": "FeatureCollection",
        "features": [
          {
            "type": "Feature",
            "id": "O6uvpzGd5pu",
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[30.0,-10.0],[31.0,-10.0],[31.0,-9.0],[30.0,-9.0],[30.0,-10.0]]]
            },
            "properties": { "name": "Bo" }
          }
        ]
      },
      "dataset_id": "chirps-daily",
      "params": ["precip"],
      "time": "2026-01-31",
      "aggregation": "mean",
      "data_element": "abc123def45"
    }
  }')

echo "$PIPELINE_JOB" | jq '{jobId, processId, status, summary: .outputs.summary}'
```

### 4) Retrieve job result via OGC Jobs API

```bash
PIPELINE_JOB_ID=$(echo "$PIPELINE_JOB" | jq -r '.jobId')
curl -s "$BASE/jobs/$PIPELINE_JOB_ID" | jq '.outputs'
```

### 5) Extract DHIS2 payload from outputs

```bash
echo "$PIPELINE_JOB" | jq '.outputs.dataValueSet'
```

Expected output shape:

```json
{
  "dataSet": "chirps-daily",
  "period": "20260131",
  "dataValues": [
    {
      "dataElement": "abc123def45",
      "orgUnit": "O6uvpzGd5pu",
      "period": "20260131",
      "value": "7.81",
      "comment": "precip mean"
    }
  ]
}
```

This demonstrates the full chain:

1. OGC Collections for dataset discovery
2. OGC Processes for execution
3. OGC Jobs for retrieval
4. DHIS2-compatible `dataValueSet` as final output
```

---

## Step 5 — OGC Jobs API: retrieve the persisted result

The execution response is also stored in the job store and can be retrieved
independently — useful for auditing, retries, or longer-running jobs.

```bash
# OGC Jobs API — GET /jobs/{jobId}
# Retrieves the stored job record by its ID.

curl -s "$BASE/jobs/$JOB_ID" | jq '{
  jobId,
  processId,
  status,
  created,
  updated,
  value: .outputs.rows[0].value
}'
```

```json
{
  "jobId":     "a1b2c3d4-…",
  "processId": "raster.zonal_stats",
  "status":    "succeeded",
  "created":   "2026-01-31T12:00:00Z",
  "updated":   "2026-01-31T12:00:01Z",
  "value":     3.2
}
```

List all jobs (audit trail):

```bash
# OGC Jobs API — GET /jobs
curl -s "$BASE/jobs" | jq '[.jobs[] | {jobId, processId, status, created}]'
```

---

## Step 6 — Format as DHIS2 dataValueSet

A DHIS2 `dataValueSet` needs four metadata UIDs from your instance:
`dataElement`, `categoryOptionCombo`, `orgUnit` (already captured), `period`.

```bash
# ── Configure for your DHIS2 instance ───────────────────────────────────
DE_UID="rbkr8PL0rwM"    # dataElement UID  (replace with your own)
COC_UID="HllvX50cXC0"   # categoryOptionCombo UID (default COC on most servers)
# ────────────────────────────────────────────────────────────────────────

# DHIS2 daily period = YYYYMMDD
PERIOD=$(echo "$DATE" | tr -d '-')     # → "20260131"

DATA_VALUE_SET=$(jq -n \
  --arg ou  "$OU_ID" \
  --arg pe  "$PERIOD" \
  --arg de  "$DE_UID" \
  --arg coc "$COC_UID" \
  --arg val "$VALUE" \
  --arg par "$PARAM" \
  '{
    dataSet:    "chirps-daily-import",
    period:     $pe,
    orgUnit:    $ou,
    dataValues: [{
      dataElement:         $de,
      categoryOptionCombo: $coc,
      orgUnit:             $ou,
      period:              $pe,
      value:               $val,
      comment:             ("chirps3 " + $par + " mean")
    }]
  }')

echo "$DATA_VALUE_SET"
```

Output:

```json
{
  "dataSet":    "chirps-daily-import",
  "period":     "20260131",
  "orgUnit":    "O6uvpzGd5pu",
  "dataValues": [{
    "dataElement":         "rbkr8PL0rwM",
    "categoryOptionCombo": "HllvX50cXC0",
    "orgUnit":             "O6uvpzGd5pu",
    "period":              "20260131",
    "value":               "3.2",
    "comment":             "chirps3 precip mean"
  }]
}
```

Post to DHIS2 (requires `EOAPI_DHIS2_BASE_URL` configured, or call your DHIS2 directly):

```bash
curl -s -X POST "$EOAPI_DHIS2_BASE_URL/api/dataValueSets" \
  -H "Authorization: Bearer $DHIS2_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$DATA_VALUE_SET" \
  | jq '.importCount'
```

---

## All steps as a single script

Copy-paste this into a terminal with the server running.
Replace the `DE_UID` / `COC_UID` lines with your own DHIS2 metadata.

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:8000"
DATE="2026-01-31"
DE_UID="rbkr8PL0rwM"
COC_UID="HllvX50cXC0"

echo "── Step 1: OGC Features API — org unit geometry ───────"
ORG_UNITS=$(curl -s "$BASE/features/dhis2-org-units/items?level=2")
echo "$ORG_UNITS" | jq '[.features[] | {id, name:.properties.name}]'

OU_ID=$(echo "$ORG_UNITS"   | jq -r '.features[0].id')
OU_NAME=$(echo "$ORG_UNITS" | jq -r '.features[0].properties.name')
OU_BBOX=$(echo "$ORG_UNITS" | jq '
  .features[0].geometry.coordinates[0]
  | [(map(.[0])|min),(map(.[1])|min),(map(.[0])|max),(map(.[1])|max)]')
echo "→ $OU_NAME ($OU_ID)  bbox=$OU_BBOX"

echo ""
echo "── Step 2: OGC Collections API — discover dataset ─────"
EXEC_HREF=$(curl -s "$BASE/collections/chirps-daily" \
  | jq -r '.links[]|select(.rel=="process-execute" and (.href|contains("zonal_stats")))|.href')
echo "→ execute href: $EXEC_HREF"

echo ""
echo "── Step 3: OGC Processes API — inspect process ─────────"
curl -s "$BASE/processes/raster.zonal_stats" \
  | jq '{id, title, inputs: (.inputs|keys)}'

echo ""
echo "── Step 4: OGC Processes API — execute (sync) ──────────"
JOB=$(curl -s -X POST "$EXEC_HREF" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --argjson aoi "$OU_BBOX" --arg date "$DATE" \
    '{inputs:{dataset_id:"chirps-daily",params:["precip"],time:$date,aoi:$aoi,aggregation:"mean"}}')")

JOB_ID=$(echo "$JOB" | jq -r '.jobId')
VALUE=$(echo "$JOB"  | jq -r '.outputs.rows[0].value')
STATUS=$(echo "$JOB" | jq -r '.outputs.rows[0].status')
echo "→ job=$JOB_ID  value=${VALUE}mm  row_status=$STATUS"

echo ""
echo "── Step 5: OGC Jobs API — retrieve persisted result ────"
curl -s "$BASE/jobs/$JOB_ID" | jq '{jobId, status, value:.outputs.rows[0].value}'

echo ""
echo "── Step 6: DHIS2 dataValueSet ──────────────────────────"
PERIOD=$(echo "$DATE" | tr -d '-')
DATA_VALUE_SET=$(jq -n \
  --arg ou "$OU_ID" --arg pe "$PERIOD" \
  --arg de "$DE_UID" --arg coc "$COC_UID" --arg val "$VALUE" \
  '{dataSet:"chirps-daily-import",period:$pe,orgUnit:$ou,
    dataValues:[{dataElement:$de,categoryOptionCombo:$coc,
                 orgUnit:$ou,period:$pe,value:$val}]}')
echo "$DATA_VALUE_SET" | jq .

# Uncomment to push to DHIS2:
# curl -s -X POST "$EOAPI_DHIS2_BASE_URL/api/dataValueSets" \
#   -H "Authorization: Bearer $DHIS2_TOKEN" \
#   -H "Content-Type: application/json" \
#   -d "$DATA_VALUE_SET" | jq '.importCount'
```

---

## Batch variant: all districts in one pass

```bash
BASE="http://127.0.0.1:8000"
DATE="2026-01-31"
DE_UID="rbkr8PL0rwM"
COC_UID="HllvX50cXC0"
PERIOD=$(echo "$DATE" | tr -d '-')

EXEC_HREF=$(curl -s "$BASE/collections/chirps-daily" \
  | jq -r '.links[]|select(.rel=="process-execute" and (.href|contains("zonal_stats")))|.href')

curl -s "$BASE/features/dhis2-org-units/items?level=2" \
  | jq -c '.features[]' \
  | while read -r feature; do

  ou_id=$(echo "$feature"   | jq -r '.id')
  ou_name=$(echo "$feature" | jq -r '.properties.name')
  bbox=$(echo "$feature" | jq '
    .geometry.coordinates[0]
    | [(map(.[0])|min),(map(.[1])|min),(map(.[0])|max),(map(.[1])|max)]')

  job=$(curl -s -X POST "$EXEC_HREF" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --argjson aoi "$bbox" --arg date "$DATE" \
      '{inputs:{dataset_id:"chirps-daily",params:["precip"],time:$date,aoi:$aoi,aggregation:"mean"}}')")

  value=$(echo "$job" | jq -r '.outputs.rows[0].value')
  job_id=$(echo "$job" | jq -r '.jobId')

  echo "$ou_name ($ou_id): ${value} mm  [job: $job_id]"

  jq -n \
    --arg ou "$ou_id" --arg pe "$PERIOD" \
    --arg de "$DE_UID" --arg coc "$COC_UID" --arg val "$value" \
    '{dataElement:$de,categoryOptionCombo:$coc,orgUnit:$ou,period:$pe,value:$val}'

done
```
