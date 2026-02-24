# API Examples

Base URL (local):

http://127.0.0.1:8000

OGC landing page:

http://127.0.0.1:8000/

```bash
curl "http://127.0.0.1:8000/"
```

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

Describe aggregate-import process:

http://127.0.0.1:8000/processes/eo-aggregate-import

```bash
curl "http://127.0.0.1:8000/processes/eo-aggregate-import"
```

Execute aggregate-import process (dry-run):

http://127.0.0.1:8000/processes/eo-aggregate-import/execution

```bash
curl -X POST "http://127.0.0.1:8000/processes/eo-aggregate-import/execution" \
	-H "Content-Type: application/json" \
	-d '{
		"inputs": {
			"datasetId": "chirps-daily",
			"parameters": ["precip"],
			"datetime": "2026-01-31T00:00:00Z",
			"orgUnitLevel": 2,
			"aggregation": "mean",
			"dhis2": {
				"dataElementId": "<INSERT-DATA-ELEMENT-ID>",
				"dryRun": true
			}
		}
	}'
```

Execute xclim CDD process (dry-run):

http://127.0.0.1:8000/processes/xclim-cdd/execution

```bash
curl -X POST "http://127.0.0.1:8000/processes/xclim-cdd/execution" \
	-H "Content-Type: application/json" \
	-d '{
		"inputs": {
			"datasetId": "chirps-daily",
			"parameter": "precip",
			"start": "2026-01-01",
			"end": "2026-01-31",
			"orgUnitLevel": 2,
			"threshold": { "value": 1.0, "unit": "mm/day" },
			"dhis2": {
				"dataElementId": "<INSERT-CDD-DATA-ELEMENT-ID>",
				"dryRun": true
			}
		}
	}'
```

Execute xclim warm-days process (dry-run):

http://127.0.0.1:8000/processes/xclim-warm-days/execution

```bash
curl -X POST "http://127.0.0.1:8000/processes/xclim-warm-days/execution" \
	-H "Content-Type: application/json" \
	-d '{
		"inputs": {
			"datasetId": "era5-land-daily",
			"parameter": "2m_temperature",
			"start": "2026-01-01",
			"end": "2026-01-31",
			"orgUnitLevel": 2,
			"threshold": { "value": 35.0, "unit": "degC" },
			"dhis2": {
				"dataElementId": "<INSERT-WARM-DAYS-DATA-ELEMENT-ID>",
				"dryRun": true
			}
		}
	}'
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
				"name": "aggregate-precip",
				"processId": "eo-aggregate-import",
				"payload": {
					"inputs": {
						"datasetId": "chirps-daily",
						"parameters": ["precip"],
						"datetime": "2026-01-31T00:00:00Z",
						"orgUnitLevel": 2,
						"aggregation": "mean",
						"dhis2": {
							"dataElementId": "<INSERT-DATA-ELEMENT-ID>",
							"dryRun": true
						}
					}
				}
			},
			{
				"name": "cdd",
				"processId": "xclim-cdd",
				"payload": {
					"inputs": {
						"datasetId": "chirps-daily",
						"parameter": "precip",
						"start": "2026-01-01",
						"end": "2026-01-31",
						"orgUnitLevel": 2,
						"threshold": { "value": 1.0, "unit": "mm/day" },
						"dhis2": {
							"dataElementId": "<INSERT-CDD-DATA-ELEMENT-ID>",
							"dryRun": true
						}
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

Schedules allow user-defined recurring execution of `eo-aggregate-import` (for example nightly imports).

Create a nightly schedule:

http://127.0.0.1:8000/schedules

```bash
curl -X POST "http://127.0.0.1:8000/schedules" \
	-H "Content-Type: application/json" \
	-d '{
		"name": "nightly-precip-import",
		"cron": "0 0 * * *",
		"timezone": "UTC",
		"enabled": true,
		"inputs": {
			"datasetId": "chirps-daily",
			"parameters": ["precip"],
			"datetime": "2026-01-31T00:00:00Z",
			"orgUnitLevel": 2,
			"aggregation": "mean",
			"dhis2": {
				"dataElementId": "<INSERT-DATA-ELEMENT-ID>",
				"dryRun": true
			}
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
