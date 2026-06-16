# DAC 2026 Reference

This page is the public step-by-step reference for the DAC 2026 OCS examples.

Assumptions:

- OCS runs at `http://127.0.0.1:8000`
- Sierra Leone level-2 boundaries are available at
  `docs/dac2026/examples/organisationUnits_sle_level2.json`
- the example time window is `2025-01-01` to `2025-02-28`

## Setup

Clone the workshop branch and start the local instance:

```bash
git clone https://github.com/dhis2/open-climate-service.git
cd open-climate-service
git checkout dac2026
cp climate-service.yaml.example climate-service.yaml
cp .env.example .env
```

Then update `climate-service.yaml` with the country extent you want to use for
the workshop instance, and continue with:

```bash
make sync
make run
```

Validate that the service is running:

```bash
curl -s "$OCS_BASE_URL/extent" | jq
curl -s "$OCS_BASE_URL/collections" | jq -r '.collections[].id'
```

## Shared defaults

```bash
export OCS_BASE_URL="http://127.0.0.1:8000"
export SLE_GEOJSON="docs/dac2026/examples/organisationUnits_sle_level2.json"
export START_DATE="2025-01-01"
export END_DATE="2025-02-28"
export DHIS2_DE_UID="DE_123"
```

## Country bbox

Derive the bbox from a trusted national boundary file.

Examples:

```bash
ogrinfo -al -so country.geojson
```

```bash
uv run python - <<'PY'
import geopandas as gpd
g = gpd.read_file("country.geojson").to_crs(4326)
print(list(g.total_bounds))  # xmin, ymin, xmax, ymax
PY
```

SLE example:

```bash
uv run python - <<'PY'
import geopandas as gpd
g = gpd.read_file("docs/dac2026/examples/organisationUnits_sle_level2.json").to_crs(4326)
print(list(g.total_bounds))  # xmin, ymin, xmax, ymax
PY
```

Quick fallback:

- `https://bboxfinder.com/`

## Publish a dataset

Publish the daily CHIRPS example dataset:

```bash
curl -s -X POST "$OCS_BASE_URL/ingestions" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "start": "2025-01-01",
    "end": "2025-02-28",
    "publish": true
  }' | jq
```

Validate:

```bash
curl -s "$OCS_BASE_URL/collections" | jq -r '.collections[].id'
curl -s "$OCS_BASE_URL/datasets/chirps3_precipitation_daily" | jq '.publication.status, .extent.temporal'
```

## Pre-flight validation

Run these before the examples:

```bash
curl -s "$OCS_BASE_URL/collections" | jq -r '.collections[].id'
curl -s "$OCS_BASE_URL/process_graphs" | jq -r '.processes[].id' | grep -E 'aggregate_to_(chap_csv|dhis2_json)'
curl -s "$OCS_BASE_URL/file_formats" | jq -r '.output | keys[]'
jq -r '.features[:5][] | .id' "$SLE_GEOJSON"
```

Validate:

- intended dataset ids are published
- `aggregate_to_chap_csv` exists
- `aggregate_to_dhis2_json` exists
- `CSV`, `CHAPCSV`, and `DHIS2JSON` are listed
- GeoJSON feature ids are real, not `null`

## Service surface

Before using the example payloads, you can inspect:

- `/manage`
- `/collections`
- `/map`
- `/stac`

## Example 1: monthly CSV from daily precipitation

### Payload

```json
{
  "process": {
    "process_graph": {
      "load": {
        "process_id": "load_collection",
        "arguments": {
          "id": "chirps3_precipitation_daily",
          "temporal_extent": ["2025-01-01", "2025-02-28"]
        }
      },
      "monthly": {
        "process_id": "aggregate_temporal_period",
        "arguments": {
          "data": {"from_node": "load"},
          "period": "month",
          "reducer": {
            "process_graph": {
              "sum": {
                "process_id": "sum",
                "arguments": {"data": {"from_parameter": "data"}},
                "result": true
              }
            }
          }
        }
      },
      "save": {
        "process_id": "save_result",
        "arguments": {
          "data": {"from_node": "monthly"},
          "format": "CSV"
        },
        "result": true
      }
    }
  }
}
```

The `temporal_extent` in `load_collection` selects the part of the dataset to
load before resampling.

### Run

```bash
ls docs/dac2026/examples/demo2-temporal-resampling.json

curl -s -X POST "$OCS_BASE_URL/result" \
  -H "Content-Type: application/json" \
  --data @docs/dac2026/examples/demo2-temporal-resampling.json > /tmp/dac-example-1.csv
```

This command creates `/tmp/dac-example-1.csv`.

### Expected result

Typical shape:

```text
y,x,t,precip
10.02499925531447,-13.52499751932919,2025-01-31,0.0
10.02499925531447,-13.52499751932919,2025-02-28,0.51207256
```

### Validation

```bash
head /tmp/dac-example-1.csv
wc -l /tmp/dac-example-1.csv
```

## Example 2: temporal resampling

### Payload

Use the checked-in request file:

- `docs/dac2026/examples/demo2-temporal-resampling.json`

### Run

```bash
ls docs/dac2026/examples/demo2-temporal-resampling.json

curl -s -X POST "$OCS_BASE_URL/result" \
  -H "Content-Type: application/json" \
  --data @docs/dac2026/examples/demo2-temporal-resampling.json > /tmp/dac-example-2.csv
```

This command creates `/tmp/dac-example-2.csv`.

### Expected result

Typical shape:

```text
y,x,t,precip
10.02499925531447,-13.52499751932919,2025-01-31,0.0
10.02499925531447,-13.52499751932919,2025-02-28,0.51207256
```

The output only contains monthly rows for the requested `temporal_extent`.

### Validation

```bash
head /tmp/dac-example-2.csv
cut -d, -f1-4 /tmp/dac-example-2.csv | sed -n '1,8p'
```

## Example 3: DHIS2 JSON export

### Run

```bash
ls docs/dac2026/examples/demo3-dhis2json-precip-monthly.json

curl -s -X POST "$OCS_BASE_URL/result" \
  -H "Content-Type: application/json" \
  --data @docs/dac2026/examples/demo3-dhis2json-precip-monthly.json > /tmp/dac-example-3-response.json
```

This command creates `/tmp/dac-example-3-response.json`.

### Expected result

Typical shape:

```json
{
  "dataValues": [
    {
      "dataElement": "DE_123",
      "orgUnit": "O6uvpzGd5pu",
      "period": "202501",
      "value": "12.44626"
    }
  ]
}
```

### Validation

```bash
jq '.dataValues[:5]' /tmp/dac-example-3-response.json
```

## Example 4: CHAP CSV export

### Run

```bash
curl -s -X POST "$OCS_BASE_URL/result" \
  -H "Content-Type: application/json" \
  --data @docs/dac2026/examples/demo4-chapcsv-precip-temp-monthly.json > /tmp/dac-example-4.csv
```

This command creates `/tmp/dac-example-4.csv`.

### Expected result

Typical shape:

```text
time_period,location,mean_temperature,rainfall
202501,O6uvpzGd5pu,28.282999,12.44626
202501,PMa2VCrupOd,28.253462,0.95707685
```

### Validation

```bash
head /tmp/dac-example-4.csv
cut -d, -f1-4 /tmp/dac-example-4.csv | sed -n '1,8p'
```

## If the GeoJSON has bad ids

Stop and fix the file first.

Do not proceed with CHAP or DHIS2 examples until:

- feature ids are present
- ids are stable
- ids are meaningful for the session

## Troubleshooting

When examples fail, classify the problem early:

1. service not running
2. dataset not published
3. geometry ids missing or invalid
4. payload shape incorrect
5. provider or auth issue
