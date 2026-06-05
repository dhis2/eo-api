# openEO DHIS2DVSJSON Export Design

## Context

Open Climate Service is moving toward openEO as the primary execution surface.
Analytical workflows are increasingly expected to run through process graphs and
`save_result`, with reusable UDPs and asynchronous jobs layered on top.

Issue [#175](https://github.com/dhis2/open-climate-service/issues/175) adds
`DHIS2DVSJSON` as an openEO export format. The goal is to let users produce
DHIS2-ready import payloads directly from an openEO workflow, rather than
introducing a separate export API outside the openEO model.

This note defines the intended design before implementation.

## Goal

Support workflows of the form:

```text
load_collection -> aggregate_spatial -> save_result(format="DHIS2DVSJSON", options=...)
```

where:

- upstream processes produce the analytical result
- spatial aggregation to DHIS2 organisation units happens before export
- `save_result` is responsible only for serialization into DHIS2 data value JSON

## Non-goals

- building a DHIS2 import client
- moving aggregation logic into the export writer
- introducing a separate non-openEO export endpoint
- supporting arbitrary schema mapping in the first implementation

## Export semantics

`DHIS2DVSJSON` should behave like any other openEO export format:

- synchronous `POST /result`
  - returns the actual serialized DHIS2 JSON payload
- batch `POST /jobs`
  - persists the result as a downloadable artifact

This keeps `save_result` semantics consistent across execution modes:

- sync returns the requested export immediately
- batch writes the requested export for later download

## Expected output shape

The output must match the DHIS2 data value import envelope:

```json
{
  "dataValues": [
    {
      "dataElement": "BXgDHhPdFVU",
      "orgUnit": "O6uvpzGd5pu",
      "period": "202401",
      "value": "42.5"
    }
  ]
}
```

Required fields per item:

- `dataElement`
- `orgUnit`
- `period`
- `value`

Optional fields in the first implementation:

- `categoryOptionCombo`

Explicitly out of scope for the first implementation:

- `attributeOptionCombo`

## openEO contract

### Format name

The format identifier exposed through `save_result` should be:

- `DHIS2DVSJSON`

This name is intentionally shorter than `DHIS2DataValueSetJSON` while still
making the payload type explicit:

- `DHIS2`
- `DVS` = Data Value Set
- `JSON`

### save_result options

Initial required/optional options:

- required: `data_element_id`
- required: `org_unit_field`
- optional: `period_field`
- required for date-like periods: `period_type`
- optional: `category_option_combo`

These are intentionally narrow. The first implementation should assume the
standard output shape produced by upstream aggregation instead of introducing
general-purpose schema mapping.

The first implementation should make the default field assumptions explicit:

- `org_unit_field` identifies the field that contains the DHIS2 org unit UID
- `period_field` defaults to `t`
- `period_type` must be supplied when the period column contains date-like values
  such as timestamps rather than already formatted DHIS2 period strings

The writer should not attempt to infer the org unit field automatically from an
arbitrary schema.

If later needed, broader mapping-oriented options can be added explicitly.

## Input assumptions

The first implementation should assume a standard aggregated result shape rather
than an arbitrary input table.

Expected logical fields after upstream processing:

- one DHIS2 org unit identifier per row/feature
- one time period per row/feature
- one numeric value per row/feature

This means the writer should be applied after `aggregate_spatial`, not before.

Default field assumptions for the first implementation:

- org unit UID comes from the field named by `org_unit_field`
- period value comes from `period_field`, defaulting to `t`
- the exported value comes from the single analytical variable selected for
  `save_result`

## Writer design

The writer should live below the route layer and be reusable from both sync and
batch result paths. Its job is to transform an already computed analytical
result into the DHIS2 data value envelope, not to perform aggregation or data
selection.

### Preferred boundary

Treat `DHIS2DVSJSON` as a custom openEO export writer, not just another generic
vector file format.

Reason:

- it is domain-specific
- it serializes into a DHIS2 envelope, not a generic geospatial exchange format
- `CHAPCSV` is coming next and has similar “domain export” characteristics

This suggests either:

- extending the existing export registry so custom tabular/vector writers can be
  registered cleanly, or
- introducing a dedicated custom-export registry used by both sync and batch
  result paths

The important part is to avoid hardcoding one-off format logic directly into the
route layer.

## Data model choice

There are two plausible writer inputs:

### Option A: GeoDataFrame input

Pros:

- fits naturally with the current vector export path
- likely simplest for narrow `DHIS2DVSJSON` rows

Cons:

- less natural for the multi-variable wide export needed by `CHAPCSV`

### Option B: xarray Dataset input

Pros:

- closer to the native analytical result model
- better long-term fit for `CHAPCSV`

Cons:

- requires extra reshaping for `DHIS2DVSJSON`

### Recommended approach

For `#175`, allow the implementation to consume the current post-aggregation
shape that is easiest to serialize, but structure helper functions so they can
later support both:

- narrow DHIS2 row serialization
- wide CHAP CSV serialization

The shared logic should live below the route layer.

## Shared helper requirements

Two helper concerns should be shared because `CHAPCSV` will need them next:

### 1. DHIS2 period formatting

Convert result time values into DHIS2 period strings:

- daily -> `YYYYMMDD`
- weekly -> `YYYYWnn`
- monthly -> `YYYYMM`
- quarterly -> `YYYYQn`
- yearly -> `YYYY`

This should be a reusable helper, not embedded only in `_write_dhis2_json()`.

If the input granularity cannot be represented as a DHIS2 period string, the
writer should raise a client-facing error rather than applying a fallback
silently.

Because monthly, quarterly, and yearly aggregates are ambiguous when represented
as ordinary timestamps, the first implementation should accept an explicit
`period_type` option instead of relying on inference.

### 2. Value string formatting

Values must be emitted as strings, without scientific notation.

Examples:

- `42` -> `"42"`
- `42.5` -> `"42.5"`
- `1e-05` -> `"0.00001"`

This should also be shared, because the export layer should define one canonical
stringification policy for downstream system formats.

### 3. Null value handling

Missing values should not be emitted as DHIS2 data values.

Policy for the first implementation:

- rows with `None`, `NaN`, or null-equivalent values are omitted from the
  `dataValues` array

## Sync and batch integration

Implementation should cover both execution modes.

### Synchronous path

In `POST /result`:

- `save_result(format="DHIS2DVSJSON")` should return the serialized JSON payload
- response media type should be `application/json`

### Batch path

In job persistence:

- result should be written to a downloadable file named `result.json`
- result asset metadata should expose the correct media type
- file content is UTF-8 encoded JSON

The same serialization logic should be reused across both paths.

## Organisation unit geometry sourcing

`DHIS2DVSJSON` should not be responsible for fetching or inferring DHIS2
organisation unit boundaries. It consumes the output of upstream spatial
aggregation and serializes that result into the DHIS2 data value envelope.

That keeps two concerns separate:

- `aggregate_spatial`
  - computes zonal statistics for supplied polygons
- `DHIS2DVSJSON`
  - serializes already aggregated rows into DHIS2 JSON

### Pragmatic first step

For `#175`, organisation unit sourcing stays out of scope. The implementation
should assume the process graph already has the correct polygons when it calls
`aggregate_spatial`.

The most pragmatic way to validate that flow end-to-end later is:

- provide a real DHIS2 org unit GeoJSON file
- feed that GeoJSON into `aggregate_spatial`
- export the resulting rows with `save_result(format="DHIS2DVSJSON")`

This gives a realistic system test without coupling the initial export work to
live DHIS2 API access, credentials, or backend-specific fetch helpers.

### Possible future sources of org unit geometry

These are valid future directions, but not part of `#175`:

- GeoJSON file input
  - simplest and most reproducible
- inline GeoJSON / FeatureCollection in the process request
  - convenient for API-driven workflows
- DHIS2 API fetch
  - better UX later, but introduces auth, config, caching, and failure handling
- backend-specific UDP / helper process
  - e.g. a process that resolves DHIS2 org units to GeoJSON before aggregation

## Error handling

The first implementation should fail clearly when required export options are
missing.

Examples:

- missing `data_element_id`
  - return a structured client error
- missing `period_type` for date-like period values
  - return a structured client error
- unsupported or ambiguous period representation
  - return a structured client error
- writer produces no output in batch mode
  - treat as internal export failure

Avoid silently falling back to generic result summaries when a requested export
format cannot be produced.

## Testing expectations

Minimum test coverage for `#175`:

### Serialization helpers

- daily period -> `YYYYMMDD`
- weekly period -> `YYYYWnn`
- monthly period -> `YYYYMM`
- quarterly period -> `YYYYQn`
- yearly period -> `YYYY`
- numeric values are rendered as strings without scientific notation
- null-valued rows are omitted

### Batch export

- persisted `DHIS2DVSJSON` result writes `result.json`
- downloadable asset returns `application/json`

### Synchronous export

- `POST /result` returns the actual DHIS2 data value set JSON payload
- missing `data_element_id` returns a client error
- missing `org_unit_field` returns a client error
- missing `period_type` for date-like values returns a client error
- `categoryOptionCombo` is passed through when provided

### Staged validation strategy

The initial implementation does not need live DHIS2-backed end-to-end tests to
be correct.

Recommended staged validation:

1. unit and route tests
   - use small synthetic post-aggregation fixtures
   - prove `DHIS2DVSJSON` serialization correctness in sync and batch modes
2. later end-to-end test
   - feed a proper DHIS2 org unit GeoJSON file into `aggregate_spatial`
   - confirm the exported `DHIS2DVSJSON` payload is valid and structurally ready
     for DHIS2 import

This keeps `#175` focused while preserving a concrete path to realistic
workflow validation.

## Relationship to CHAPCSV

Issue [#176](https://github.com/dhis2/open-climate-service/issues/176) should
build on the same export-layer design.

Recommended order:

1. implement `DHIS2JSON`
2. extract any period/value formatting helpers needed for export-layer reuse
3. implement `CHAPCSV` on top of those helpers

This keeps `DHIS2DVSJSON` intentionally narrow while creating a clean path for the
broader wide-format export that follows.

## Proposed first implementation scope

1. Add `DHIS2DVSJSON` as a supported openEO export format.
2. Add shared helpers for DHIS2 period formatting and value stringification.
3. Implement `_write_dhis2_json(...)` in the export/persistence layer.
4. Wire synchronous `/result` to return the same payload directly.
5. Wire batch jobs to persist the same payload as a downloadable file.
6. Add focused tests for helper behavior and both result paths.

## Open questions

These should remain visible, but the export-registry question should preferably
be resolved before `CHAPCSV` lands so domain exports do not get locked into a
weak abstraction.

- Should multi-variable `DHIS2DVSJSON` ever be supported, or should it remain
  single-data-element by design?
- Should custom export formats live inside `_VECTOR_FORMATS`, or should a more
  explicit custom export registry be introduced before `CHAPCSV` lands?
