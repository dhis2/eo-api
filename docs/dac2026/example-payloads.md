# DAC 2026 Example Payloads

These JSON files are the checked-in request payloads used by the DAC 2026 live
demos.

Use them directly with `curl --data @...` as shown in the DAC example
reference.

## Payloads

1. `demo1-ingest-chirps3.json`
   - Ingest and publish `chirps3_precipitation_daily`
   - [Open file](examples/demo1-ingest-chirps3.json)

2. `demo2-temporal-resampling.json`
   - Load daily CHIRPS and resample the selected `temporal_extent` to monthly
   - [Open file](examples/demo2-temporal-resampling.json)

3. `demo3-dhis2json-precip-monthly.json`
   - Aggregate monthly precipitation to Sierra Leone admin units and export
     DHIS2 JSON
   - [Open file](examples/demo3-dhis2json-precip-monthly.json)

4. `demo4-chapcsv-precip-temp-monthly.json`
   - Merge monthly precipitation and temperature, aggregate spatially, and
     export CHAP CSV
   - [Open file](examples/demo4-chapcsv-precip-temp-monthly.json)

5. `organisationUnits_sle_level2.json`
   - Sierra Leone level-2 GeoJSON used for bbox checks, geometry validation,
     and district-level examples
   - [Open file](examples/organisationUnits_sle_level2.json)
