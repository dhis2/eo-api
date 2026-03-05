# Archived Process Wrappers

These files are legacy helper-process wrappers that are no longer exposed as public OGC API Processes.

Archived wrappers:
- `feature_fetch.py`
- `data_aggregate.py`
- `datavalue_build.py`

Reason:
- API semantics were tightened to keep public OGC Processes focused on orchestration and dataset-level execution.
- The corresponding logic now lives in reusable components under `src/eo_api/integrations/`.

If needed for compatibility in the future, wrappers can be restored and re-registered in `pygeoapi-config.yml`.
