# Code Review: Red Flags in `src/eo_api`

## Context

Critical review of the `src/eo_api` codebase looking for obvious red flags -- security issues, reliability problems, and architectural concerns that could cause real trouble.

---

## Phased Roadmap

### Phase 1: Security hardening (issues #1, #3, #4)

**1a. Fix SSRF + path traversal in `zonal_statistics.py:108-121`**
- Replace `urlopen()` with `httpx.get()` using a timeout
- Remove local file path support entirely

**1b. Fix CORS in `main.py:12-18`**
- Drop `allow_credentials=True` (the API does not use cookies or session auth)
- Keep `allow_origins=["*"]` since this is a public data API

**1c. Replace dynamic import with allowlist in `cache.py:166-172`**
- Build a dict mapping function path strings to actual callables
- Look up from allowlist instead of `importlib.import_module`

### Phase 2: Reliability (issues #2, #7, #8)

**2a. Lazy-load DHIS2 constants in `constants.py:12-14`**
- Wrap the DHIS2 call + GeoDataFrame computation in a function with `@functools.lru_cache`
- Close the client after use
- Update `cache.py` and `serialize.py` imports to call the function instead of reading module-level vars

**2b. Narrow exception handling in `registry.py:28`**
- Catch `(yaml.YAMLError, OSError)` instead of bare `Exception`
- Re-raise on unexpected errors so they surface at startup

**2c. Preserve exception chains in `chirps3_dhis2_pipeline.py:523-524`**
- Change `from None` to `from e`

### Phase 3: Correctness + cleanup (issues #5, #6, #9, #10)

**3a. Make OGCAPI base URL configurable in `tasks.py:17`**
- `OGCAPI_BASE_URL = os.getenv("OGCAPI_BASE_URL", "http://localhost:8000/ogcapi")`

**3b. Replace `assert` with exceptions in `serialize.py:41,70`**
- `if len(...) != 1: raise ValueError(...)`

**3c. Cache registry lookup in `registry.py:34-37`**
- Add `@functools.lru_cache` to `list_datasets()` (YAML files don't change at runtime)
- `get_dataset()` uses the cached result

**3d. Clean up temp file on write failure in `serialize.py:87-93`**
- Wrap `ds.to_netcdf(path)` in try/except, remove file on failure

### Verification

- `ruff check src/` and `ruff format --check src/` pass
- `mypy src/` passes
- `pyright` passes
- `pytest` passes

---

## Detailed Findings

## 1. SSRF + path traversal in zonal_statistics.py

**File:** `routers/ogcapi/plugins/processes/zonal_statistics.py:108-121`

`_read_geojson_input()` accepts user-supplied strings and will:
- Fetch arbitrary URLs via `urlopen()` with **no timeout** (can hang forever)
- Read arbitrary local files with **no path restriction** (e.g. `/etc/passwd`)

```python
if parsed.scheme in {"http", "https"}:
    with urlopen(geojson_input) as response:     # SSRF, no timeout
        payload = response.read().decode("utf-8")

path = Path(geojson_input)                        # path traversal
```

**Fix:** Use httpx with a timeout, remove file-path support entirely.

---

## 2. Module-level DHIS2 network call blocks import

**File:** `datasets/constants.py:12-14`

```python
client = create_client()
ORG_UNITS_GEOJSON = get_org_units_geojson(client, level=2)
BBOX = list(map(float, gpd.read_file(json.dumps(ORG_UNITS_GEOJSON)).total_bounds))
```

- App startup blocks on DHIS2 being reachable -- no timeout, no fallback
- If DHIS2 is down, the app won't start at all
- The `client` is never closed
- Imported transitively by `datasets/cache.py` and `datasets/serialize.py`, so this runs even if you only touch the datasets module

**Fix:** Lazy-load behind a function with `@lru_cache`, add timeout/fallback.

---

## 3. CORS allows all origins with credentials

**File:** `main.py:12-18`

```python
allow_origins=["*"],
allow_credentials=True,
```

`allow_origins=["*"]` + `allow_credentials=True` is the one CORS combination that's explicitly dangerous -- it enables cross-origin credential theft. Browsers actually block this combo for `Access-Control-Allow-Credentials`, but misconfigured CORS middleware may reflect the `Origin` header instead of sending `*`, which **does** work and is exploitable.

**Fix:** Drop `allow_credentials=True` since cookies/auth headers aren't needed cross-origin.

---

## 4. Dynamic import from YAML config

**File:** `datasets/cache.py:166-172`

```python
def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    module = importlib.import_module(module_path)
    return getattr(module, function_name)
```

The function path comes from YAML files in `datasets/registry/`. If those files are ever editable by users or loaded from an untrusted source, this is arbitrary code execution.

Currently low-risk since the YAML is checked into the repo, but the pattern is fragile.

**Fix:** Use an explicit allowlist mapping instead of dynamic import.

---

## 5. Hardcoded localhost URL for Prefect tasks

**File:** `prefect_flows/tasks.py:17`

```python
OGCAPI_BASE_URL = "http://localhost:8000/ogcapi"
```

Prefect tasks call back to the API over HTTP using a hardcoded localhost URL. This breaks in any deployment where Prefect workers run outside the API container.

**Fix:** Read from environment variable with sensible default.

---

## 6. `assert` used for runtime validation

**File:** `datasets/serialize.py:41,70`

```python
assert len(temp_df[time_dim].unique()) == 1
assert len(temp_ds[time_dim].values) == 1
```

These are stripped when Python runs with `-O`. If these invariants matter (and they do -- the preview functions assume a single timestep), they should be real exceptions.

**Fix:** Replace with `if ... : raise ValueError(...)`.

---

## 7. Silent data loss on YAML parse errors

**File:** `datasets/registry.py:28`

```python
except Exception:
    logger.exception("Error loading %s", file_path.name)
```

If a YAML config file is corrupted or has a syntax error, the datasets in that file silently disappear from the registry. The bare `Exception` catch also swallows unexpected errors like `TypeError` from malformed data.

**Fix:** Catch `(yaml.YAMLError, OSError)` specifically, and consider failing loudly on startup.

---

## 8. Exception chain suppressed with `from None`

**File:** `routers/ogcapi/plugins/processes/chirps3_dhis2_pipeline.py:523-524`

```python
except Exception as e:
    raise ProcessorExecuteError(str(e)) from None
```

`from None` suppresses the original traceback. When this pipeline fails for unexpected reasons, you'll get a flat error message with no stack trace to debug from.

**Fix:** Use `from e` instead of `from None`.

---

## 9. Registry rebuilt from disk on every request

**File:** `datasets/registry.py:34-37`

```python
def get_dataset(dataset_id: str) -> dict[str, Any] | None:
    datasets_lookup = {d["id"]: d for d in list_datasets()}  # reads all YAML files
    return datasets_lookup.get(dataset_id)
```

Every call to `get_dataset()` reads and parses all YAML files from disk and rebuilds the lookup dict. This runs on every API request that involves a dataset.

**Fix:** Cache with `@lru_cache` or build the lookup once at startup.

---

## 10. Temp file leak on write failure

**File:** `datasets/serialize.py:87-93`

```python
fd = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
path = fd.name
fd.close()
ds.to_netcdf(path)    # if this fails, orphaned file in /tmp
return path
```

If `to_netcdf()` raises, the temp file is never cleaned up. Not catastrophic since it's in `/tmp`, but worth a try/except that removes the file on failure.

---

## Summary by severity

| # | Issue | Impact |
|---|-------|--------|
| 1 | SSRF + path traversal | Security: can read local files, make internal requests |
| 2 | Module-level DHIS2 call | Reliability: app won't start if DHIS2 is down |
| 3 | CORS misconfiguration | Security: potential cross-origin credential leak |
| 4 | Dynamic import from YAML | Security: fragile pattern, code exec if YAML untrusted |
| 5 | Hardcoded localhost URL | Deployment: breaks non-local Prefect setups |
| 6 | assert for validation | Correctness: stripped with `-O` flag |
| 7 | Silent YAML error swallowing | Reliability: datasets silently disappear |
| 8 | Exception chain suppressed | Debuggability: no stack trace on failures |
| 9 | Registry rebuilt per request | Performance: unnecessary disk I/O |
| 10 | Temp file leak | Cleanliness: orphaned files on failure |
