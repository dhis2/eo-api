# DHIS2-OGC Mapping Spec (v1)

## Purpose

Define how OGC-facing query behavior is translated to DHIS2-native API parameters while keeping OGC as the external contract.

## Principles

- OGC is the public API contract.
- DHIS2 remains an internal backend contract.
- Prefer pushdown to DHIS2 whenever possible.
- Fallback behavior must be explicit and predictable.

## Capability Mapping

| Capability | OGC input | DHIS2 param | Translation rule | Fallback |
|---|---|---|---|---|
| Paging | `offset`, `limit` | `paging=true`, `page`, `pageSize` | `page = floor(offset/limit)+1`, `pageSize = limit` | If `offset` does not align with page boundary, fetch page then slice locally |
| Paging disabled | policy/config or explicit internal mode | `paging=false` | Request full result set in one DHIS2 call | Enforce max fetch threshold; reject if too large |
| Field projection | `properties=a,b,c` | `fields=id,a,b,c,geometry` | Include `id` always; include `geometry` unless `skipGeometry=true` | Unknown fields -> `400 InvalidParameterValue` |
| Filtering | `filter=` (CQL2 subset) | `filter=` | Translate supported subset (`=`, `IN`, `ILIKE`, `AND`) | Unsupported terms: partial pushdown + local refine, or explicit `400` |
| Nested field fetch | queryable alias | nested `fields` syntax | Alias map (e.g. `parentId -> parent[id]`) | Unmapped alias -> `400` |
| Property transform | output schema | N/A | Rename/normalize fields after fetch | Omit invalid transformed fields and log warning |

## CQL2 Subset for v1

- `field = value`
- `field IN (...)`
- `field ILIKE 'abc%'`
- `expr1 AND expr2`

## `paging=false` Guidance

`paging=false` should be treated as an internal execution mode, not the default for all requests.

Recommended behavior:

1. Default to paged mode (`paging=true`) for normal collection item requests.
2. Use `paging=false` only when:
   - request is explicitly configured for full fetch, and
   - estimated result size is within safe threshold.
3. If threshold is exceeded, reject with clear error (or require paging).

Operational rationale:

- `paging=false` is convenient for small metadata snapshots.
- It is unsafe on large DHIS2 instances without guardrails.

## Example Translations

### 1) Paging

OGC:

```text
/items?limit=50&offset=100
```

DHIS2:

```text
paging=true&page=3&pageSize=50
```

### 2) Field projection

OGC:

```text
/items?properties=name,code,level
```

DHIS2:

```text
fields=id,name,code,level,geometry
```

### 3) Filter pushdown

OGC:

```text
/items?filter=level = 3 AND code ILIKE 'VN-%'
```

DHIS2:

```text
filter=level:eq:3
filter=code:ilike:VN-%
```

## Execution Transparency

Each request should be tagged in metadata/logs as one of:

- `pushdown_full`
- `pushdown_partial`
- `local_only`

This makes performance and correctness behavior observable.
