# Pygeoapi Maintainability Argument

## Purpose

This note captures the justification for the current publication bridge architecture, especially in response to the critique that an earlier integration used only a few lines of code via:

```python
from pygeoapi.starlette_app import APP as pygeoapi_app
from pygeoapi.starlette_app import CONFIG
```

The short version is:

- the old approach looked simpler because it leaned on pygeoapi runtime globals
- the current approach is more maintainable because EO API now owns publication truth explicitly
- pygeoapi is still used for standard OGC serving, but it is no longer the owner of backend publication state

## The Real Question

The architectural question is not:

- "Should we use the standard pygeoapi implementation or build our own?"

The real question is:

- "Which part of the system should own publication state and execution-linked publication lifecycle?"

The current branch does **not** replace pygeoapi as a standards-oriented serving component.

Instead, it moves ownership of publication truth into the EO API backend and uses pygeoapi as one serving projection of that truth.

## What The Thin Old Approach Did Well

The old approach had a real strength:

- it made pygeoapi integration look very small and direct

That is useful for:

- quick demos
- minimal bootstrapping
- proving that pygeoapi can be mounted and served successfully

For a spike or early prototype, this is a good move.

## What The Thin Old Approach Hid

The small wrapper did not remove complexity.
It mostly relocated complexity into pygeoapi runtime globals and config handling.

That creates ambiguity around:

1. where publication truth actually lives
2. how dynamic publication updates should happen
3. how job/output lifecycle links to collections
4. how cleanup and deletion should remove published resources
5. how source and derived resources should share one publication model
6. how nightly refreshes should update collection extents and metadata

So the old approach was shorter, but it encouraged a hidden ownership model:

- pygeoapi `APP` and `CONFIG` start to feel like the publication database

That is the maintainability problem.

## Why The Current Approach Is Cleaner

The current branch introduces an explicit publication bridge:

```text
workflow/source dataset
  -> publication registration
  -> PublishedResource JSON
  -> generated pygeoapi YAML
  -> /pygeoapi collection
```

This gives each layer one responsibility:

1. EO API backend owns execution truth
2. EO API backend owns publication truth through `PublishedResource`
3. generated pygeoapi config is a serving projection of that truth
4. pygeoapi remains the standards-oriented collection/items serving layer

This is cleaner because:

- publication state is explicit
- job-to-publication linkage is explicit
- cleanup semantics are explicit
- source and derived resources share one model
- pygeoapi is replaceable because it consumes projection rather than owning truth

## Why This Is More Maintainable Long Term

Long-term maintainability improves because the current architecture:

1. avoids treating pygeoapi runtime globals as the domain model
2. gives EO API a backend-owned publication record that can evolve independently
3. supports dynamic workflow outputs without hardcoding unknown future collections
4. allows retention/cleanup to remove publications coherently
5. allows metadata refresh and extent updates to happen in backend-owned publication state first
6. reduces framework lock-in because pygeoapi becomes an adapter, not the publication brain

In short:

- the old approach optimized for a short integration seam
- the current approach optimizes for explicit ownership and lifecycle clarity

That is the better long-term tradeoff.

## What We Still Reuse From Pygeoapi

This architecture does **not** argue against pygeoapi.

We still rely on pygeoapi for:

1. standards-oriented collection/items serving
2. provider-backed feature/coverage publication
3. generic browse behavior
4. HTML/JSON OGC browse surfaces while that still adds value

So the current model is not:

- "replace the standard implementation"

It is:

- "keep the standard implementation for serving, but stop using it as the owner of backend publication state"

## Short Defense Statement

Use this if you need a short explanation:

> The earlier pygeoapi integration was shorter because it outsourced state handling to pygeoapi’s app/config layer. The current approach is more maintainable because EO API now owns workflow-aware publication truth explicitly, while pygeoapi remains the standard serving layer fed by generated configuration.

## Even Shorter Version

> We still use pygeoapi for standard OGC serving. What changed is that publication truth now lives in EO API instead of pygeoapi runtime globals. That is why the current design is more maintainable.
