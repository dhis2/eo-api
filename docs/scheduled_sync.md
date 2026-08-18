# Scheduled dataset synchronization

Open Climate Service can periodically check whether an existing managed dataset has new
source periods and submit an asynchronous sync job when work is required. The scheduler is
a clock only: planning, retries, execution, progress, and restart recovery remain owned by
the native OCS job service.

## Configure schedules

Schedules are an instance-level operational choice in `climate-service.yaml`:

```yaml
scheduler:
  enabled: true
  timezone: UTC
  dataset_sync:
    - dataset_id: chirps3_precipitation_daily
      cron: "0 6 * * *"
      publish: true
      max_attempts: 3
```

`cron` is a standard five-field expression interpreted in the configured IANA timezone.
UTC is the default and is recommended for checks that follow upstream publication times.
Only one entry is allowed per dataset.

The target dataset must already have been ingested. A scheduled check calls the normal sync
planner with an open target end, skips an up-to-date or active dataset, and submits actionable
work through the same native job path as an asynchronous `POST /sync/{dataset_id}` request.
Routine no-op checks do not create job records.

## Inspect status

`GET /schedules` reports whether the process-local clock is running and, for each schedule,
its next check, last check, outcome, message, and submitted native job ID. Check state is
volatile and resets when the process restarts; submitted jobs remain durable in the native
job store.

## Deployment constraints

Exactly one OCS process or replica may set `scheduler.enabled: true`. Active-job detection
and submission are not an atomic cross-process operation, and the store write lock is
process-local. Enabling the clock in multiple replicas could therefore submit concurrent
writers for the same store. API-only replicas must leave scheduling disabled.

A read-only instance never starts its scheduler. Use a separate writable operator instance
to maintain data served by a structurally read-only public instance.

## Current scope

Scheduled sync supports previously ingested historical or otherwise append/rematerialize
datasets. Future-facing forecast datasets are rejected at startup: refreshing a forecast
requires replacing an overlapping forward window, not merely appending periods after the
latest stored timestamp.

Event-driven derived workflows, cross-service orchestration, schedule mutation APIs,
distributed leader election, and forecast-window refresh are later automation phases.
