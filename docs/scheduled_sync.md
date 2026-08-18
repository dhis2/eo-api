# Scheduled dataset synchronization

Open Climate Service can periodically update selected, already-ingested datasets. APScheduler
owns cron timing only; due work is queued through the native job service, which owns execution,
status, retries, and restart recovery.

## Enable scheduling for the deployment

An operator enables the capability in `climate-service.yaml`:

```yaml
scheduler:
  enabled: true
  timezone: UTC
  max_concurrent_syncs: 1
```

`timezone` is the default for newly created schedules. `max_concurrent_syncs` configures the
native worker pool used by queued ingestion and sync jobs; the default of one makes writes run
sequentially. Exactly one writable OCS process may enable the scheduler clock.

## Create a dataset schedule

Scheduling is an explicit per-dataset choice. The target must be an existing managed dataset.
Create or replace its only schedule with:

```http
PUT /schedules/chirps3_precipitation_daily
Content-Type: application/json

{
  "cron": "0 6 * * *",
  "timezone": "Europe/Oslo",
  "enabled": true,
  "publish": true,
  "max_attempts": 3
}
```

`cron` uses the standard five-field format. Omitting `timezone` applies the deployment default.
The dataset ID is the resource key, so two schedules cannot exist for the same dataset.

Edit timing or enablement without creating another schedule:

```http
PATCH /schedules/chirps3_precipitation_daily
Content-Type: application/json

{
  "cron": "30 7 * * 1-5",
  "enabled": true
}
```

Disable a schedule with `{"enabled": false}`. Delete it with
`DELETE /schedules/{dataset_id}`; neither operation deletes the dataset or its job history.

## Run now and inspect status

`POST /schedules/{dataset_id}/run` queues an immediate sync without changing the recurring
cadence, including when that schedule is disabled. `GET /schedules` and
`GET /schedules/{dataset_id}` expose definitions, next checks, recent enqueue outcomes, and job
IDs.

When several schedules become due together, each callback enqueues and returns. The native job
queue executes them according to the configured worker limit. The sync itself may complete as a
no-op when upstream data has not changed; that outcome is retained on the native job record.

Schedule definitions are stored in `data_dir/schedules/schedules.json` and reconstructed at
startup. Native run history remains in `data_dir/jobs/jobs.json`.

## Current constraints

A read-only instance cannot create, edit, delete, or run schedules and never starts the clock.
Because OCS does not yet authenticate individual users, schedule mutation belongs on a trusted
writable/operator instance rather than a public endpoint.
Future-facing forecast datasets are rejected until overlapping-window refresh is implemented.
Event-driven workflows, distributed leader election, and cross-service orchestration remain later
automation phases.
