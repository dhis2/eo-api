# OCS operator surfaces

Status: accepted design direction ([CLIM-921](https://dhis2.atlassian.net/browse/CLIM-921))

## Decision

OCS owns its operational capabilities, policy, and state. An instance must remain fully
operable without DHIS2. OCS therefore provides a standalone operator experience through
its HTTP API, built-in `/manage` console, and host CLI.

The DHIS2 Climate App is an optional client of the OCS API. It may provide DHIS2-specific
presentation, mappings, and interaction flows, but it must not duplicate OCS lifecycle
rules or maintain a parallel copy of OCS operational state.

The domain services and persistent stores are the source of truth. HTTP routes, `/manage`,
the CLI, and external clients are adapters over those services.

## Built-in web experience

OCS owns two distinct built-in web surfaces:

- **The landing page (`/`) is user-facing discovery.** It presents the instance, published
  data, available templates, exploration tools, and documentation. It must remain useful
  without DHIS2 and scale as the catalogue grows. Its navigation and visual redesign are
  delegated to a separate landing-page improvement ticket.
- **The `/manage` console is operator-facing administration.** It presents operational
  state and authenticated actions such as ingestion, synchronization, and dataset lifecycle
  management. It is unavailable on a public read-only instance.

This separation is part of the CLIM-921 decision. The follow-up ticket changes the landing
page's information architecture and presentation; it does not reconsider ownership or move
operator actions onto the public page.

## Placement and access

| Operator task | OCS surface | Access class | Climate App role |
| --- | --- | --- | --- |
| Browse published datasets and extents | Public API and landing page | Public read | Display through the API |
| Inspect disk use and quota | API and `/manage` | Operator read | Optional display through the API |
| Start ingestion or synchronization | API, `/manage`, and CLI | Operator write; host CLI for offline/read-only maintenance | Optional trigger through the API |
| Inspect job history and errors | API, `/manage`, and CLI | Operator read | Optional display through the API |
| Unpublish or delete a managed dataset | Shared lifecycle service, exposed through API, `/manage`, and CLI | Operator write; destructive delete requires explicit confirmation | Optional trigger through the API |
| Inspect schedule state and last-run outcome | API and `/manage` | Operator read | Optional display through the API |
| Define schedules | `climate-service.yaml`; a future CLI may edit it | Host operator | None |
| Inspect time-axis gaps | API, `/manage`, and CLI | Public summary for published data; operator diagnostics otherwise | Optional display through the API |
| Configure read-only mode and write policy | Configuration, middleware, and authentication | Host operator | Acts as an authenticated client |
| Mutate a public read-only instance | CLI using the shared service layer | Host-only | None |

## Rules

- Network-exposed operator reads and writes require the authentication and authorization
  policy from CLIM-843. Read-only does not automatically mean safe for anonymous access:
  job errors, quota, and scheduler diagnostics can contain operational information.
- `/manage` and the CLI reuse the same domain services as the API routes; they do not
  implement separate lifecycle rules.
- The CLI may call services directly so it can work while the server is stopped and on a
  read-only deployment. Until cross-process locking and transactional persistence exist,
  direct store mutation must require the server to be stopped or otherwise guarantee a
  single writer.
- Schedule definitions remain operator-managed configuration for the first version. The
  web console displays their state but does not edit them.
- The landing page remains an OCS-owned user-facing surface, while `/manage` remains the
  OCS-owned operator surface. A separate ticket implements the landing-page redesign without
  changing that boundary.

## Follow-up work

| Work | Ticket or disposition |
| --- | --- |
| Authentication and authorization for operator routes and views | [CLIM-843](https://dhis2.atlassian.net/browse/CLIM-843) |
| Disk usage and quota reporting | [CLIM-845](https://dhis2.atlassian.net/browse/CLIM-845) |
| Safe unpublish and delete lifecycle operations | [CLIM-871](https://dhis2.atlassian.net/browse/CLIM-871) |
| Time-axis integrity and gap inspection | [CLIM-913](https://dhis2.atlassian.net/browse/CLIM-913), after the ordered-period work in PR #357 |
| Durable scheduler failure visibility | [CLIM-919](https://dhis2.atlassian.net/browse/CLIM-919) |
| Host CLI ingestion for read-only deployments | [CLIM-862](https://dhis2.atlassian.net/browse/CLIM-862) |
| `/manage` job-history and scheduler-status views | New implementation ticket required |
| Broader lifecycle and inspection CLI commands | New implementation ticket required |
| Navigable, scalable OCS landing page | [CLIM-940](https://dhis2.atlassian.net/browse/CLIM-940) |

The recommended implementation order is:

1. Safe read-side services and diagnostics.
2. Shared lifecycle operations and explicit destructive safeguards.
3. Host CLI adapters over those services.
4. Authentication and authorization for the network operator surface.
5. Authenticated API and `/manage` mutations.

The landing-page redesign can proceed independently because it changes presentation only.
