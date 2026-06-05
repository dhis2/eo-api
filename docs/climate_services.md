# Climate Services

## What are climate services?

Climate services is the systematic production, translation, and delivery of climate information to support decision-making. The term covers the full chain from raw observations and model output to the usable information that planners, health workers, farmers, and governments need to act.

The [Global Framework for Climate Services](https://gfcs.wmo.int/) (GFCS), established by the World Meteorological Organization, describes this chain in five components:

- **Observations and monitoring** — the measurement of atmospheric, oceanic, and land-surface conditions that provides the raw material for all climate information.
- **Research, modelling, and prediction** — the science that converts observations into reanalysis products, seasonal forecasts, and long-range projections.
- **Climate services information system** — the infrastructure that stores, processes, and makes climate data accessible.
- **User interface** — the tools and platforms through which decision-makers access and interact with climate information.
- **Capacity development** — the training and institutional support that enables countries to produce and use climate services.

In practice, most countries — especially low- and middle-income countries — lack the infrastructure to access, process, and operationalise climate data across the first three components. Data is fragmented across dozens of providers, each with different APIs, formats, and access requirements. Bridging the gap between global data producers and national decision-makers is the core challenge that climate services try to solve.

---

## What is the Open Climate Service?

The Open Climate Service is an open-source implementation of the climate services information system component. It is the technical infrastructure that downloads, processes, stores, and serves climate and Earth Observation data — making it accessible to health systems, early warning platforms, and analytical tools without requiring specialised expertise for each data source.

Concretely, the Open Climate Service is the [DHIS2 Climate API](https://github.com/dhis2/climate-api) — a FastAPI-based platform that:

- **Ingests** climate and Earth Observation data from multiple upstream providers (CHIRPS, ERA5-Land, WorldPop, GEFS, and others) through a unified interface.
- **Processes** raw data into harmonised, analysis-ready outputs: reprojected, chunked, and stored in cloud-native GeoZarr format.
- **Serves** data through open standards — OGC API, STAC, and direct Zarr access — so any compliant tool can consume it.
- **Integrates** with DHIS2 for health sector use, but operates independently of any specific platform.

The name reflects two commitments:

**Open** means open source, open standards, and open deployment. The platform is built on open-source software, exposes data through OGC API and GeoZarr standards, and can be deployed on national infrastructure so countries retain full control of their data. There is no dependency on proprietary services.

**Climate Service** means the platform provides an operational service — not a one-off tool — that systematically acquires, processes, and delivers climate information on a schedule. It is infrastructure that runs continuously, keeps datasets current, and serves multiple consumers from a single deployment.

---

## Where does it sit in the climate services chain?

The Open Climate Service sits at the **climate services information system** layer. It does not produce observations or run climate models. Instead, it connects to the outputs of global data producers — reanalysis products, satellite-derived datasets, numerical weather prediction — and makes them accessible at country scale.

```
Global data producers
  (ECMWF, NOAA, NASA, CHIRPS, WorldPop, …)
          │
          │  standardised ingestion
          ▼
Open Climate Service
  (download → process → store → serve)
          │
          │  OGC API, Zarr, STAC
          ▼
User interfaces and analytical tools
  (DHIS2 Maps, DHIS2 Climate App, CHAP, QGIS, …)
          │
          │  aggregated, formatted data
          ▼
Decision-makers
  (health planners, epidemiologists, early warning systems, …)
```

This positioning is deliberate. The hard problems in climate services for low- and middle-income countries are not scientific — global climate data is largely open and of high quality. The hard problems are operational: acquiring data reliably, processing it consistently, keeping it current, and serving it in formats that decision-support tools can use. The Open Climate Service is designed to solve exactly those problems.

---

## Relationship to DHIS2

DHIS2 is the most widely deployed health information system in low- and middle-income countries. Linking climate data to health data — the same org units, the same time periods — is the core analytical requirement for climate-sensitive disease surveillance and early warning.

The Open Climate Service is designed as shared infrastructure for the DHIS2 climate and health ecosystem: a single, well-defined source of spatiotemporal raster data that the DHIS2 Climate App, the DHIS2 Maps App, the CHAP Modelling Platform, and other tools can build on. It handles data acquisition and harmonisation so that application developers do not need to solve those problems independently for each dataset.

Although developed in close alignment with DHIS2, the platform operates independently. It has no hard dependency on a DHIS2 instance, and its OGC API and Zarr endpoints can be consumed by any compliant tool — including QGIS, Jupyter notebooks, and custom analytics pipelines.
