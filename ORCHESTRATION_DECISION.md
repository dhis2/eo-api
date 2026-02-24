# Orchestration Decision Scorecard

## Purpose

Select the primary orchestrator for eo-api scheduled and long-running execution.

Options compared:

- Prefect
- Airflow
- Dagster
- Temporal
- Argo Workflows
- Internal scheduler only

## Scoring model

- Scale: 1 (weak) to 5 (strong)
- Weighted score = score × weight
- Weights reflect current eo-api phase (MVP to near-production)

| Criterion | Weight |
|---|---:|
| API-triggered integration fit | 20 |
| Time to production | 15 |
| Operations overhead | 15 |
| Reliability and retries | 15 |
| Governance and auditability | 10 |
| Developer productivity | 10 |
| Scalability and parallelism | 10 |
| Cost predictability | 5 |
| **Total** | **100** |

## Results

| Option | API fit (20) | Time (15) | Ops (15) | Reliability (15) | Governance (10) | Dev speed (10) | Scale (10) | Cost (5) | **Weighted total / 100** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Prefect** | 5 | 5 | 4 | 4 | 3 | 5 | 4 | 4 | **87** |
| **Dagster** | 3 | 3 | 3 | 4 | 4 | 4 | 4 | 3 | **68** |
| **Airflow** | 3 | 3 | 2 | 4 | 5 | 3 | 5 | 3 | **67** |
| **Temporal** | 4 | 2 | 3 | 5 | 3 | 2 | 5 | 3 | **66** |
| **Argo Workflows** | 3 | 2 | 3 | 4 | 3 | 2 | 5 | 3 | **61** |
| **Internal scheduler only** | 2 | 5 | 5 | 2 | 1 | 4 | 2 | 5 | **59** |

## Recommendation

### Primary now

- **Use Prefect as the primary orchestrator** for eo-api.
- Keep the internal scheduler enabled as fallback for simple local/dev operation.

### Why

- Strongest fit with current API-driven execution design.
- Lowest implementation friction in this repository today.
- Good reliability and operational controls without Airflow-level overhead.

## Re-evaluation triggers

Re-run this decision if one or more of these become true:

- Organization mandates a central Airflow platform for all production data pipelines.
- Multi-team governance, strict lineage, or compliance reporting dominates requirements.
- Workflow complexity shifts to very long-lived, stateful business processes.
- eo-api run volume and fan-out exceed current orchestration/operator capacity.

## Practical adoption plan

1. Keep Prefect as default orchestration backend.
2. Preserve adapter-style boundary in eo-api so alternate orchestrators can be added.
3. Review quarterly using real metrics:
   - schedule success rate
   - mean time to recovery
   - operator effort per week
   - backfill effort and failure rate

## Notes

- Scores are tuned for current project context, not universal.
- If enterprise governance is weighted higher than implementation speed, Airflow can become the preferred option.
