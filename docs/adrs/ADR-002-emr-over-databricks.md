# ADR-002: Amazon EMR over Databricks

## Status
Accepted

## Date
2026-05-07

## Context

PulseTrack needs Spark compute for two distinct workloads:

1. **Long-running structured streaming**: bronze/silver/gold queries for
   wearable and pharmacy events. Continuous YARN applications, modest
   per-executor sizing, sustained 24/7 footprint.
2. **Scheduled batch**: EHR daily, identity bridge, dbt-supporting Spark
   transforms, nightly maintenance (OPTIMIZE / expire_snapshots). Burst
   workloads, ~10–60 min runs, idle 90%+ of the time.

The decision space in May 2026 narrowed to three serious options: managed
Databricks (Lakehouse Platform), Amazon EMR on EC2 with our own cluster
lifecycle, and EMR Serverless. Self-managed Spark on EKS was on the table
but ruled out early as too much operational burden for a small team.

Cost was the primary forcing function — at our scale (dev: ~$200/mo target,
prod target: ~$2k/mo), the Databricks markup over raw EC2 is the difference
between "viable" and "not viable" for the project.

## Decision

Use **Amazon EMR 7.13 (Spark 3.5.6) on EC2** with:

- Spot instances for core nodes (configurable in `dev.tfvars`).
- 2-hour auto-termination on idle to avoid wasted spend.
- Iceberg 1.10 + Delta 3.x preinstalled (EMR 7.13 ships both).
- Glue catalog for all Iceberg tables.
- A separate `teardown-compute.sh` lifecycle that keeps S3/Glue/IAM intact
  across cluster recycle (see commit `a286cda`).

Snowflake handles the dbt mart layer separately — EMR is not involved in
SQL-warehouse-shaped work.

## Consequences

**Positive**:
- ~30–50% cost reduction vs. equivalent Databricks DBU billing at our scale.
  Dev cluster runs ~$0.40/hr on spot + EMR markup; an equivalent Databricks
  All-Purpose Compute cluster is ~$0.80/hr+ before DBUs.
- Native AWS service integration: MSK, Glue, S3, CloudWatch all wire up
  without a Databricks-side connector.
- Glue catalog is reusable everywhere — Snowflake reads the same metadata,
  no Unity Catalog lock-in.
- EMR step API gives Prefect a clean handoff for batch Spark submissions
  (see `orchestration/tasks/emr_tasks.py`).

**Negative**:
- We manage cluster lifecycle ourselves. `teardown-compute.sh`, the
  2-hour auto-termination, and the `infrastructure/modules/compute/` module
  are all glue we'd skip on Databricks.
- No Photon-equivalent. Spark performance is vanilla. For our scale this
  doesn't bind (~10M-event scale-test runs in <15 min — see
  `docs/scale_test_results.md`).
- Less ergonomic notebooks. No Databricks Notebooks; engineers run via
  spark-submit or VS Code on the master node.
- EMR upgrades require explicit Terraform changes (vs. Databricks' rolling
  runtime updates).

## Alternatives Considered

- **Databricks Lakehouse Platform**: rejected primarily on cost. Secondary
  reasons: Unity Catalog would replace Glue as catalog-of-record, fragmenting
  the metadata story for non-Spark consumers (Snowflake AUTO_REFRESH wants
  Glue). Databricks' Iceberg story was also UniForm-mediated rather than
  native at decision time.
- **EMR Serverless**: rejected because at decision time it didn't support
  all the integrations we needed (notably MSK IAM auth had gaps and
  startup latency was 60–90s — too high for our 15-minute Prefect cadence
  on small batch flows). Worth re-evaluating in Phase 2.
- **Self-managed Spark on EKS**: rejected as too much operational burden.
  We'd own Spark Operator, Kubernetes node groups, auto-scalers, and the
  EKS upgrade cycle for marginal cost savings over EMR.
- **Glue ETL (Spark-as-a-service)**: rejected because Glue's Spark
  versions trail EMR by several minor releases, and the per-DPU pricing is
  competitive with EMR only for very short bursty jobs.

## References

- `/Users/nerdboss-stm/pulsetrack-cm/infrastructure/modules/compute/` — EMR
  cluster Terraform module.
- `/Users/nerdboss-stm/pulsetrack-cm/infrastructure/terraform.tfvars.example`
  — instance type + spot bid defaults.
- `/Users/nerdboss-stm/pulsetrack-cm/infrastructure/teardown-compute.sh` —
  Option B teardown preserving data layer.
- Commit `3448d12` — "feat: cloud-native pipeline — EMR, S3, MSK, Glue".
- Commit `1e03a98` — "modernize stack to EMR 7.13 + Iceberg 1.10".
- Commit `a286cda` — teardown-compute.sh split.
- Related: ADR-001 (Iceberg), ADR-005 (streaming-first hybrid).
