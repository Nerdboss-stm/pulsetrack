# ADR-001: Apache Iceberg over Delta Lake

## Status
Accepted

## Date
2026-05-09

## Context

PulseTrack needs an open table format on S3 with:

- **ACID transactions** so concurrent Spark streaming writers don't corrupt
  partitions, and so silver/gold consumers see consistent snapshots.
- **Time travel** for replay, debugging, and audit (HIPAA-adjacent data —
  "what did the table look like at 03:47 yesterday?" is a real question).
- **Schema evolution** so we can add columns (e.g. `source_type` enum extension)
  without rewriting historical Parquet.
- **Multi-engine readability** — Spark on EMR is the primary writer, but
  Snowflake reads the same files via external tables + `AUTO_REFRESH`, and
  future Athena/Trino access shouldn't require copying.
- **Glue catalog integration** because AWS Glue is our system-of-record for
  metadata and IAM-governed table-level access.

The field in February 2026:

| Format | Maturity | Catalog story | Spark ecosystem | Multi-engine |
|--------|----------|---------------|-----------------|--------------|
| Delta Lake 3.x | Very mature | Strong inside Databricks, weaker with Glue | Excellent (Databricks-led) | Improving (UniForm) |
| Iceberg 1.10 | Mature | First-class Glue + REST catalog | Good and growing | Excellent (Snowflake, Trino, BigQuery, Athena) |
| Hudi 0.15 | Mature | Glue support exists but lags | Good | Weaker |

## Decision

Use **Apache Iceberg 1.10** with the **AWS Glue catalog** as the production
table format for bronze, silver, and gold layers. Delta Lake remains the
local-dev format for laptop/Azurite testing so contributors don't need an
AWS account to develop.

Configured in `/Users/nerdboss-stm/pulsetrack-cm/streaming/spark_config.py`
via the `glue_iceberg` Spark catalog binding (`GlueCatalog` impl, `S3FileIO`,
zstd Parquet compression).

## Consequences

**Positive**:
- Glue is the catalog of record. IAM-governed table access is enforced at the
  catalog level, not duplicated.
- Snowflake reads the same Parquet/Iceberg manifests directly. No CDC pipeline
  to sync Snowflake — the lakehouse *is* the source of truth.
- Hidden partitioning + partition transforms (`days(ingestion_timestamp)`)
  let us evolve partition layouts without rewriting queries. See V004
  reversed-id migration as a concrete example.
- REST catalog spec means future migration to Tabular/Polaris/Lakekeeper is
  a configuration change, not a data migration.
- Active community: 1.10 shipped streaming-skip-overwrite-snapshots, which
  unblocked silver streaming consumers over bronze (see commit `0cadc84`).

**Negative**:
- Iceberg's Spark integration is less battle-tested than Delta's. We hit
  three real bugs migrating from Delta to Iceberg (see commit `8028edb`:
  "close 3 gaps from prompt 4").
- Fewer DBA-style tools. No equivalent to Delta's `DESCRIBE HISTORY` UX in
  the Spark UI; we read snapshot metadata directly.
- Compaction (`OPTIMIZE`) requires explicit `rewrite_data_files` /
  `expire_snapshots` calls in a maintenance flow — Delta's autoOptimize is
  more turnkey.

## Alternatives Considered

- **Delta Lake 3.x**: rejected because non-Databricks Glue catalog support is
  weaker, and Delta's roadmap is steered by Databricks' commercial interests.
  Specifically, Snowflake's read-path for Delta tables on Glue lags Iceberg
  (no `AUTO_REFRESH` for Delta external tables at decision time).
- **Apache Hudi 0.15**: rejected because Glue catalog support lagged and the
  Spark Structured Streaming write path was less straightforward. Hudi's
  upsert-by-default model is also a worse fit for our append-heavy bronze.
- **Plain Parquet on S3 (no table format)**: rejected because we need ACID
  for concurrent streaming + maintenance writes.

## References

- `/Users/nerdboss-stm/pulsetrack-cm/streaming/spark_config.py` — Glue Iceberg
  catalog config.
- `/Users/nerdboss-stm/pulsetrack-cm/streaming/bronze_ingestion.py` — bronze
  table DDL + partition transforms.
- Commit `0aaf0e1` — "feat: Iceberg on Glue + Glacierbase migration framework".
- Commit `1e03a98` — "feat: modernize stack to EMR 7.13 + Iceberg 1.10".
- Commit `0cadc84` — streaming-skip-overwrite-snapshots fix.
- Related: ADR-002 (EMR over Databricks), ADR-004 (Glacierbase).
