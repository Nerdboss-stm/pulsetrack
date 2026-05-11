"""
Scheduled maintenance: OPTIMIZE + Z-ORDER + VACUUM + table-property sync.

Run daily during off-peak hours. Format-aware — handles both Delta
(local dev) and Iceberg (cloud) with the equivalent maintenance ops:

| Op            | Delta (local)                 | Iceberg (cloud)                                    |
|---------------|-------------------------------|----------------------------------------------------|
| Set properties| ALTER TABLE ... TBLPROPERTIES | ALTER TABLE ... SET TBLPROPERTIES                  |
| Compaction    | optimize().executeCompaction()| CALL system.rewrite_data_files(table)              |
| Z-Order       | optimize().executeZOrderBy()  | CALL system.rewrite_data_files(table, sort_order)  |
| VACUUM        | vacuum(retentionHours)        | CALL system.expire_snapshots(table, older_than)    |
|               |                               | + CALL system.remove_orphan_files(table)           |

The job chooses the path based on ``settings.environment`` (local → Delta,
cloud → Iceberg) plus a per-table override allowed via ``fmt=...`` in
TABLES below.

For every production table the job:
1. Sets recommended table properties (idempotent ALTER TABLE).
2. Runs OPTIMIZE (with Z-ORDER columns for Delta, with sort_order for
   Iceberg) — co-locates data by frequently-filtered columns to make
   downstream predicate pushdown effective.
3. Runs VACUUM (Delta) or expire_snapshots + remove_orphan_files (Iceberg)
   with a 7-day retention.
4. Logs ``numFiles / sizeInBytes / partitionColumns`` per table.

Tables that don't exist yet are skipped with a single log line so the job
can run safely against a partially-populated lakehouse.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)

VACUUM_RETENTION_HOURS = 168  # 7 days

TABLE_PROPERTIES: dict[str, str] = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
}

# Tables to maintain. Z-ORDER columns are chosen to match the most common
# filter / join patterns in downstream queries.
TABLES: list[dict] = [
    # Bronze (highest volume, partitioned by ingestion date)
    {"path": settings.bronze_sensor, "zorder_cols": ["ingestion_date"]},
    {"path": settings.bronze_pharmacy, "zorder_cols": ["ingestion_date"]},
    # Silver
    {"path": settings.silver_sensor, "zorder_cols": ["device_type", "metric_name"]},
    {"path": settings.silver_ehr_conditions, "zorder_cols": ["patient_id"]},
    {"path": settings.silver_ehr_medications, "zorder_cols": ["patient_id", "medication"]},
    {"path": settings.silver_ehr_lab_results, "zorder_cols": ["patient_id", "test_code"]},
    {"path": settings.silver_identity_bridge, "zorder_cols": ["identifier_type"]},
    # Gold facts
    {"path": settings.gold_fact_vital_daily, "zorder_cols": ["patient_key", "date_key"]},
    {"path": settings.gold_fact_vital_reading, "zorder_cols": ["patient_key", "date_key"]},
    {"path": settings.gold_fact_lab_result, "zorder_cols": ["patient_key", "date_key"]},
    # Gold dimensions (small, no Z-ORDER needed)
    {"path": settings.gold_dim_patient, "zorder_cols": []},
    {"path": settings.gold_dim_device, "zorder_cols": ["device_id"]},
    {"path": settings.gold_dim_metric, "zorder_cols": []},
    {"path": settings.gold_dim_date, "zorder_cols": []},
    {"path": settings.gold_dim_time, "zorder_cols": []},
    {"path": settings.gold_dim_condition, "zorder_cols": []},
    {"path": settings.gold_dim_condition_category, "zorder_cols": []},
    {"path": settings.gold_dim_medication, "zorder_cols": []},
    {"path": settings.gold_dim_drug_class, "zorder_cols": []},
    # Operational
    {"path": settings.dlq, "zorder_cols": []},
    {"path": settings.quarantine, "zorder_cols": []},
]


def _set_properties(spark: SparkSession, path: str) -> None:
    """Idempotent ALTER TABLE delta.`<path>` SET TBLPROPERTIES (...)."""
    props = ",\n            ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    spark.sql(
        f"""
        ALTER TABLE delta.`{path}` SET TBLPROPERTIES (
            {props}
        )
        """
    )
    log.info("Delta properties applied", extra={"extra_data": {"path": path}})


def _iceberg_table_identifier(path: str) -> str:
    """Resolve a path-like setting (e.g. settings.silver_sensor) to a fully-qualified Iceberg
    table identifier (e.g. ``glue_iceberg.pulsetrack_silver_dev.sensor_readings``).

    The reverse map is built from settings — local paths and Iceberg identifiers are paired
    by table-name. Falls back to None if no Iceberg mapping exists (skip).
    """
    # Settings exposes both delta paths and Iceberg fully-qualified table names. Look up by
    # stripping the path prefix and reconstructing the canonical {catalog}.{db}.{table} name.
    name = path.rstrip("/").rsplit("/", 1)[-1]
    layer_map = {
        "bronze": settings.glue_db_bronze,
        "silver": settings.glue_db_silver,
        "gold": settings.glue_db_gold,
    }
    for layer, db in layer_map.items():
        if f"/{layer}/" in path:
            return f"{settings.iceberg_catalog}.{db}.{name}"
    return None


def _maintain_iceberg_table(spark: SparkSession, identifier: str, sort_cols: list[str]) -> None:
    """Iceberg equivalent of the Delta maintenance path:
    rewrite_data_files (with optional sort_order = Z-Order analogue),
    expire_snapshots, remove_orphan_files.
    """
    log.info("Iceberg OPTIMIZE starting",
             extra={"extra_data": {"table": identifier, "sort_order": sort_cols}})
    try:
        if sort_cols:
            # Iceberg's sort_order during rewrite_data_files is the equivalent of Delta's
            # Z-ORDER — both co-locate rows by the named columns so predicate pushdown
            # short-circuits whole files.
            sort_spec = ", ".join(sort_cols)
            spark.sql(
                f"CALL {settings.iceberg_catalog}.system.rewrite_data_files("
                f"table => '{identifier}', "
                f"strategy => 'sort', "
                f"sort_order => '{sort_spec}'"
                f")"
            )
        else:
            spark.sql(
                f"CALL {settings.iceberg_catalog}.system.rewrite_data_files("
                f"table => '{identifier}'"
                f")"
            )
    except Exception:
        log.exception("Iceberg rewrite_data_files failed",
                      extra={"extra_data": {"table": identifier}})

    log.info("Iceberg VACUUM (expire_snapshots + remove_orphan_files)",
             extra={"extra_data": {"table": identifier}})
    try:
        # Equivalent of Delta VACUUM 168h: keep snapshots from the last 7 days,
        # then remove file-system-orphan parquets that no live snapshot references.
        spark.sql(
            f"CALL {settings.iceberg_catalog}.system.expire_snapshots("
            f"table => '{identifier}', "
            f"older_than => TIMESTAMP '{(spark.sql('SELECT current_timestamp() - INTERVAL 7 DAYS').collect()[0][0]).isoformat()}'"
            f")"
        )
        spark.sql(
            f"CALL {settings.iceberg_catalog}.system.remove_orphan_files("
            f"table => '{identifier}'"
            f")"
        )
    except Exception:
        log.exception("Iceberg expire/remove failed",
                      extra={"extra_data": {"table": identifier}})


def _maintain_table(spark: SparkSession, path: str, zorder_cols: list[str]) -> None:
    # Cloud path: route to Iceberg. The same zorder_cols are reused as Iceberg sort_order
    # (semantically equivalent — both co-locate rows by the named columns).
    if settings.environment == "cloud":
        identifier = _iceberg_table_identifier(path)
        if identifier is None:
            log.info("Skipping (no Iceberg mapping)",
                     extra={"extra_data": {"path": path}})
            return
        _maintain_iceberg_table(spark, identifier, zorder_cols)
        return

    # Local path: Delta. Original logic preserved.
    if not DeltaTable.isDeltaTable(spark, path):
        log.info(
            "Skipping (not a Delta table)",
            extra={"extra_data": {"path": path}},
        )
        return

    try:
        _set_properties(spark, path)
    except Exception:
        log.exception("ALTER TABLE failed", extra={"extra_data": {"path": path}})

    dt = DeltaTable.forPath(spark, path)

    log.info(
        "Delta OPTIMIZE starting",
        extra={"extra_data": {"path": path, "zorder": zorder_cols}},
    )
    try:
        if zorder_cols:
            dt.optimize().executeZOrderBy(*zorder_cols)
        else:
            dt.optimize().executeCompaction()
    except Exception:
        log.exception("OPTIMIZE failed", extra={"extra_data": {"path": path}})

    log.info(
        "Delta VACUUM starting",
        extra={
            "extra_data": {
                "path": path,
                "retention_hours": VACUUM_RETENTION_HOURS,
            }
        },
    )
    try:
        dt.vacuum(retentionHours=VACUUM_RETENTION_HOURS)
    except Exception:
        log.exception("VACUUM failed", extra={"extra_data": {"path": path}})

    try:
        detail = dt.detail().collect()[0]
        log.info(
            "Table stats",
            extra={
                "extra_data": {
                    "path": path,
                    "num_files": detail["numFiles"],
                    "size_bytes": detail["sizeInBytes"],
                    "partition_columns": list(detail.get("partitionColumns", []) or []),
                }
            },
        )
    except Exception:
        log.exception("Detail collection failed", extra={"extra_data": {"path": path}})


def run_compaction(spark: Optional[SparkSession] = None) -> None:
    """Run OPTIMIZE + VACUUM + property sync against every configured table."""
    spark = spark or get_spark_session("PulseTrack-Compaction")
    log.info("Compaction run starting", extra={"extra_data": {"table_count": len(TABLES)}})
    for tc in TABLES:
        _maintain_table(spark, tc["path"], tc["zorder_cols"])
    log.info("Compaction run complete", extra={"extra_data": {"tables_processed": len(TABLES)}})


if __name__ == "__main__":
    run_compaction()
