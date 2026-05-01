"""
Scheduled maintenance: OPTIMIZE + Z-ORDER + VACUUM + table-property sync.

Run daily during off-peak hours. For every production Delta table the job:

1. Sets the recommended Delta table properties (idempotent ALTER TABLE).
2. Runs OPTIMIZE; with Z-ORDER columns it executes ``executeZOrderBy``
   (co-locates data by frequently-filtered columns), otherwise plain
   ``executeCompaction`` to coalesce small files into ~128MB targets.
3. VACUUM with ``retentionHours = 168`` (7 days) — old files are removed.
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
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
    {"path": settings.bronze_sensor,             "zorder_cols": ["ingestion_date"]},
    {"path": settings.bronze_pharmacy,           "zorder_cols": ["ingestion_date"]},

    # Silver
    {"path": settings.silver_sensor,             "zorder_cols": ["device_type", "metric_name"]},
    {"path": settings.silver_ehr_conditions,     "zorder_cols": ["patient_id"]},
    {"path": settings.silver_ehr_medications,    "zorder_cols": ["patient_id", "medication"]},
    {"path": settings.silver_ehr_lab_results,    "zorder_cols": ["patient_id", "test_code"]},
    {"path": settings.silver_identity_bridge,    "zorder_cols": ["identifier_type"]},

    # Gold facts
    {"path": settings.gold_fact_vital_daily,     "zorder_cols": ["patient_key", "date_key"]},
    {"path": settings.gold_fact_vital_reading,   "zorder_cols": ["patient_key", "date_key"]},
    {"path": settings.gold_fact_lab_result,      "zorder_cols": ["patient_key", "date_key"]},

    # Gold dimensions (small, no Z-ORDER needed)
    {"path": settings.gold_dim_patient,            "zorder_cols": []},
    {"path": settings.gold_dim_device,             "zorder_cols": ["device_id"]},
    {"path": settings.gold_dim_metric,             "zorder_cols": []},
    {"path": settings.gold_dim_date,               "zorder_cols": []},
    {"path": settings.gold_dim_time,               "zorder_cols": []},
    {"path": settings.gold_dim_condition,          "zorder_cols": []},
    {"path": settings.gold_dim_condition_category, "zorder_cols": []},
    {"path": settings.gold_dim_medication,         "zorder_cols": []},
    {"path": settings.gold_dim_drug_class,         "zorder_cols": []},

    # Operational
    {"path": settings.dlq,                         "zorder_cols": []},
    {"path": settings.quarantine,                  "zorder_cols": []},
]


def _set_properties(spark: SparkSession, path: str) -> None:
    """Idempotent ALTER TABLE delta.`<path>` SET TBLPROPERTIES (...)."""
    props = ",\n            ".join(
        f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items()
    )
    spark.sql(
        f"""
        ALTER TABLE delta.`{path}` SET TBLPROPERTIES (
            {props}
        )
        """
    )
    log.info("Delta properties applied", extra={"extra_data": {"path": path}})


def _maintain_table(spark: SparkSession, path: str, zorder_cols: list[str]) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        log.info(
            "Skipping (not a Delta table)",
            extra={"extra_data": {"path": path}},
        )
        return

    try:
        _set_properties(spark, path)
    except Exception:
        log.exception("ALTER TABLE failed",
                      extra={"extra_data": {"path": path}})

    dt = DeltaTable.forPath(spark, path)

    log.info(
        "OPTIMIZE starting",
        extra={"extra_data": {"path": path, "zorder": zorder_cols}},
    )
    try:
        if zorder_cols:
            dt.optimize().executeZOrderBy(*zorder_cols)
        else:
            dt.optimize().executeCompaction()
    except Exception:
        log.exception("OPTIMIZE failed",
                      extra={"extra_data": {"path": path}})

    log.info(
        "VACUUM starting",
        extra={"extra_data": {
            "path": path, "retention_hours": VACUUM_RETENTION_HOURS,
        }},
    )
    try:
        dt.vacuum(retentionHours=VACUUM_RETENTION_HOURS)
    except Exception:
        log.exception("VACUUM failed",
                      extra={"extra_data": {"path": path}})

    try:
        detail = dt.detail().collect()[0]
        log.info(
            "Table stats",
            extra={"extra_data": {
                "path": path,
                "num_files": detail["numFiles"],
                "size_bytes": detail["sizeInBytes"],
                "partition_columns": list(detail.get("partitionColumns", []) or []),
            }},
        )
    except Exception:
        log.exception("Detail collection failed",
                      extra={"extra_data": {"path": path}})


def run_compaction(spark: Optional[SparkSession] = None) -> None:
    """Run OPTIMIZE + VACUUM + property sync against every configured table."""
    spark = spark or get_spark_session("PulseTrack-Compaction")
    log.info("Compaction run starting",
             extra={"extra_data": {"table_count": len(TABLES)}})
    for tc in TABLES:
        _maintain_table(spark, tc["path"], tc["zorder_cols"])
    log.info("Compaction run complete",
             extra={"extra_data": {"tables_processed": len(TABLES)}})


if __name__ == "__main__":
    run_compaction()
