"""
Identity-resolution KPIs for the patient_identity_bridge.

Call :func:`compute_resolution_metrics` after each bridge run to publish a
single structured-log line summarizing linkage health. The metrics are also
exposed as Prometheus gauges so dashboards can track drift over time.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from prometheus_client import Gauge

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)

# ── Prometheus instruments ────────────────────────────────────────────────────
identity_link_rate = Gauge(
    "pt_identity_link_rate_pct",
    "Percentage of bridge rows in 'linked' status",
)
identity_unique_patients = Gauge(
    "pt_identity_unique_patients",
    "Distinct patient_keys with at least one linked identifier",
)
identity_pending = Gauge(
    "pt_identity_pending_rows",
    "Bridge rows still in pending_registration",
)
identity_avg_ids_per_patient = Gauge(
    "pt_identity_avg_ids_per_patient",
    "Average number of identifier rows per linked patient",
)


def compute_resolution_metrics(spark: SparkSession) -> dict:
    """Compute and log identity-resolution KPIs. Returns the metrics dict."""
    bridge = spark.read.format("delta").load(settings.silver_identity_bridge)
    bridge.cache()

    total = bridge.count()
    linked = bridge.filter(F.col("link_status") == "linked").count()
    pending = bridge.filter(F.col("link_status") == "pending_registration").count()

    unique_patients = (
        bridge.filter(F.col("patient_key").isNotNull())
        .select("patient_key")
        .distinct()
        .count()
    )

    avg_ids_row = (
        bridge.filter(F.col("patient_key").isNotNull())
        .groupBy("patient_key")
        .count()
        .agg(F.avg("count").alias("avg_ids_per_patient"))
        .collect()
    )
    avg_ids = float(avg_ids_row[0]["avg_ids_per_patient"]) if avg_ids_row else 0.0

    breakdown = [
        row.asDict() for row in
        bridge.groupBy("identifier_type", "link_status").count()
              .orderBy("identifier_type", "link_status").collect()
    ]
    bridge.unpersist()

    link_rate = round(linked / max(total, 1) * 100, 1)

    metrics = {
        "total_bridge_rows": total,
        "linked": linked,
        "pending": pending,
        "link_rate_pct": link_rate,
        "unique_patients": unique_patients,
        "avg_identifiers_per_patient": round(avg_ids, 2),
        "breakdown": breakdown,
    }

    identity_link_rate.set(link_rate)
    identity_unique_patients.set(unique_patients)
    identity_pending.set(pending)
    identity_avg_ids_per_patient.set(avg_ids)

    log.info("Identity resolution metrics", extra={"extra_data": metrics})
    return metrics
