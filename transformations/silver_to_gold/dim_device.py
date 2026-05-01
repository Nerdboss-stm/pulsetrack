"""
dim_device — Gold layer (SCD2 on firmware_version).

Grain: 1 row per (device_id, firmware_version) version with effective dates
and an ``is_current`` flag. History is derived deterministically from
Silver: each distinct (device_id, firmware_version) becomes a row whose
``effective_start`` is the first observation and whose ``effective_end`` is
the first observation of the *next* firmware on that device. The latest
firmware row per device is marked ``is_current = true``.

Because Silver is the immutable source of truth, the table is rebuilt with
``mode("overwrite")`` rather than merged in place — that keeps SCD2 lineage
deterministic and idempotent across reruns.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)


def main():
    spark = get_spark_session("GoldDimDevice")

    silver = (
        spark.read.format("delta")
        .load(settings.silver_sensor)
        .filter(F.col("device_id").isNotNull())
        .select("device_id", "device_type", "firmware_version", "event_timestamp")
    )

    # Earliest + latest event per (device, firmware) combination.
    versions = (
        silver.groupBy("device_id", "device_type", "firmware_version")
        .agg(
            F.min("event_timestamp").alias("first_seen"),
            F.max("event_timestamp").alias("last_seen"),
        )
    )

    # Ordering by first_seen lets us derive effective_end from the next
    # firmware's first_seen on the same device.
    w = Window.partitionBy("device_id").orderBy("first_seen")
    history = (
        versions.withColumn("next_first_seen", F.lead("first_seen").over(w))
        .withColumn("effective_start", F.to_date(F.col("first_seen")))
        .withColumn(
            "effective_end",
            F.when(
                F.col("next_first_seen").isNotNull(),
                F.to_date(F.col("next_first_seen")),
            ).otherwise(F.lit(None).cast(DateType())),
        )
        .withColumn("is_current", F.col("next_first_seen").isNull())
        .withColumn(
            "device_key",
            F.abs(
                F.hash(
                    F.concat_ws(
                        "|",
                        F.col("device_id"),
                        F.col("firmware_version"),
                        F.col("first_seen").cast("string"),
                    )
                )
            ).cast("long"),
        )
    )

    dim = history.select(
        "device_key",
        "device_id",
        "device_type",
        "firmware_version",
        "effective_start",
        "effective_end",
        "is_current",
        F.col("first_seen").alias("first_event_at"),
        F.col("last_seen").alias("last_event_at"),
    )

    (
        dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(settings.gold_dim_device)
    )

    n = dim.count()
    n_current = dim.filter(F.col("is_current")).count()
    n_devices = dim.select("device_id").distinct().count()
    log.info(
        "dim_device written (SCD2)",
        extra={"extra_data": {
            "row_count": n,
            "current_rows": n_current,
            "distinct_devices": n_devices,
            "path": settings.gold_dim_device,
        }},
    )


if __name__ == "__main__":
    main()
