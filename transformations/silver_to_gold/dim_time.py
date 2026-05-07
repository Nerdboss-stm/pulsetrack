import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)


def main():
    spark = get_spark_session("GoldDimTime")

    # 1440 rows: time_key = HHMM (0 → 2359)
    rows = [(h * 100 + m, h, m) for h in range(24) for m in range(60)]
    df = spark.createDataFrame(rows, ["time_key", "hour", "minute"])

    df = (
        df.withColumn(
            "time_str",
            F.concat_ws(
                ":",
                F.lpad(F.col("hour").cast("string"), 2, "0"),
                F.lpad(F.col("minute").cast("string"), 2, "0"),
            ),
        ).withColumn(
            "period_of_day",
            F.when((F.col("hour") >= 5) & (F.col("hour") < 12), "morning")
            .when((F.col("hour") >= 12) & (F.col("hour") < 17), "afternoon")
            .when((F.col("hour") >= 17) & (F.col("hour") < 21), "evening")
            .otherwise("night"),
        )
        # Sleep window matches sleep_ring active hours (10pm–7am)
        .withColumn("is_sleep_window", (F.col("hour") >= 22) | (F.col("hour") < 7))
        # Typical clinical hours 8am–5pm
        .withColumn("is_clinical_hours", (F.col("hour") >= 8) & (F.col("hour") < 17))
    )

    df.write.format("delta").mode("overwrite").save(settings.gold_dim_time)
    log.info(
        "dim_time written",
        extra={"extra_data": {"row_count": df.count(), "path": settings.gold_dim_time}},
    )


if __name__ == "__main__":
    main()
