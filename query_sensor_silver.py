import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F  # noqa: E402

from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)
spark = get_spark_session("CheckSilver")

df = spark.read.format("delta").load(settings.silver_sensor)

log.info("Schema", extra={"extra_data": {"schema": df.schema.json()}})

log.info(
    "Rows per device type",
    extra={
        "extra_data": {"counts": [r.asDict() for r in df.groupBy("device_type").count().collect()]}
    },
)

log.info(
    "Rows per metric",
    extra={
        "extra_data": {
            "counts": [
                r.asDict()
                for r in df.groupBy("metric_name")
                .count()
                .orderBy("count", ascending=False)
                .collect()
            ]
        }
    },
)

log.info(
    "Invalid readings",
    extra={
        "extra_data": {"counts": [r.asDict() for r in df.groupBy("is_valid").count().collect()]}
    },
)

log.info(
    "Late arriving",
    extra={
        "extra_data": {
            "counts": [r.asDict() for r in df.groupBy("is_late_arriving").count().collect()]
        }
    },
)

sample = (
    df.filter(F.col("device_type") == "smartwatch")
    .select("device_id", "metric_name", "metric_value", "is_valid")
    .limit(5)
    .collect()
)
log.info(
    "Sample smartwatch explosion",
    extra={"extra_data": {"rows": [r.asDict() for r in sample]}},
)

spark.stop()
