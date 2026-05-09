import argparse
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from lakehouse import make_writer_for  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)

# Drug classes from ehr_generator.py MEDICATIONS.
# Snowflake parent table — dim_medication FKs here.
DRUG_CLASS_SEED = [
    ("Biguanides", "Antidiabetics"),
    ("ACE Inhibitors", "Antihypertensives"),
    ("Bronchodilators", "Respiratory agents"),
    ("Statins", "Cardiovascular agents"),
    ("SSRIs", "Antidepressants"),
    ("PPIs", "Gastrointestinal agents"),
]


def main(fmt: str = "delta") -> None:
    spark = get_spark_session("GoldDimDrugClass")

    df = spark.createDataFrame(DRUG_CLASS_SEED, ["class_name", "drug_family"])

    df = df.withColumn("drug_class_key", F.abs(F.hash(F.col("class_name"))).cast("long")).select(
        "drug_class_key", "class_name", "drug_family"
    )

    writer = make_writer_for(
        spark,
        fmt,
        path=settings.gold_dim_drug_class,
        table_name="dim_drug_class",
        layer="gold",
    )
    writer.overwrite(df)
    log.info(
        "dim_drug_class written",
        extra={
            "extra_data": {
                "row_count": df.count(),
                "format": fmt,
                "target": writer.identity.fqn if fmt == "iceberg" else writer.identity.path,
            }
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=["delta", "iceberg"], default="delta")
    args = parser.parse_args()
    main(fmt=args.format)
