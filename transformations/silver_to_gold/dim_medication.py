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

# Medications from ehr_generator.py MEDICATIONS.
# Snowflake child table — drug_class_key is FK to dim_drug_class.
MEDICATION_SEED = [
    ("metformin", "Metformin HCl", "Biguanides"),
    ("lisinopril", "Lisinopril", "ACE Inhibitors"),
    ("albuterol", "Albuterol Sulfate", "Bronchodilators"),
    ("atorvastatin", "Atorvastatin Calcium", "Statins"),
    ("sertraline", "Sertraline HCl", "SSRIs"),
    ("omeprazole", "Omeprazole", "PPIs"),
]


def main(fmt: str = "delta") -> None:
    spark = get_spark_session("GoldDimMedication")

    drug_classes = make_writer_for(
        spark,
        fmt,
        path=settings.gold_dim_drug_class,
        table_name="dim_drug_class",
        layer="gold",
    ).read_batch()

    df = spark.createDataFrame(MEDICATION_SEED, ["medication_name", "generic_name", "class_name"])

    df = (
        df.join(drug_classes.select("drug_class_key", "class_name"), on="class_name", how="left")
        .withColumn("medication_key", F.abs(F.hash(F.col("medication_name"))).cast("long"))
        .select("medication_key", "medication_name", "generic_name", "drug_class_key")
    )

    writer = make_writer_for(
        spark,
        fmt,
        path=settings.gold_dim_medication,
        table_name="dim_medication",
        layer="gold",
    )
    writer.overwrite(df)
    log.info(
        "dim_medication written",
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
