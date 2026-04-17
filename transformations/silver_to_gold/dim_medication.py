import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)

# Medications from ehr_generator.py MEDICATIONS.
# Snowflake child table — drug_class_key is FK to dim_drug_class.
MEDICATION_SEED = [
    ("metformin",    "Metformin HCl",       "Biguanides"),
    ("lisinopril",   "Lisinopril",           "ACE Inhibitors"),
    ("albuterol",    "Albuterol Sulfate",    "Bronchodilators"),
    ("atorvastatin", "Atorvastatin Calcium", "Statins"),
    ("sertraline",   "Sertraline HCl",       "SSRIs"),
    ("omeprazole",   "Omeprazole",           "PPIs"),
]


def main():
    spark = get_spark_session("GoldDimMedication")

    drug_classes = spark.read.format("delta").load(settings.gold_dim_drug_class)

    df = spark.createDataFrame(MEDICATION_SEED, ["medication_name", "generic_name", "class_name"])

    df = (
        df
        .join(drug_classes.select("drug_class_key", "class_name"), on="class_name", how="left")
        .withColumn("medication_key", F.abs(F.hash(F.col("medication_name"))).cast("long"))
        .select("medication_key", "medication_name", "generic_name", "drug_class_key")
    )

    df.write.format("delta").mode("overwrite").save(settings.gold_dim_medication)
    log.info(
        "dim_medication written",
        extra={"extra_data": {
            "row_count": df.count(),
            "path": settings.gold_dim_medication,
        }},
    )


if __name__ == "__main__":
    main()
