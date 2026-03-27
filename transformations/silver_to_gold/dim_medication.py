import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from streaming.spark_config import get_spark_session

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

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

    drug_classes = spark.read.format("delta").load(f"{GOLD_BASE}/dim_drug_class")

    df = spark.createDataFrame(MEDICATION_SEED, ["medication_name", "generic_name", "class_name"])

    df = (
        df
        .join(drug_classes.select("drug_class_key", "class_name"), on="class_name", how="left")
        .withColumn("medication_key", F.abs(F.hash(F.col("medication_name"))).cast("long"))
        .select("medication_key", "medication_name", "generic_name", "drug_class_key")
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_medication")
    print(f"✅ dim_medication rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
