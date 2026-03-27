import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from streaming.spark_config import get_spark_session

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

# Drug classes from ehr_generator.py MEDICATIONS.
# Snowflake parent table — dim_medication FKs here.
DRUG_CLASS_SEED = [
    ("Biguanides",      "Antidiabetics"),
    ("ACE Inhibitors",  "Antihypertensives"),
    ("Bronchodilators", "Respiratory agents"),
    ("Statins",         "Cardiovascular agents"),
    ("SSRIs",           "Antidepressants"),
    ("PPIs",            "Gastrointestinal agents"),
]


def main():
    spark = get_spark_session("GoldDimDrugClass")

    df = spark.createDataFrame(DRUG_CLASS_SEED, ["class_name", "drug_family"])

    df = (
        df
        .withColumn("drug_class_key", F.abs(F.hash(F.col("class_name"))).cast("long"))
        .select("drug_class_key", "class_name", "drug_family")
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_drug_class")
    print(f"✅ dim_drug_class rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
