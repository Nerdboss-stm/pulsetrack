import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from streaming.spark_config import get_spark_session

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

# ICD-10 conditions from ehr_generator.py.
# Snowflake child table — condition_category_key is FK to dim_condition_category.
CONDITION_SEED = [
    ("E11.9",  "Type 2 diabetes without complications",               "Endocrine"),
    ("I10",    "Essential hypertension",                              "Circulatory"),
    ("J45.20", "Mild intermittent asthma, uncomplicated",             "Respiratory"),
    ("E78.5",  "Hyperlipidemia, unspecified",                         "Endocrine"),
    ("F32.1",  "Major depressive disorder, single episode, moderate", "Mental"),
    ("M54.5",  "Low back pain",                                       "Musculoskeletal"),
    ("K21.0",  "GERD with esophagitis",                               "Digestive"),
]


def main():
    spark = get_spark_session("GoldDimCondition")

    cats = spark.read.format("delta").load(f"{GOLD_BASE}/dim_condition_category")

    df = spark.createDataFrame(CONDITION_SEED, ["condition_code", "condition_name", "category_name"])

    df = (
        df
        .join(cats.select("condition_category_key", "category_name"), on="category_name", how="left")
        .withColumn("condition_key", F.abs(F.hash(F.col("condition_code"))).cast("long"))
        .select("condition_key", "condition_code", "condition_name", "condition_category_key")
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_condition")
    print(f"✅ dim_condition rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
