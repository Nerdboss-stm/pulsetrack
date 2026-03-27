import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from streaming.spark_config import get_spark_session

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

# ICD-10 chapters for the categories present in PulseTrack EHR data.
# Source: ehr_generator.py ICD10_CONDITIONS categories.
# Snowflake parent table — dim_condition FKs here.
CATEGORY_SEED = [
    ("Endocrine",       "E00-E89", "Chapter IV: Endocrine, nutritional and metabolic diseases"),
    ("Circulatory",     "I00-I99", "Chapter IX: Diseases of the circulatory system"),
    ("Respiratory",     "J00-J99", "Chapter X: Diseases of the respiratory system"),
    ("Mental",          "F01-F99", "Chapter V: Mental, Behavioural and Neurodevelopmental disorders"),
    ("Musculoskeletal", "M00-M99", "Chapter XIII: Diseases of the musculoskeletal system and connective tissue"),
    ("Digestive",       "K00-K95", "Chapter XI: Diseases of the digestive system"),
]


def main():
    spark = get_spark_session("GoldDimConditionCategory")

    df = spark.createDataFrame(CATEGORY_SEED, ["category_name", "category_code", "icd_chapter"])

    df = (
        df
        .withColumn("condition_category_key",
            F.abs(F.hash(F.col("category_name"))).cast("long"))
        .select("condition_category_key", "category_code", "category_name", "icd_chapter")
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_condition_category")
    print(f"✅ dim_condition_category rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
