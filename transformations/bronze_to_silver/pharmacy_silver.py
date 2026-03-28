"""
Pharmacy Silver — Bronze → Silver
===================================
Reads Debezium-style CDC events from Bronze pharmacy Delta table.
CDC envelope: {"op": "c"|"u"|"d", "before": {...}, "after": {...}}

Applies latest-wins CDC: for each rx_id, keep the final "after" state.
Output: /tmp/pulsetrack-lakehouse/silver/pharmacy_prescriptions/

Schema matches DataModel.md silver_prescriptions:
  rx_id, pharmacy_patient_id, ndc_code, drug_name,
  fill_date, days_supply, refill_number, status, processed_timestamp
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

BRONZE_RX_PATH = "/tmp/pulsetrack-lakehouse/bronze/pharmacy"
SILVER_RX_PATH = "/tmp/pulsetrack-lakehouse/silver/pharmacy_prescriptions"

CDC_AFTER_SCHEMA = StructType([
    StructField("rx_id",               StringType()),
    StructField("pharmacy_patient_id", StringType()),
    StructField("ndc_code",            StringType()),
    StructField("drug_name",           StringType()),
    StructField("fill_date",           StringType()),
    StructField("days_supply",         IntegerType()),
    StructField("refill_number",       IntegerType()),
    StructField("status",              StringType()),
])


def run_pharmacy_silver(spark: SparkSession):
    if not DeltaTable.isDeltaTable(spark, BRONZE_RX_PATH):
        print("  WARNING: Bronze pharmacy not found — skipping pharmacy silver.")
        print("  Run batch_ingestion/pharmacy_bronze.py first.")
        return

    bronze = spark.read.format("delta").load(BRONZE_RX_PATH)
    print(f"  Bronze pharmacy rows: {bronze.count()}")

    # Parse CDC envelope — keep "after" payload for inserts and updates
    parsed = (
        bronze
        .withColumn("op",    F.get_json_object(F.col("raw_value"), "$.op"))
        .withColumn("after", F.from_json(F.get_json_object(F.col("raw_value"), "$.after"),
                                         CDC_AFTER_SCHEMA))
        .filter(F.col("op").isin("c", "u"))
        .filter(F.col("after.rx_id").isNotNull())
        .withColumn("processed_timestamp", F.col("ingestion_timestamp"))
    )

    # Latest state per rx_id (last CDC event wins)
    from pyspark.sql.window import Window
    w = Window.partitionBy("after.rx_id").orderBy(F.col("ingestion_timestamp").desc())
    latest = (
        parsed
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("after.rx_id").alias("rx_id"),
            F.col("after.pharmacy_patient_id").alias("pharmacy_patient_id"),
            F.col("after.ndc_code").alias("ndc_code"),
            F.col("after.drug_name").alias("drug_name"),
            F.col("after.fill_date").alias("fill_date"),
            F.col("after.days_supply").alias("days_supply"),
            F.col("after.refill_number").alias("refill_number"),
            F.col("after.status").alias("status"),
            F.col("processed_timestamp"),
        )
    )

    if DeltaTable.isDeltaTable(spark, SILVER_RX_PATH):
        DeltaTable.forPath(spark, SILVER_RX_PATH).alias("tgt").merge(
            latest.alias("src"),
            "tgt.rx_id = src.rx_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        latest.write.format("delta").save(SILVER_RX_PATH)

    print(f"✅ pharmacy_prescriptions rows: {spark.read.format('delta').load(SILVER_RX_PATH).count()}")


if __name__ == "__main__":
    from streaming.spark_config import get_spark_session
    spark = get_spark_session("PharmacySilver")
    run_pharmacy_silver(spark)
