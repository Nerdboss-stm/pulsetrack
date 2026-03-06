"""
Spark Session for PulseTrack (Azure / Azurite)
===============================================
Connects to Azurite (local Azure Blob emulator) instead of MinIO (S3).

KEY DIFFERENCE FROM GHOSTKITCHEN:
- GhostKitchen uses S3 protocol (fs.s3a.*) connecting to MinIO
- PulseTrack uses WASB protocol (fs.azure.*) connecting to Azurite
- Everything else (Spark, Delta Lake, Kafka) is IDENTICAL

This demonstrates cloud-agnostic pipeline design:
same Spark code, different storage connector.
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name="PulseTrack"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ── Azurite (Azure Blob emulator) configuration ──
        # Azurite uses a well-known default storage account and key
        .config("spark.hadoop.fs.azure.storage.emulator.account.name", "devstoreaccount1")
        .config("spark.hadoop.fs.azure.account.key.devstoreaccount1.blob.core.windows.net",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
        
        # Performance
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        
    )

    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "org.apache.hadoop:hadoop-azure:3.3.4",
        ]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark