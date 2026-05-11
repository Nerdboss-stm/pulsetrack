"""
Spark Session for PulseTrack — local (Azurite + Delta) or cloud (EMR + Glue Iceberg).

Local mode: downloads Kafka/Avro/hadoop-azure JARs via Maven, points at the
Azurite emulator with its well-known dev key.

Cloud mode: assumes EMR has Spark/Delta/Iceberg JARs preinstalled. Configures
Glue as the Iceberg catalog, enables dynamic allocation, uses zstd Parquet
compression and Kryo serialization for S3 efficiency.

Branching is controlled by ``settings.environment`` (PT_ENVIRONMENT env var).
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import settings

# Spark connector versions tracked alongside the runtime Spark version.
# Local dev: pinned to the latest 3.5.x line so the local pipeline stays
# close to what EMR ships (Spark 3.5.6 in emr-7.13.0, Hadoop 3.4.2).
# Cluster-side spark-submit ``--packages`` invocations should match these.
LOCAL_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
    "org.apache.spark:spark-avro_2.12:3.5.6",
    "org.apache.hadoop:hadoop-azure:3.4.2",
]

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def _apply_delta_defaults(builder):
    return (
        builder.config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
        .config("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
        .config(
            "spark.databricks.delta.properties.defaults.logRetentionDuration", "interval 30 days"
        )
        .config(
            "spark.databricks.delta.properties.defaults.deletedFileRetentionDuration",
            "interval 7 days",
        )
    )


def _apply_cloud(builder):
    builder = builder.config(
        "spark.sql.extensions",
        f"{DELTA_EXTENSION},{ICEBERG_EXTENSION}",
    )
    return (
        builder.config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue_iceberg.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_iceberg.warehouse", settings.glue_iceberg_warehouse)
        .config("spark.sql.catalog.glue_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "10")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )


def _apply_local(builder):
    return (
        builder.config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.fs.azure.storage.emulator.account.name", "devstoreaccount1")
        .config(
            "spark.hadoop.fs.azure.account.key.devstoreaccount1.blob.core.windows.net",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",  # nosec B105 - Azurite well-known dev key
        )
    )


def get_spark_session(app_name: str = "PulseTrack") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
    )
    # In cloud mode (PT_ENVIRONMENT=cloud), let spark-submit's --deploy-mode/--master
    # determine the cluster master. Calling .master() here overrides the YARN
    # config and forces local mode — which means no YARN executors get allocated.
    # The AM stays in ACCEPTED state forever because it never registers with RM.
    # (See postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md fix #15.)
    if settings.environment != "cloud":
        builder = builder.master(settings.spark_master)
    builder = _apply_delta_defaults(builder)

    if settings.environment == "cloud":
        builder = _apply_cloud(builder)
        spark = builder.getOrCreate()
    else:
        builder = _apply_local(builder)
        spark = configure_spark_with_delta_pip(builder, extra_packages=LOCAL_PACKAGES).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
