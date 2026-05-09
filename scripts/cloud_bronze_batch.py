"""
Cloud Bronze ingestion — one-shot batch read of MSK -> Bronze Delta on S3.

Why this exists alongside `streaming/bronze_ingestion.py`:
    * The project's bronze ingestion is structured streaming (continuous).
      For an interview-grade end-to-end demo we want a deterministic
      "read everything that's on the topic, write Bronze, exit" job that
      composes cleanly with the silver/gold batch transforms.
    * The decode logic is intentionally identical to bronze_ingestion's
      `_decode_envelope` — same Confluent wire prefix strip, same is_parseable
      flag — so Bronze rows are the same shape regardless of which path
      produced them. The silver transform doesn't care where they came from.

Submitted via spark-submit with these jars (we add at submit time so the
ingestion is self-contained — bootstrap doesn't need to symlink everything):
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,
               org.apache.spark:spark-avro_2.12:3.5.1,
               software.amazon.msk:aws-msk-iam-auth:2.2.0
    --jars     file:///usr/share/aws/delta/lib/delta-spark_2.12-3.1.0-amzn-0.jar,
               file:///usr/share/aws/delta/lib/delta-storage-3.1.0-amzn-0.jar

Args:
    sys.argv[1] = bootstrap brokers
    sys.argv[2] = source kafka topic (e.g., sensor_readings)
    sys.argv[3] = bronze delta path (e.g., s3://bucket/bronze/sensor_readings)
    sys.argv[4] = path to sensor_reading.avsc on the cluster
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType


MSK_IAM_OPTS = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.sasl.jaas.config": (
        "software.amazon.msk.auth.iam.IAMLoginModule required;"
    ),
    "kafka.sasl.client.callback.handler.class": (
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    ),
}


def main(brokers: str, topic: str, bronze_path: str, schema_file: str) -> None:
    with open(schema_file) as fh:
        schema_str = fh.read()

    spark = (
        SparkSession.builder.appName("pulsetrack-cloud-bronze-batch")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    print(f"[bronze] brokers={brokers}", flush=True)
    print(f"[bronze] topic={topic}", flush=True)
    print(f"[bronze] sink={bronze_path}", flush=True)

    reader = spark.read.format("kafka").options(**MSK_IAM_OPTS)
    reader = (
        reader.option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
    )
    raw = reader.load()
    raw_count = raw.count()
    print(f"[bronze] Kafka batch read returned {raw_count} records", flush=True)
    if raw_count == 0:
        print("[bronze] Topic is empty — nothing to ingest. Exiting cleanly.", flush=True)
        spark.stop()
        return

    # Strip Confluent wire prefix (1 magic + 4 schema-id) before from_avro.
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    decoded = (
        raw.select(
            F.col("value").alias("raw_avro_bytes"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias(
                "ingestion_date"
            ),
            F.date_format(F.current_timestamp(), "HH").alias("ingestion_hour"),
            from_avro(avro_payload, schema_str, {"mode": "PERMISSIVE"}).alias(
                "decoded"
            ),
        )
        .withColumn(
            "is_parseable",
            F.col("decoded").isNotNull()
            & F.col("decoded.reading_id").isNotNull(),
        )
    )

    parseable = decoded.filter(F.col("is_parseable")).count()
    print(
        f"[bronze] decoded={decoded.count()} parseable={parseable} "
        f"unparseable={raw_count - parseable}",
        flush=True,
    )

    (
        decoded.write.format("delta")
        .mode("append")
        .partitionBy("ingestion_date", "ingestion_hour")
        .save(bronze_path)
    )
    print(f"[bronze] Wrote {decoded.count()} rows to {bronze_path}", flush=True)

    print("[bronze] OK — cloud bronze batch ingestion complete.", flush=True)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(
            "usage: cloud_bronze_batch.py <brokers> <topic> <bronze_path> <schema_file>",
            file=sys.stderr,
        )
        sys.exit(2)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
