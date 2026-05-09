"""
PulseTrack EMR cloud-mode smoke test.

Self-contained — no project imports, no tarball required. Verifies the full
cloud lakehouse path on the live EMR cluster:

  1. Spark + extensions (Delta + Iceberg) load cleanly.
  2. Glue Catalog access — lists bronze/silver/gold databases through Iceberg.
  3. S3 R/W round-trip on the lakehouse bucket (raw parquet).
  4. Iceberg create + write + read + drop via the Glue catalog.
  5. Delta create + write + read + drop on plain S3 path.

Submitted via:
  spark-submit \
      --packages io.delta:delta-spark_2.12:3.0.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      /tmp/smoke_test_cloud.py ${LAKEHOUSE}

Exit code 0 = pass.
"""

import sys

from pyspark.sql import SparkSession


def main(lakehouse_bucket: str) -> None:
    spark = (
        SparkSession.builder
        .appName("pulsetrack-cloud-smoke-test")
        .getOrCreate()
    )

    # ─── 1. Spark + extensions sanity ──────────────────────────────────────
    print(f"[smoke] Spark version: {spark.version}", flush=True)
    print(
        f"[smoke] sql.extensions: {spark.conf.get('spark.sql.extensions', 'unset')}",
        flush=True,
    )
    print(
        f"[smoke] spark_catalog: {spark.conf.get('spark.sql.catalog.spark_catalog', 'unset')}",
        flush=True,
    )

    # ─── 2. Glue Catalog access via Iceberg catalog ────────────────────────
    print("[smoke] Listing Glue databases via glue_iceberg catalog...", flush=True)
    dbs = spark.sql("SHOW DATABASES IN glue_iceberg").collect()
    db_names = sorted(r[0] for r in dbs)
    print(f"[smoke] Glue databases visible: {db_names}", flush=True)
    expected = {"pulsetrack_bronze_dev", "pulsetrack_silver_dev", "pulsetrack_gold_dev"}
    missing = expected - set(db_names)
    assert not missing, f"missing Glue databases: {missing}"
    print("[smoke] OK — all 3 PulseTrack databases visible in Glue Catalog.", flush=True)

    # ─── 3. S3 R/W round-trip (plain parquet) ──────────────────────────────
    s3_test_path = f"s3://{lakehouse_bucket}/smoke-test/probe/"
    print(f"[smoke] Writing parquet probe -> {s3_test_path}", flush=True)
    df = spark.createDataFrame(
        [(1, "alpha"), (2, "beta"), (3, "gamma")], ["id", "name"]
    )
    df.write.mode("overwrite").parquet(s3_test_path)
    rt = spark.read.parquet(s3_test_path).orderBy("id").collect()
    assert [r.id for r in rt] == [1, 2, 3], f"S3 round-trip mismatch: {rt}"
    print(f"[smoke] OK — S3 round-trip read back {len(rt)} rows.", flush=True)

    # ─── 4. Iceberg create + write + read + drop (Glue-managed) ────────────
    iceberg_table = "glue_iceberg.pulsetrack_bronze_dev.smoke_probe_iceberg"
    print(f"[smoke] Iceberg create+write+read+drop: {iceberg_table}", flush=True)
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
    spark.sql(f"CREATE TABLE {iceberg_table} (id BIGINT, name STRING) USING iceberg")
    df.write.format("iceberg").mode("append").save(iceberg_table)
    ib = spark.sql(f"SELECT id, name FROM {iceberg_table} ORDER BY id").collect()
    assert [r.id for r in ib] == [1, 2, 3], f"Iceberg round-trip mismatch: {ib}"
    spark.sql(f"DROP TABLE {iceberg_table}")
    print("[smoke] OK — Iceberg via Glue Catalog works end-to-end.", flush=True)

    # ─── 5. Delta create + write + read + drop (plain S3 path) ─────────────
    delta_path = f"s3://{lakehouse_bucket}/smoke-test/delta_probe/"
    print(f"[smoke] Delta create+write+read+drop: {delta_path}", flush=True)
    df.write.format("delta").mode("overwrite").save(delta_path)
    dr = (
        spark.read.format("delta")
        .load(delta_path)
        .orderBy("id")
        .collect()
    )
    assert [r.id for r in dr] == [1, 2, 3], f"Delta round-trip mismatch: {dr}"
    # Cleanup so the bucket isn't littered with tiny Delta logs.
    spark.sql(f"DELETE FROM delta.`{delta_path}`")
    print("[smoke] OK — Delta on S3 works end-to-end.", flush=True)

    print("[smoke] ALL CHECKS PASSED ✓ (Delta + Iceberg + Glue + S3)", flush=True)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: smoke_test_cloud.py <lakehouse_bucket>", file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1])
