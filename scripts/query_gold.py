"""Verify the cloud Gold fact table — count rows, join dim_metric for names,
show distribution per metric and per date."""

from pyspark.sql import SparkSession

LAKEHOUSE = "s3://pulsetrack-lakehouse-dev-03a28ee7"
GOLD_FACT = f"{LAKEHOUSE}/gold/fact_vital_daily_summary"
GOLD_DIM_METRIC = f"{LAKEHOUSE}/gold/dim_metric"


def main() -> None:
    spark = (
        SparkSession.builder.appName("query-gold-fact-vital-daily")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    fact = spark.read.format("delta").load(GOLD_FACT)
    dim_metric = spark.read.format("delta").load(GOLD_DIM_METRIC)

    print("[query] fact schema: " + fact.schema.simpleString(), flush=True)
    print("[query] dim_metric schema: " + dim_metric.schema.simpleString(), flush=True)
    print(f"[query] fact row count: {fact.count()}", flush=True)
    print(f"[query] dim_metric row count: {dim_metric.count()}", flush=True)
    print(
        f"[query] distinct patients: {fact.select('patient_key').distinct().count()}",
        flush=True,
    )
    print(
        f"[query] distinct dates:    {fact.select('date_key').distinct().count()}",
        flush=True,
    )
    print(
        f"[query] distinct metrics:  {fact.select('metric_key').distinct().count()}",
        flush=True,
    )

    print("[query] --- per-metric row count ---", flush=True)
    joined = fact.join(dim_metric, "metric_key", "left")
    joined.groupBy("metric_name").count().orderBy("metric_name").show(
        truncate=False
    )

    print("[query] --- per-date row count ---", flush=True)
    fact.groupBy("date_key").count().orderBy("date_key").show(truncate=False)

    print("[query] --- sample fact rows joined with dim_metric ---", flush=True)
    joined.select(
        "patient_key",
        "metric_name",
        "date_key",
        "avg_value",
        "min_value",
        "max_value",
        "reading_count",
        "anomaly_count",
    ).orderBy("patient_key", "metric_name", "date_key").show(
        20, truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()
