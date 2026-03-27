import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from datetime import date, timedelta
from pyspark.sql import functions as F
from streaming.spark_config import get_spark_session

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"


def main():
    spark = get_spark_session("GoldDimDate")

    start = date(2024, 1, 1)
    dates = [(start + timedelta(days=i)).isoformat() for i in range(1096)]

    df = spark.createDataFrame([(d,) for d in dates], ["date_str"])

    df = (
        df
        .withColumn("date", F.to_date(F.col("date_str")))
        .withColumn("date_key",
            (F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")).cast("int"))
        .withColumn("year",         F.year("date"))
        .withColumn("month",        F.month("date"))
        .withColumn("day",          F.dayofmonth("date"))
        .withColumn("quarter",      F.quarter("date"))
        .withColumn("day_of_week",  F.dayofweek("date"))   # 1=Sun, 7=Sat
        .withColumn("day_name",     F.date_format("date", "EEEE"))
        .withColumn("month_name",   F.date_format("date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend",   F.dayofweek("date").isin([1, 7]))
        # Oct–Mar = flu season
        .withColumn("is_flu_season", F.month("date").isin([1, 2, 3, 10, 11, 12]))
        # US holidays: fixed-date + floating (Memorial Day, Labor Day, Thanksgiving)
        .withColumn("is_holiday", (
            ((F.month("date") == 1)  & (F.dayofmonth("date") == 1))   |   # New Year's Day
            ((F.month("date") == 7)  & (F.dayofmonth("date") == 4))   |   # Independence Day
            ((F.month("date") == 12) & (F.dayofmonth("date") == 25))  |   # Christmas
            ((F.month("date") == 5)  & (F.dayofweek("date") == 2) & (F.dayofmonth("date") >= 25)) |  # Memorial Day (last Mon May)
            ((F.month("date") == 9)  & (F.dayofweek("date") == 2) & (F.dayofmonth("date") <= 7))  |  # Labor Day (1st Mon Sep)
            ((F.month("date") == 11) & (F.dayofweek("date") == 5) & (F.ceil(F.dayofmonth("date") / 7) == 4))  # Thanksgiving (4th Thu Nov)
        ))
        # Medical fiscal year: Q1=Oct-Dec, Q2=Jan-Mar, Q3=Apr-Jun, Q4=Jul-Sep
        .withColumn("fiscal_quarter_medical",
            F.when(F.month("date").isin([10, 11, 12]), 1)
             .when(F.month("date").isin([1, 2, 3]),    2)
             .when(F.month("date").isin([4, 5, 6]),    3)
             .otherwise(4)
        )
        # CDC epi week ≈ ISO week (Mon–Sun)
        .withColumn("cdc_epi_week", F.weekofyear("date"))
        .drop("date_str")
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/dim_date")
    print(f"✅ dim_date rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
