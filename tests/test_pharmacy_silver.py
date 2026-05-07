"""Pharmacy Silver tests — parsing + quality flags + dedup."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _make_bronze(spark, decoded_rows):
    """Build a Bronze-shaped DataFrame from a list of (decoded-dict, is_parseable) tuples."""
    decoded_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("patient_id", StringType()),
            StructField("drug_name", StringType()),
            StructField("ndc_code", StringType()),
            StructField("prescriber_npi", StringType()),
            StructField("fill_date", IntegerType()),
            StructField("quantity", IntegerType()),
            StructField("fda_report_id", StringType()),
            StructField("event_timestamp", LongType()),
        ]
    )
    schema = StructType(
        [
            StructField("decoded", decoded_schema),
            StructField("is_parseable", BooleanType()),
            StructField("ingestion_timestamp", TimestampType()),
        ]
    )
    rows = []
    now = datetime.now(timezone.utc)
    for decoded, parseable in decoded_rows:
        rows.append(Row(decoded=decoded, is_parseable=parseable, ingestion_timestamp=now))
    return spark.createDataFrame(rows, schema)


def test_parse_drops_unparseable(spark):
    from transformations.bronze_to_silver.pharmacy_silver import parse_pharmacy

    bronze = _make_bronze(
        spark,
        [
            (
                {
                    "event_id": "ev1",
                    "event_type": "new_fill",
                    "patient_id": "p1",
                    "drug_name": "Metformin",
                    "ndc_code": None,
                    "prescriber_npi": None,
                    "fill_date": 19000,
                    "quantity": 30,
                    "fda_report_id": None,
                    "event_timestamp": 1700000000000,
                },
                True,
            ),
            (None, False),
        ],
    )
    parsed = parse_pharmacy(bronze)
    assert parsed.count() == 1
    assert parsed.first()["event_id"] == "ev1"


def test_quality_flags_invalid_event_type(spark):
    from transformations.bronze_to_silver.pharmacy_silver import transform

    bronze = _make_bronze(
        spark,
        [
            (
                {
                    "event_id": "ev1",
                    "event_type": "garbage",
                    "patient_id": "p1",
                    "drug_name": "Metformin",
                    "ndc_code": None,
                    "prescriber_npi": None,
                    "fill_date": 19000,
                    "quantity": 30,
                    "fda_report_id": None,
                    "event_timestamp": 1700000000000,
                },
                True,
            )
        ],
    )
    silver = transform(bronze)
    assert silver.first()["is_valid"] is False


def test_quality_flags_negative_quantity_invalid(spark):
    from transformations.bronze_to_silver.pharmacy_silver import transform

    bronze = _make_bronze(
        spark,
        [
            (
                {
                    "event_id": "ev1",
                    "event_type": "new_fill",
                    "patient_id": "p1",
                    "drug_name": "Lisinopril",
                    "ndc_code": None,
                    "prescriber_npi": None,
                    "fill_date": 19000,
                    "quantity": -1,
                    "fda_report_id": None,
                    "event_timestamp": 1700000000000,
                },
                True,
            )
        ],
    )
    silver = transform(bronze)
    assert silver.first()["is_valid"] is False


def test_quality_flags_valid_passes(spark):
    from transformations.bronze_to_silver.pharmacy_silver import transform

    bronze = _make_bronze(
        spark,
        [
            (
                {
                    "event_id": "ev1",
                    "event_type": "new_fill",
                    "patient_id": "p1",
                    "drug_name": "Metformin",
                    "ndc_code": "00071-0156-23",
                    "prescriber_npi": "1234567890",
                    "fill_date": 19000,
                    "quantity": 30,
                    "fda_report_id": None,
                    "event_timestamp": 1700000000000,
                },
                True,
            )
        ],
    )
    silver = transform(bronze)
    row = silver.first()
    assert row["is_valid"] is True
    assert row["drug_name"] == "Metformin"
    assert row["event_type"] == "new_fill"


def test_dedup_on_event_id(spark):
    from transformations.bronze_to_silver.pharmacy_silver import transform

    decoded = {
        "event_id": "ev1",
        "event_type": "new_fill",
        "patient_id": "p1",
        "drug_name": "Metformin",
        "ndc_code": None,
        "prescriber_npi": None,
        "fill_date": 19000,
        "quantity": 30,
        "fda_report_id": None,
        "event_timestamp": 1700000000000,
    }
    bronze = _make_bronze(spark, [(decoded, True), (decoded, True), (decoded, True)])
    silver = transform(bronze).dropDuplicates(["event_id"])
    assert silver.count() == 1
