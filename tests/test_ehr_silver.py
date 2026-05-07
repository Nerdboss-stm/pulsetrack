"""EHR silver: build_conditions_df / build_medications_df / build_labs_df."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _patient_record(
    patient_id: str,
    entries: list[dict],
    email: str = "p@example.com",
    batch_date: str = "2026-05-03",
) -> dict:
    return {
        "patient_id": patient_id,
        "patient_email": email,
        "batch_date": batch_date,
        "entries": entries,
    }


def test_build_conditions_df_filters_to_condition_resources(spark):
    from transformations.bronze_to_silver.ehr_silver import build_conditions_df

    records = [
        _patient_record(
            "MRN-1",
            [
                {
                    "resource_type": "Condition",
                    "code": "I10",
                    "description": "Hyp",
                    "category": "Circulatory",
                    "is_chronic": True,
                    "onset_date": "2024-01-01",
                    "status": "active",
                    "clinician_npi": "NPI-1",
                },
                {
                    "resource_type": "MedicationStatement",
                    "medication": "metformin",
                    "generic_name": "Metformin HCl",
                    "drug_class": "Biguanides",
                    "dosage": "500mg",
                    "frequency": "once_daily",
                    "start_date": "2024-01-01",
                    "end_date": None,
                    "status": "active",
                    "prescriber_npi": "NPI-2",
                },
            ],
        )
    ]
    df = build_conditions_df(spark, records).collect()
    assert len(df) == 1
    assert df[0]["icd10_code"] == "I10"
    assert df[0]["patient_id"] == "MRN-1"


def test_build_conditions_df_includes_row_hash_for_change_detection(spark):
    from transformations.bronze_to_silver.ehr_silver import build_conditions_df

    records = [
        _patient_record(
            "MRN-1",
            [
                {
                    "resource_type": "Condition",
                    "code": "I10",
                    "description": "Hyp",
                    "category": "Circulatory",
                    "is_chronic": True,
                    "onset_date": "2024-01-01",
                    "status": "active",
                    "clinician_npi": "NPI-1",
                },
            ],
        )
    ]
    df = build_conditions_df(spark, records).collect()
    assert df[0]["row_hash"] is not None
    assert len(df[0]["row_hash"]) == 64  # sha256 hex


def test_build_medications_df_emits_scd2_columns(spark):
    from transformations.bronze_to_silver.ehr_silver import build_medications_df

    records = [
        _patient_record(
            "MRN-1",
            [
                {
                    "resource_type": "MedicationStatement",
                    "medication": "metformin",
                    "generic_name": "Metformin HCl",
                    "drug_class": "Biguanides",
                    "dosage": "500mg",
                    "frequency": "once_daily",
                    "start_date": "2024-01-01",
                    "end_date": None,
                    "status": "active",
                    "prescriber_npi": "NPI-2",
                },
            ],
        )
    ]
    df = build_medications_df(spark, records).collect()
    row = df[0]
    assert row["medication"] == "metformin"
    assert row["effective_start"] is not None
    assert row["effective_end"] is None  # active → no end
    assert row["is_current"] is True
    assert row["row_hash"] is not None


def test_build_medications_df_marks_stopped_meds_not_current(spark):
    from transformations.bronze_to_silver.ehr_silver import build_medications_df

    records = [
        _patient_record(
            "MRN-1",
            [
                {
                    "resource_type": "MedicationStatement",
                    "medication": "metformin",
                    "generic_name": "Metformin HCl",
                    "drug_class": "Biguanides",
                    "dosage": "500mg",
                    "frequency": "once_daily",
                    "start_date": "2024-01-01",
                    "end_date": "2024-06-01",
                    "status": "stopped",
                    "prescriber_npi": "NPI-2",
                },
            ],
        )
    ]
    row = build_medications_df(spark, records).collect()[0]
    assert row["is_current"] is False
    assert row["effective_end"] is not None


def test_build_labs_df_computes_observation_id(spark):
    from transformations.bronze_to_silver.ehr_silver import build_labs_df

    records = [
        _patient_record(
            "MRN-1",
            [
                {
                    "resource_type": "Observation",
                    "code": "HbA1c",
                    "value": 6.4,
                    "unit": "%",
                    "reference_low": 4.0,
                    "reference_high": 5.6,
                    "is_abnormal": True,
                    "date": "2026-04-01",
                },
            ],
        )
    ]
    row = build_labs_df(spark, records).collect()[0]
    assert row["test_code"] == "HbA1c"
    assert row["value"] == pytest.approx(6.4)
    assert row["is_abnormal"] is True
    assert row["observation_id"] is not None  # sha256 of natural key
