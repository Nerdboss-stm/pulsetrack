"""dim_patient: linked vs unlinked patients, PII masking, age groups."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _write_ehr_batch(tmp_lakehouse: Path, date_str: str, patients: list[dict]) -> None:
    """Drop a synthetic EHR batch under settings.ehr_batch_dir for the run."""
    from config import settings

    out_dir = Path(settings.ehr_batch_dir) / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "batch_date": date_str,
        "patient_count": len(patients),
        "patients": patients,
    }
    (out_dir / "ehr_batch.json").write_text(json.dumps(payload))


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dim_patient_fallback_when_bridge_missing(spark, tmp_lakehouse, monkeypatch):
    """No bridge → seeds dim_patient directly from EHR batch JSON."""
    monkeypatch.chdir(tmp_lakehouse)
    monkeypatch.setenv("PT_EHR_BATCH_DIR", "ehr_batches")
    import importlib
    import config
    importlib.reload(config)

    _write_ehr_batch(tmp_lakehouse, "2026-05-03", [
        {"patient_id": "MRN-1", "patient_email": "a@x.com", "patient_birth_year": 1990},
        {"patient_id": "MRN-2", "patient_email": "b@x.com", "patient_birth_year": 1955},
    ])

    from transformations.silver_to_gold.dim_patient import main as dim_patient_main
    dim_patient_main()

    dim = spark.read.format("delta").load(config.settings.gold_dim_patient)
    rows = dim.collect()
    assert len(rows) == 2
    cols = set(dim.columns)
    assert {"patient_key", "patient_id_masked", "age_group"} <= cols

    # PII masking: patient_id_masked is sha256 hex (64 chars)
    for r in rows:
        assert len(r["patient_id_masked"]) == 64

    # Age groups derived from birth_year
    age_groups = {r["age_group"] for r in rows}
    assert age_groups <= {"0-17", "18-34", "35-49", "50-64", "65+"}
