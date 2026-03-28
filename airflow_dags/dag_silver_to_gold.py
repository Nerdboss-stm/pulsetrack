"""
Airflow DAG: Silver → Gold (Snowflake Schema build)
======================================================
Dependency order (Snowflake Schema enforces FK order):

  Wave 1 — Reference dims (no upstream deps, parallel):
    dim_date, dim_time, dim_condition_category, dim_drug_class, dim_metric

  Wave 2 — Entity dim (needs identity bridge):
    dim_patient

  Wave 3 — Snowflaked entity dims (need dim_patient + FK reference dims):
    dim_condition (→ dim_condition_category)
    dim_medication (→ dim_drug_class)
    dim_device     (→ dim_patient)

  Wave 4 — Facts (need all dims):
    fact_vital_reading        (→ dim_patient, dim_device, dim_metric)
    fact_vital_daily_summary  (→ dim_patient, dim_metric)
    fact_lab_result           (→ dim_patient, dim_condition)
    fact_prescription_fill    (→ dim_patient, dim_medication)
    anomaly_detector          (→ dim_patient, dim_metric)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PULSETRACK_DIR = "/opt/airflow/pulsetrack"
BASE = f"cd {PULSETRACK_DIR} && python"

default_args = {
    "owner":           "pulsetrack",
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="pulsetrack_silver_to_gold",
    default_args=default_args,
    description="PulseTrack: Silver → Gold Snowflake Schema",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pulsetrack", "gold", "snowflake"],
) as dag:

    # ── Wave 1: Reference dimensions ────────────────────────────────────
    dim_date = BashOperator(task_id="dim_date",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_date.py")
    dim_time = BashOperator(task_id="dim_time",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_time.py")
    dim_condition_category = BashOperator(task_id="dim_condition_category",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_condition_category.py")
    dim_drug_class = BashOperator(task_id="dim_drug_class",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_drug_class.py")
    dim_metric = BashOperator(task_id="dim_metric",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_metric.py")

    # ── Wave 2: dim_patient ─────────────────────────────────────────────
    dim_patient = BashOperator(task_id="dim_patient",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_patient.py")

    # ── Wave 3: Snowflaked entity dimensions ────────────────────────────
    dim_condition = BashOperator(task_id="dim_condition",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_condition.py")
    dim_medication = BashOperator(task_id="dim_medication",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_medication.py")
    dim_device = BashOperator(task_id="dim_device",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_device.py")

    # ── Wave 4: Fact tables ─────────────────────────────────────────────
    fact_vital_reading = BashOperator(task_id="fact_vital_reading",
        bash_command=f"{BASE} transformations/silver_to_gold/fact_vital_reading.py")
    fact_vital_daily = BashOperator(task_id="fact_vital_daily_summary",
        bash_command=f"{BASE} transformations/silver_to_gold/fact_vital_daily_summary.py")
    fact_lab = BashOperator(task_id="fact_lab_result",
        bash_command=f"{BASE} transformations/silver_to_gold/fact_lab_result.py")
    fact_rx = BashOperator(task_id="fact_prescription_fill",
        bash_command=f"{BASE} transformations/silver_to_gold/fact_prescription_fill.py")
    anomaly = BashOperator(task_id="anomaly_detector",
        bash_command=f"{BASE} streaming/anomaly_detector.py")

    # ── Dependencies ────────────────────────────────────────────────────
    wave1 = [dim_date, dim_time, dim_condition_category, dim_drug_class, dim_metric]

    wave1 >> dim_patient

    dim_condition_category >> dim_condition
    dim_drug_class         >> dim_medication
    dim_patient >> [dim_condition, dim_medication, dim_device]

    dim_device  >> fact_vital_reading
    dim_metric  >> [fact_vital_reading, fact_vital_daily, anomaly]
    dim_patient >> [fact_vital_reading, fact_vital_daily, fact_lab, fact_rx, anomaly]
    dim_condition  >> fact_lab
    dim_medication >> fact_rx
