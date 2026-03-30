"""
Airflow DAG: Full Pipeline Orchestrator
========================================
Master DAG that chains Bronze → Silver → Gold end-to-end.
Triggers dag_bronze_to_silver first (batch maintenance), then
dag_silver_to_gold (dimensional build) after all Silver jobs confirm.

Architecture note (Kappa):
  This DAG is the JANITOR coordinator, not the conductor. The continuous
  Spark Structured Streaming pipeline is the real ETL engine. This DAG
  handles the batch maintenance sequence:
    1. EHR ingestion + pharmacy backfill (Silver layer)
    2. Identity resolution refresh
    3. Full Gold rebuild from refreshed Silver
    4. Post-build health check

Schedule: Manual trigger (run after deploying schema changes or
          after a full Silver rebuild is needed).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

PULSETRACK_DIR = "/opt/airflow/pulsetrack"
BASE = f"cd {PULSETRACK_DIR} && python"

default_args = {
    "owner":            "pulsetrack",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="pulsetrack_full_pipeline",
    default_args=default_args,
    description="PulseTrack: Full Bronze → Silver → Gold pipeline (batch maintenance)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pulsetrack", "orchestrator", "kappa-maintenance"],
) as dag:

    # ── Stage 1: Batch Silver maintenance ───────────────────────────────
    ehr_silver = BashOperator(
        task_id="ehr_silver",
        bash_command=f"{BASE} transformations/bronze_to_silver/ehr_silver.py",
        doc_md="Parse EHR FHIR batches → silver/ehr_conditions, ehr_medications, ehr_lab_results",
    )

    pharmacy_bronze = BashOperator(
        task_id="pharmacy_bronze",
        bash_command=f"{BASE} batch_ingestion/pharmacy_bronze.py",
        doc_md="Drain pharmacy_events Kafka topic → bronze/pharmacy/",
    )

    pharmacy_silver = BashOperator(
        task_id="pharmacy_silver",
        bash_command=f"{BASE} transformations/bronze_to_silver/pharmacy_silver.py",
        doc_md="CDC upsert bronze/pharmacy → silver/pharmacy_prescriptions",
    )

    identity_bridge = BashOperator(
        task_id="identity_bridge",
        bash_command=f"{BASE} transformations/identity_resolution/patient_identity_bridge.py",
        doc_md="Refresh patient_identity_bridge — links EHR MRNs + device accounts",
    )

    # ── Stage 2: Gold Wave 1 — Reference dims ───────────────────────────
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

    # ── Stage 2: Gold Wave 2 — Entity dim ───────────────────────────────
    dim_patient = BashOperator(task_id="dim_patient",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_patient.py")

    # ── Stage 2: Gold Wave 3 — Snowflaked entity dims ───────────────────
    dim_condition = BashOperator(task_id="dim_condition",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_condition.py")
    dim_medication = BashOperator(task_id="dim_medication",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_medication.py")
    dim_device = BashOperator(task_id="dim_device",
        bash_command=f"{BASE} transformations/silver_to_gold/dim_device.py")

    # ── Stage 2: Gold Wave 4 — Facts ────────────────────────────────────
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

    # ── Stage 3: Post-build validation ──────────────────────────────────
    health_check = BashOperator(
        task_id="pipeline_health_check",
        bash_command=f"{BASE} monitoring/pipeline_health_check.py",
        doc_md="FK integrity, row counts, HIPAA scan, identity coverage",
    )

    scd2_audit = BashOperator(
        task_id="scd2_audit",
        bash_command=f"{BASE} monitoring/scd2_audit.py",
        doc_md="SCD2 chain integrity: no gaps/overlaps in effective_date ranges",
    )

    # ── Dependencies: Silver maintenance ────────────────────────────────
    [ehr_silver, pharmacy_silver] >> identity_bridge
    pharmacy_bronze >> pharmacy_silver

    # ── Dependencies: Silver → Gold (all Silver must finish first) ──────
    wave1 = [dim_date, dim_time, dim_condition_category, dim_drug_class, dim_metric]
    identity_bridge >> wave1

    wave1 >> dim_patient

    dim_condition_category >> dim_condition
    dim_drug_class         >> dim_medication
    dim_patient >> [dim_condition, dim_medication, dim_device]

    dim_device  >> fact_vital_reading
    dim_metric  >> [fact_vital_reading, fact_vital_daily, anomaly]
    dim_patient >> [fact_vital_reading, fact_vital_daily, fact_lab, fact_rx, anomaly]
    dim_condition  >> fact_lab
    dim_medication >> fact_rx

    # ── Dependencies: Post-build checks ─────────────────────────────────
    facts = [fact_vital_reading, fact_vital_daily, fact_lab, fact_rx, anomaly]
    facts >> [health_check, scd2_audit]
