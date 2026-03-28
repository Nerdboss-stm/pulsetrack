"""
Airflow DAG: Bronze → Silver (maintenance role in Kappa architecture)
======================================================================
In Kappa, Airflow is the JANITOR, not the conductor. The continuous Spark
Structured Streaming pipeline handles the main ETL. Airflow handles:
  - Daily EHR batch ingestion (batch files → Silver via ehr_silver.py)
  - Identity resolution refresh
  - Pharmacy CDC backfill

Schedule: daily at 2AM for EHR. Manual trigger for dev.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PULSETRACK_DIR = "/opt/airflow/pulsetrack"

default_args = {
    "owner":           "pulsetrack",
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="pulsetrack_bronze_to_silver",
    default_args=default_args,
    description="PulseTrack: Batch sources → Silver (EHR, pharmacy, identity)",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pulsetrack", "silver", "kappa-maintenance"],
) as dag:

    ehr_silver = BashOperator(
        task_id="ehr_silver",
        bash_command=f"cd {PULSETRACK_DIR} && python transformations/bronze_to_silver/ehr_silver.py",
        doc_md="Parse EHR FHIR batches → silver/ehr_conditions, ehr_medications, ehr_lab_results",
    )

    pharmacy_bronze = BashOperator(
        task_id="pharmacy_bronze",
        bash_command=f"cd {PULSETRACK_DIR} && python batch_ingestion/pharmacy_bronze.py",
        doc_md="Drain pharmacy_events Kafka topic → bronze/pharmacy/",
    )

    pharmacy_silver = BashOperator(
        task_id="pharmacy_silver",
        bash_command=f"cd {PULSETRACK_DIR} && python transformations/bronze_to_silver/pharmacy_silver.py",
        doc_md="CDC upsert bronze/pharmacy → silver/pharmacy_prescriptions",
    )

    identity_bridge = BashOperator(
        task_id="identity_bridge",
        bash_command=f"cd {PULSETRACK_DIR} && python transformations/identity_resolution/patient_identity_bridge.py",
        doc_md="Refresh patient_identity_bridge — links EHR MRNs to device accounts",
    )

    # EHR and pharmacy run in parallel, both feed identity bridge
    [ehr_silver, pharmacy_silver] >> identity_bridge
    pharmacy_bronze >> pharmacy_silver
