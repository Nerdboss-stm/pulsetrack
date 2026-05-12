.PHONY: setup lint test \
        generate-ehr generate-fda generate-vitals \
        whoop-auth whoop-poll \
        stream-bronze stream-silver stream-gold \
        batch-silver identity batch-gold \
        compact quality all clean \
        cloud-upload cloud-bronze cloud-silver cloud-identity cloud-gold cloud-all \
        snowflake-state snowflake-views snowflake-samples

# ── Setup & quality ─────────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt
	pre-commit install

lint:
	black --check .
	ruff check .

test:
	pytest tests/ --ignore=tests/integration -v --cov=. --cov-report=term-missing

# ── Real-data sources ───────────────────────────────────────────────────────
generate-ehr:
	python data_generators/fhir_producer.py

generate-fda:
	python data_generators/openfda_producer.py &

generate-vitals:
	python data_generators/wearable_generator.py &

# ── WHOOP API connector ────────────────────────────────────────────────────
whoop-auth:
	python -c "from data_generators.whoop_api.auth import authorize_interactive; authorize_interactive()"

whoop-poll:
	python data_generators/whoop_api/producer.py &

# ── Streaming wearable path (Bronze → Silver → Gold facts) ─────────────────
stream-bronze:
	python streaming/bronze_ingestion.py &
	python streaming/pharmacy_bronze_ingestion.py &

stream-silver:
	python transformations/bronze_to_silver/sensor_silver.py --mode=streaming &

stream-gold:
	python transformations/silver_to_gold/fact_vital_daily_summary.py --mode=streaming &
	python transformations/silver_to_gold/fact_vital_reading.py --mode=streaming &

# ── Batch path (EHR + identity + dims) ──────────────────────────────────────
batch-silver:
	python transformations/bronze_to_silver/ehr_silver.py
	python transformations/bronze_to_silver/pharmacy_silver.py --mode=batch

identity:
	python transformations/identity_resolution/patient_identity_bridge.py

batch-gold:
	python transformations/silver_to_gold/dim_condition_category.py
	python transformations/silver_to_gold/dim_condition.py
	python transformations/silver_to_gold/dim_drug_class.py
	python transformations/silver_to_gold/dim_medication.py
	python transformations/silver_to_gold/dim_metric.py
	python transformations/silver_to_gold/dim_date.py
	python transformations/silver_to_gold/dim_time.py
	python transformations/silver_to_gold/dim_device.py
	python transformations/silver_to_gold/dim_patient.py
	python transformations/silver_to_gold/fact_lab_result.py

# ── Operations ─────────────────────────────────────────────────────────────
compact:
	python maintenance/compaction.py

quality:
	python data_quality/run_all_suites.py

all: batch-silver identity batch-gold quality

clean:
	rm -rf /tmp/pulsetrack-lakehouse/*

# ── Cloud targets (run on EMR; require infrastructure/terraform.tfstate) ───
CLOUD_BUCKET = $(shell cd infrastructure && terraform output -raw lakehouse_bucket_name)

cloud-upload:
	tar czf /tmp/pulsetrack.tar.gz \
	    --exclude='.git' --exclude='venv' --exclude='.venv' \
	    --exclude='__pycache__' --exclude='*.pyc' \
	    --exclude='infrastructure/.terraform' --exclude='spark-warehouse' .
	aws s3 cp /tmp/pulsetrack.tar.gz s3://$(CLOUD_BUCKET)/code/

cloud-bronze:
	bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py

cloud-silver:
	bash scripts/submit_emr_step.sh transformations/bronze_to_silver/sensor_silver.py
	bash scripts/submit_emr_step.sh transformations/bronze_to_silver/ehr_silver.py

cloud-identity:
	bash scripts/submit_emr_step.sh transformations/identity_resolution/patient_identity_bridge.py

cloud-gold:
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_condition_category.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_condition.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_drug_class.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_medication.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_metric.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_date.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_time.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_device.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/dim_patient.py
	bash scripts/submit_emr_step.sh transformations/silver_to_gold/fact_lab_result.py

cloud-all: cloud-upload cloud-silver cloud-identity cloud-gold

# ── Snowflake serving layer (Iceberg-backed views) ──────────────────────────
# These targets read PULSETRACK gold tables on S3+Glue via Snowflake Iceberg.
# Credentials come from AWS Secrets Manager via pt_secrets (no .env required).
# Set AWS_PROFILE first: e.g. `AWS_PROFILE=pulsetrack make snowflake-state`.
snowflake-state:
	python3 scripts/snowflake_state.py

snowflake-views:
	python3 scripts/provision_snowflake_views.py

snowflake-samples:
	python3 scripts/snowflake_view_samples.py
