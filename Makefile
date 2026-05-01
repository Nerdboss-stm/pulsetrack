.PHONY: setup lint test \
        generate-ehr generate-fda generate-vitals \
        stream-bronze stream-silver stream-gold \
        batch-silver identity batch-gold \
        compact quality all clean

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

# ── Streaming wearable path (Bronze → Silver → Gold facts) ─────────────────
stream-bronze:
	python streaming/bronze_ingestion.py &
	python streaming/pharmacy_bronze_ingestion.py &  # TODO: implement

stream-silver:
	python transformations/bronze_to_silver/sensor_silver.py --mode=streaming &

stream-gold:
	python transformations/silver_to_gold/fact_vital_daily_summary.py --mode=streaming &
	python transformations/silver_to_gold/fact_vital_reading.py --mode=streaming &

# ── Batch path (EHR + identity + dims) ──────────────────────────────────────
batch-silver:
	python transformations/bronze_to_silver/ehr_silver.py
	python transformations/bronze_to_silver/pharmacy_silver.py  # TODO: implement

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
