# PulseTrack Operations Runbook

## Quick Start

```bash
cd /Users/nerdboss-stm/pulsetrack

# 1. Start infrastructure
docker-compose up -d

# 2. Generate test data
python data_generators/wearable_generator.py &
python data_generators/pharmacy_generator.py &

# 3. Run Bronze ingestion (streaming — stays running)
python streaming/bronze_ingestion.py &

# 4. Run Silver transformation (streaming — stays running)
python streaming/sensor_silver_stream.py &   # or the relevant streaming job

# 5. Build Gold layer (batch, run after Silver has data)
python transformations/silver_to_gold/dim_date.py
python transformations/silver_to_gold/dim_time.py
python transformations/silver_to_gold/dim_condition_category.py
python transformations/silver_to_gold/dim_drug_class.py
python transformations/silver_to_gold/dim_metric.py
python transformations/silver_to_gold/dim_patient.py
python transformations/silver_to_gold/dim_condition.py
python transformations/silver_to_gold/dim_medication.py
python transformations/silver_to_gold/dim_device.py
python transformations/silver_to_gold/fact_vital_reading.py
python transformations/silver_to_gold/fact_vital_daily_summary.py
python transformations/silver_to_gold/fact_lab_result.py
python transformations/silver_to_gold/fact_prescription_fill.py
python streaming/anomaly_detector.py

# 6. Run quality checks
python data_quality/run_quality_checks.py
python monitoring/scd2_audit.py
python monitoring/pipeline_health_check.py

# 7. Run tests
pytest tests/test_gold.py -v
```

## Full Pipeline via Airflow

```bash
# Trigger the full pipeline DAG
airflow dags trigger pulsetrack_full_pipeline

# Or run individual DAGs
airflow dags trigger pulsetrack_bronze_to_silver
airflow dags trigger pulsetrack_silver_to_gold
```

## Data Layer Paths

| Layer | Path | Format |
|-------|------|--------|
| Bronze sensors | `/tmp/pulsetrack-lakehouse/bronze/sensor_readings/` | Delta |
| Bronze pharmacy | `/tmp/pulsetrack-lakehouse/bronze/pharmacy/` | Delta |
| Silver sensors | `/tmp/pulsetrack-lakehouse/silver/sensor_readings/` | Delta |
| Silver pharmacy | `/tmp/pulsetrack-lakehouse/silver/pharmacy_prescriptions/` | Delta |
| Silver EHR conditions | `/tmp/pulsetrack-lakehouse/silver/ehr_conditions/` | Delta |
| Silver EHR labs | `/tmp/pulsetrack-lakehouse/silver/ehr_lab_results/` | Delta |
| Silver EHR medications | `/tmp/pulsetrack-lakehouse/silver/ehr_medications/` | Delta |
| Identity bridge | `/tmp/pulsetrack-lakehouse/silver/identity/patient_identity_bridge/` | Delta |
| Gold dims | `/tmp/pulsetrack-lakehouse/gold/dim_*/` | Delta |
| Gold facts | `/tmp/pulsetrack-lakehouse/gold/fact_*/` | Delta |

## Troubleshooting

### "Silver sensor_readings not found"
The streaming Bronze → Silver pipeline hasn't run yet, or the wearable generator hasn't produced data.
```bash
# Check Bronze exists
ls /tmp/pulsetrack-lakehouse/bronze/sensor_readings/

# Check generator is running
python data_generators/wearable_generator.py --count 100  # generate 100 events
```

### "patient_key NULLs in dim_device"
Device accounts in Silver aren't linked in the identity bridge yet.
```bash
# Refresh identity bridge
python transformations/identity_resolution/patient_identity_bridge.py

# Re-run dim_device
python transformations/silver_to_gold/dim_device.py
```

### "FK integrity failures on fact tables"
Gold dims weren't built before facts.
```bash
# Run dims in order first (see Quick Start step 5)
# Then re-run the failing fact table
python transformations/silver_to_gold/fact_vital_reading.py
```

### Streaming lag > 5 minutes
Gold tables are stale. Check the streaming pipeline:
```bash
# Check Spark streaming query status
curl http://localhost:4040/api/v1/applications  # Spark UI

# Check Kafka consumer lag
docker exec pulsetrack-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9093 \
  --describe --group pulsetrack-sensor-group
```

### Delta table corruption
```bash
# Run Delta vacuum to clean up partial writes
python -c "
from streaming.spark_config import get_spark_session
from delta.tables import DeltaTable
spark = get_spark_session('Repair')
path = '/tmp/pulsetrack-lakehouse/gold/fact_vital_reading'
dt = DeltaTable.forPath(spark, path)
dt.vacuum(168)  # 7 days
"
```

## Backfill / Replay

In Kappa architecture, backfill = replay the stream from an earlier offset:

```bash
# Option 1: Reset Bronze checkpoint and re-run Silver
rm -rf /tmp/pulsetrack-lakehouse/checkpoints/bronze_sensors
rm -rf /tmp/pulsetrack-lakehouse/checkpoints/silver_sensors
python streaming/bronze_ingestion.py  # will re-read from Kafka earliest
```

```bash
# Option 2: Replay from Bronze Delta directly (if Kafka data is gone)
# Silver transformations read Bronze Delta — just delete Silver and re-run
rm -rf /tmp/pulsetrack-lakehouse/silver/sensor_readings/_delta_log
python transformations/bronze_to_silver/sensor_silver.py
```

## HIPAA Deletion Workflow

```bash
# 1. Identify patient SHA-256 key from their email
python -c "
import hashlib
email = 'patient@example.com'
key = hashlib.sha256(email.lower().strip().encode()).hexdigest()
print(f'patient_key sha256: {key}')
"

# 2. Remove from all layers (Bronze, Silver, Gold)
# NOTE: Gold records are already de-identified — deletion optional
spark.sql(f\"DELETE FROM delta.\`{SILVER_PATH}/identity/patient_identity_bridge\` WHERE patient_key = '{key}'\")

# 3. Log deletion for audit trail
echo "$(date -u) HIPAA_DELETE patient_key={key} operator=..." >> /var/log/pulsetrack/hipaa_audit.log
```

## Performance Tuning

| Parameter | Default | Tune when |
|-----------|---------|----------|
| `spark.sql.shuffle.partitions` | 4 | Increasing data volume → raise to 8-16 |
| `spark.driver.memory` | 2g | OOM errors → raise to 4g |
| Streaming trigger | availableNow=True | Switch to `processingTime("30 seconds")` for continuous mode |
| Delta OPTIMIZE | not scheduled | Run weekly on fact tables to compact small files |

```bash
# Compact fact_vital_reading (run weekly)
python -c "
from streaming.spark_config import get_spark_session
from delta.tables import DeltaTable
spark = get_spark_session('Optimize')
DeltaTable.forPath(spark, '/tmp/pulsetrack-lakehouse/gold/fact_vital_reading').optimize().executeCompaction()
"
```

## Key Metrics to Monitor

| Metric | Healthy | Alert |
|--------|---------|-------|
| Identity coverage | ≥ 95% | < 80% |
| Gold table freshness | < 1h for streaming facts | > 4h |
| Anomaly alert rate | < 5% of readings | > 10% |
| Streaming lag | < 30s | > 5 min |
| fact_vital_reading rows | Growing daily | Zero for 24h |
