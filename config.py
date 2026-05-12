"""
PulseTrack — Centralized configuration via pydantic-settings.

All paths, endpoints, and tunables live here. Override any field by setting
PT_<FIELD>=value as an environment variable, or by creating a .env file.
Source modules import `settings` and reference attributes — no hardcoded paths.
"""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Environment ─────────────────────────────────────────────────────
    environment: Literal["local", "cloud"] = "local"

    # ── Spark ───────────────────────────────────────────────────────────
    spark_master: str = "local[*]"
    shuffle_partitions: int = 4

    # ── Kafka ───────────────────────────────────────────────────────────
    kafka_bootstrap: str = "localhost:9093"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = ""
    kafka_topic_sensor: str = "sensor_readings"
    kafka_topic_pharmacy: str = "pharmacy_events"
    kafka_topic_dlq: str = "pulsetrack_dlq"

    # ── Schema Registry ─────────────────────────────────────────────────
    schema_registry_url: str = "http://localhost:8081"
    schema_registry_api_key: str = ""
    schema_registry_api_secret: str = ""

    # ── Iceberg catalog ─────────────────────────────────────────────────
    iceberg_catalog_type: str = "hadoop"
    glue_iceberg_warehouse: str = ""

    # ── Lakehouse base paths ────────────────────────────────────────────
    lakehouse_base: str = (
        "/tmp/pulsetrack-lakehouse"  # nosec B108 - dev default, prod sets PT_LAKEHOUSE_BASE to s3://...
    )
    ehr_batch_dir: str = "data/ehr_batches"

    # ── API endpoints ───────────────────────────────────────────────────
    openfda_base_url: str = "https://api.fda.gov"
    hapi_fhir_base_url: str = "https://hapi.fhir.org/baseR4"

    # ── Streaming tunables ──────────────────────────────────────────────
    # Tuned 2026-05-12 after the prior 10M run showed only 5.7% bronze drain:
    #   - 30s × 10k offsets = ~333 rec/s ceiling vs 28k rec/s producer rate.
    #   - 5s × 100k offsets = ~20k rec/s ceiling (60× higher) → drain in <10 min.
    # See postmortem 2026-05-11 / scale_test_results.md §6 ("producer
    # throughput ≠ bronze throughput").
    trigger_interval: str = "5 seconds"
    max_offsets_per_trigger: int = 100000
    watermark_delay: str = "10 minutes"

    # ── Quality thresholds ──────────────────────────────────────────────
    late_arrival_threshold_seconds: int = 7200

    # ── Generator tunables ──────────────────────────────────────────────
    wearable_events_per_second: int = 10
    pharmacy_changes_per_minute: int = 5
    user_count: int = 100

    # ── WHOOP API ───────────────────────────────────────────────────────
    whoop_client_id: str = ""
    whoop_client_secret: str = ""
    whoop_redirect_uri: str = "http://localhost:8765/callback"
    whoop_oauth_authorize_url: str = "https://api.prod.whoop.com/oauth/oauth2/auth"
    whoop_oauth_token_url: str = "https://api.prod.whoop.com/oauth/oauth2/token"
    whoop_api_base_url: str = "https://api.prod.whoop.com/developer"
    whoop_token_path: str = "~/.whoop_tokens.json"
    whoop_offsets_path: str = "~/.whoop_poll_offsets.json"
    whoop_account_id: str = ""
    whoop_user_email: str = ""
    whoop_poll_interval_seconds: int = 900
    whoop_backfill_days: int = 30

    # ── Anthropic Claude API (for ai/ modules) ─────────────────────────
    # Resolves in priority:
    #   1. ANTHROPIC_API_KEY env var
    #   2. .env file (via pydantic-settings)
    #   3. Prefect Block (set at flow-run time)
    # Empty string → ai/ modules raise RuntimeError when called.
    anthropic_api_key: str = ""
    ai_model: str = "claude-sonnet-4-5"
    ai_max_tokens: int = 4096

    # ── Observability ────────────────────────────────────────────────────
    # Channels are best-effort: missing webhook URL means stub-only
    # logging. PagerDuty routing is only used for status='error'.
    slack_webhook_url: str = ""
    pagerduty_routing_key: str = ""
    sns_alert_topic_arn: str = (
        "arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts"
    )
    observability_table: str = "glue_iceberg.pulsetrack_gold_dev.monitor_runs"
    whoop_oauth_scopes: str = (
        "read:recovery read:sleep read:workout read:cycles "
        "read:body_measurement read:profile offline"
    )

    # ── Prometheus metrics ports (one per process to avoid collision) ───
    metrics_port_wearable: int = 8000
    metrics_port_bronze_sensor: int = 8001
    metrics_port_bronze_pharmacy: int = 8002
    metrics_port_openfda: int = 8003
    metrics_port_silver_sensor: int = 8004
    metrics_port_silver_pharmacy: int = 8005
    metrics_port_gold_daily: int = 8006
    metrics_port_gold_reading: int = 8007
    metrics_port_whoop: int = 8008

    # ── Bronze paths ────────────────────────────────────────────────────
    @property
    def bronze_sensor(self) -> str:
        return f"{self.lakehouse_base}/bronze/sensor_readings"

    @property
    def bronze_pharmacy(self) -> str:
        return f"{self.lakehouse_base}/bronze/pharmacy_events"

    # ── Silver paths ────────────────────────────────────────────────────
    @property
    def silver_base(self) -> str:
        return f"{self.lakehouse_base}/silver"

    @property
    def silver_sensor(self) -> str:
        return f"{self.lakehouse_base}/silver/sensor_readings"

    @property
    def silver_ehr_conditions(self) -> str:
        return f"{self.lakehouse_base}/silver/ehr_conditions"

    @property
    def silver_ehr_medications(self) -> str:
        return f"{self.lakehouse_base}/silver/ehr_medications"

    @property
    def silver_ehr_lab_results(self) -> str:
        return f"{self.lakehouse_base}/silver/ehr_lab_results"

    @property
    def silver_pharmacy(self) -> str:
        return f"{self.lakehouse_base}/silver/pharmacy_fills"

    @property
    def silver_identity_bridge(self) -> str:
        return f"{self.lakehouse_base}/silver/identity/patient_identity_bridge"

    # ── Gold paths ──────────────────────────────────────────────────────
    @property
    def gold_base(self) -> str:
        return f"{self.lakehouse_base}/gold"

    @property
    def gold_fact_vital_daily(self) -> str:
        return f"{self.lakehouse_base}/gold/fact_vital_daily_summary"

    @property
    def gold_fact_vital_reading(self) -> str:
        return f"{self.lakehouse_base}/gold/fact_vital_reading"

    @property
    def gold_fact_lab_result(self) -> str:
        return f"{self.lakehouse_base}/gold/fact_lab_result"

    @property
    def gold_dim_patient(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_patient"

    @property
    def gold_dim_device(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_device"

    @property
    def gold_dim_metric(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_metric"

    @property
    def gold_dim_date(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_date"

    @property
    def gold_dim_time(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_time"

    @property
    def gold_dim_condition(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_condition"

    @property
    def gold_dim_condition_category(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_condition_category"

    @property
    def gold_dim_medication(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_medication"

    @property
    def gold_dim_drug_class(self) -> str:
        return f"{self.lakehouse_base}/gold/dim_drug_class"

    # ── Operational paths ───────────────────────────────────────────────
    @property
    def quarantine(self) -> str:
        return f"{self.lakehouse_base}/quarantine"

    @property
    def dlq(self) -> str:
        return f"{self.lakehouse_base}/dlq"

    @property
    def checkpoint_base(self) -> str:
        return f"{self.lakehouse_base}/checkpoints"

    @property
    def checkpoint_bronze_sensor(self) -> str:
        return f"{self.checkpoint_base}/bronze_sensors"

    @property
    def checkpoint_bronze_pharmacy(self) -> str:
        return f"{self.checkpoint_base}/bronze_pharmacy"

    # ── Iceberg / Glue Catalog identifiers ──────────────────────────────
    # The Spark catalog name set in spark_config.py's ``_apply_cloud``.
    # Same name across environments — only the database suffix changes.
    iceberg_catalog: str = "glue_iceberg"

    # AWS deployment environment (dev/staging/prod) — distinct from
    # ``environment`` above which is local/cloud (chooses the Spark config
    # path). The Glue databases created by Terraform use this suffix
    # (e.g. ``pulsetrack_bronze_dev`` from ``environment="dev"`` in
    # ``infrastructure/terraform.tfvars``). Override with ``PT_AWS_ENV``.
    aws_env: str = "dev"

    @property
    def glue_db_bronze(self) -> str:
        return f"pulsetrack_bronze_{self.aws_env}"

    @property
    def glue_db_silver(self) -> str:
        return f"pulsetrack_silver_{self.aws_env}"

    @property
    def glue_db_gold(self) -> str:
        return f"pulsetrack_gold_{self.aws_env}"

    model_config = SettingsConfigDict(
        env_prefix="PT_",
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
