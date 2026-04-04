"""
PulseTrack — Centralized configuration via pydantic-settings.

All paths and tunables live here. Override any field by setting
PT_<FIELD>=value as an environment variable, or by creating a .env file.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Lakehouse base path — every Bronze/Silver/Gold table is computed off this.
    lakehouse_base: str = "/tmp/pulsetrack-lakehouse"

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

    model_config = SettingsConfigDict(
        env_prefix="PT_",
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
