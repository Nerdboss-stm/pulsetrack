"""
PulseTrack — Centralized configuration via pydantic-settings.

All paths and tunables live here. Override any field by setting
PT_<FIELD>=value as an environment variable, or by creating a .env file.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Lakehouse base path — every Bronze/Silver/Gold table is computed off this.
    lakehouse_base: str = "/tmp/pulsetrack-lakehouse"

    model_config = SettingsConfigDict(
        env_prefix="PT_",
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
