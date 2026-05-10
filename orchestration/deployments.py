"""
Prefect Cloud deployments — the cron + interval schedules.

Run via:
  python -m orchestration.deployments

This script uses Prefect's ``serve()`` to register the deployments and
start a local agent that pulls runs from Prefect Cloud. For headless
operation, replace ``serve()`` with ``deploy()`` after `prefect deploy`
has registered the project.

Schedule cadence (matches WHOOP's published pipeline cadence):
  - ehr-daily:        06:00 UTC daily         (batch EHR ingestion)
  - pharmacy-daily:   07:00 UTC daily         (batch pharmacy ingestion)
  - whoop-poll:       every 15 minutes        (WHOOP API polling)
  - streaming-monitor: every 5 minutes        (stream health watchdog)
  - maintenance:      02:00 UTC daily         (OPTIMIZE / VACUUM / cleanup)
  - dbt-weekly:       10:00 UTC every Friday  (WHOOP's deploy day)
  - full-refresh:     no schedule             (manual run-now only)
"""

from __future__ import annotations

from prefect import serve

from orchestration.flows.daily_ehr_pipeline import ehr_daily
from orchestration.flows.daily_pharmacy_pipeline import pharmacy_daily
from orchestration.flows.dbt_pipeline import dbt_weekly
from orchestration.flows.full_refresh import full_refresh
from orchestration.flows.maintenance_pipeline import maintenance
from orchestration.flows.streaming_monitor import streaming_monitor
from orchestration.flows.whoop_poll_pipeline import whoop_poll


def main():
    serve(
        ehr_daily.to_deployment(
            name="ehr-daily",
            cron="0 6 * * *",          # 06:00 UTC daily
            tags=["batch", "ehr", "daily"],
            description="Daily EHR ingestion → silver → identity bridge → gold.",
        ),
        pharmacy_daily.to_deployment(
            name="pharmacy-daily",
            cron="0 7 * * *",          # 07:00 UTC daily (offset from EHR)
            tags=["batch", "pharmacy", "daily"],
            description="Daily OpenFDA poll → bronze → silver → identity → gold.",
        ),
        whoop_poll.to_deployment(
            name="whoop-poll",
            interval=900,              # every 15 minutes
            tags=["streaming", "whoop", "poll"],
            description="Poll WHOOP API for new cycles/sleep/recovery/workout.",
        ),
        streaming_monitor.to_deployment(
            name="streaming-monitor",
            interval=300,              # every 5 minutes
            tags=["monitor", "streaming", "ops"],
            description="Health watchdog for bronze/silver/gold streaming queries.",
        ),
        maintenance.to_deployment(
            name="maintenance-nightly",
            cron="0 2 * * *",          # 02:00 UTC daily
            tags=["maintenance", "ops"],
            description=(
                "Iceberg OPTIMIZE / expire_snapshots / orphan-file cleanup, "
                "dbt source freshness, Glacierbase pending-migration check."
            ),
        ),
        dbt_weekly.to_deployment(
            name="dbt-weekly",
            cron="0 10 * * 5",         # 10:00 UTC every Friday (WHOOP's deploy day)
            tags=["dbt", "weekly", "release"],
            description="Full dbt build + test + snapshot + docs.",
        ),
        full_refresh.to_deployment(
            name="full-refresh",
            tags=["recovery", "manual", "dangerous"],
            description=(
                "Nuclear option — rebuild silver + gold from bronze. "
                "Requires operator_confirmation parameter."
            ),
            # No schedule — manual trigger only.
        ),
    )


if __name__ == "__main__":
    main()
