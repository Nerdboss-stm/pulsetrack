"""
Run every registered Great Expectations suite against the latest contents of
the corresponding Delta tables. Useful as a one-shot quality audit:

    make quality   # → python data_quality/run_all_suites.py

Each suite reads its source table, projects to the column shape the suite
expects, and runs validate(). Exits with code 1 if any suite fails so it can
gate downstream steps in CI.
"""
from __future__ import annotations

import os
import sys
from typing import Callable

from delta.tables import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings  # noqa: E402
from data_quality.expectations.bronze_sensor_suite import (  # noqa: E402
    SUITE_NAME as BRONZE_SUITE,
    prepare_for_validation as prepare_bronze,
)
from data_quality.expectations.gold_vitals_suite import (  # noqa: E402
    SUITE_NAME as GOLD_SUITE,
    prepare_for_validation as prepare_gold,
)
from data_quality.expectations.silver_sensor_suite import (  # noqa: E402
    SUITE_NAME as SILVER_SUITE,
    prepare_for_validation as prepare_silver,
)
from data_quality.gx_config import get_context, validate  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)

Audit = tuple[str, str, str, str, Callable]
AUDITS: list[Audit] = [
    # (suite_name, layer label, source label, table path, projection fn)
    (BRONZE_SUITE, "bronze", "sensor",      settings.bronze_sensor,           prepare_bronze),
    (SILVER_SUITE, "silver", "sensor",      settings.silver_sensor,           prepare_silver),
    (GOLD_SUITE,   "gold",   "vital_daily", settings.gold_fact_vital_daily,   prepare_gold),
]


def main() -> int:
    spark = get_spark_session("PulseTrack-QualityAudit")
    get_context()

    failures = 0
    for suite_name, layer, source, path, prepare in AUDITS:
        if not DeltaTable.isDeltaTable(spark, path):
            log.warning(
                "Skipping suite — table missing",
                extra={"extra_data": {"suite": suite_name, "path": path}},
            )
            continue
        df = spark.read.format("delta").load(path)
        prepared = prepare(df)
        ok = validate(prepared, suite_name=suite_name, layer=layer, source=source)
        if not ok:
            failures += 1

    if failures:
        log.error(
            "Quality audit failed",
            extra={"extra_data": {"failed_suites": failures}},
        )
        return 1
    log.info("Quality audit passed", extra={"extra_data": {"audits": len(AUDITS)}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
