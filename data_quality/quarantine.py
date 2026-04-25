"""
Quarantine sink for records that PARSE successfully but FAIL quality
validation. These rows are valid syntactically — they just don't meet
business rules (out-of-range vitals, missing required dims, late by more
than the watermark, etc.) — so they bypass the DLQ and land in a separate
Delta table per layer.

The schema is intentionally permissive (`mergeSchema=true`): callers stream
arbitrary DataFrames in, and the quarantine table grows columns over time.
"""
from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import records_quarantined  # noqa: E402
from utils.retry import retry  # noqa: E402

log = get_logger(__name__)


@retry(max_retries=3, backoff_factor=2.0)
def quarantine_records(
    df: DataFrame,
    validity_col: str,
    layer: str,
    source: str,
    reason: Optional[str] = None,
) -> int:
    """
    Filter `df` for rows where ``validity_col == False`` (the quality flag),
    enrich with quarantine metadata, append to the quarantine Delta table,
    and bump ``records_quarantined`` by the number of rows written.

    Returns the row count written.
    """
    bad = (
        df.filter(F.col(validity_col) == F.lit(False))
        .withColumn("quarantine_reason", F.lit(reason or validity_col))
        .withColumn("quarantine_layer",  F.lit(layer))
        .withColumn("quarantine_source", F.lit(source))
        .withColumn("quarantined_at",    F.current_timestamp())
    ).cache()

    n = bad.count()
    if n > 0:
        (
            bad.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(settings.quarantine)
        )
        records_quarantined.labels(layer=layer).inc(n)
        log.warning(
            "Records quarantined",
            extra={"extra_data": {
                "layer": layer,
                "source": source,
                "reason": reason or validity_col,
                "row_count": n,
                "path": settings.quarantine,
            }},
        )
    bad.unpersist()
    return n
