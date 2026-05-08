"""
Streaming-job helpers shared across Spark applications.

Public entrypoints:
* :func:`setup_graceful_shutdown` — SIGTERM/SIGINT → orderly ``query.stop()``.
* :func:`register_metrics_listener` — attach a StreamingQueryListener that
  updates the consumer-lag gauge and processing-latency histogram from each
  micro-batch's progress.
"""

from __future__ import annotations

import json
import os
import signal
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from logger import get_logger  # noqa: E402
from metrics import consumer_lag, processing_latency  # noqa: E402

log = get_logger(__name__)


def setup_graceful_shutdown(query, spark=None) -> None:
    """Install SIGTERM and SIGINT handlers that stop the streaming query."""

    def handler(signum, _frame):
        log.info(
            "Received shutdown signal, stopping streaming query",
            extra={"extra_data": {"signal": signum, "query_id": str(query.id)}},
        )
        try:
            query.stop()
        except Exception:
            log.exception("Error stopping streaming query")
        log.info("Streaming query stopped, flushing checkpoint")
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                log.exception("Error stopping Spark session")
        log.info("Shutdown complete")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def _spark_listener_class():
    """Resolve PySpark's StreamingQueryListener lazily so import cost is paid once."""
    from pyspark.sql.streaming import StreamingQueryListener

    return StreamingQueryListener


def _update_consumer_lag(progress, layer: str) -> None:
    """Pull Kafka source offsets out of ``progress`` and update the gauge.

    Public-by-name so unit tests can exercise the per-batch logic with a fake
    progress object — the StreamingQueryListener's onQueryProgress only fires
    in a live streaming query and is hard to drive from tests.
    """
    duration_ms = getattr(progress, "batchDuration", 0) or 0
    if duration_ms > 0:
        processing_latency.labels(layer=layer).observe(duration_ms / 1000.0)

    for source in getattr(progress, "sources", []) or []:
        description = getattr(source, "description", "") or ""
        if "kafka" not in description.lower():
            continue
        end_offsets = getattr(source, "endOffset", None)
        input_rows = getattr(source, "numInputRows", 0) or 0
        if not end_offsets:
            continue
        try:
            parsed = json.loads(end_offsets) if isinstance(end_offsets, str) else end_offsets
        except json.JSONDecodeError:
            parsed = {}
        for topic, partition_map in parsed.items():
            if not isinstance(partition_map, dict) or not partition_map:
                continue
            per_partition = input_rows / max(1, len(partition_map))
            for partition in partition_map.keys():
                consumer_lag.labels(topic=topic, partition=str(partition)).set(per_partition)


def _build_metrics_listener(layer: str):
    """Construct a listener subclass tied to a specific lakehouse layer label."""
    Base = _spark_listener_class()

    class PulseTrackMetricsListener(Base):  # pragma: no cover - exercised only by live streams
        def onQueryStarted(self, event):  # noqa: N802
            log.info("Streaming query started", extra={"extra_data": {"id": str(event.id)}})

        def onQueryProgress(self, event):  # noqa: N802
            _update_consumer_lag(event.progress, layer)

        def onQueryTerminated(self, event):  # noqa: N802
            log.info(
                "Streaming query terminated",
                extra={"extra_data": {"id": str(event.id), "exception": str(event.exception)}},
            )

    return PulseTrackMetricsListener


def register_metrics_listener(spark, layer: str) -> None:  # pragma: no cover - live-stream only
    """Attach the metrics listener to ``spark`` for queries running on this session."""
    listener_cls = _build_metrics_listener(layer)
    spark.streams.addListener(listener_cls())
    log.info("Metrics listener attached", extra={"extra_data": {"layer": layer}})
