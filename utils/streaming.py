"""
Streaming-job helpers shared across Spark applications.

The single public entrypoint is :func:`setup_graceful_shutdown` — register it
once after a streaming query starts so SIGTERM/SIGINT triggers an orderly
``query.stop()`` and (optional) ``spark.stop()`` rather than a hard kill that
leaves the checkpoint half-written.
"""

from __future__ import annotations

import os
import signal
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from logger import get_logger  # noqa: E402

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
