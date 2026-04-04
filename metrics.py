"""
PulseTrack — Prometheus metrics.

Import the instruments and increment them at the relevant boundaries
in pipeline code.
"""
from prometheus_client import Counter

# ── Counters ────────────────────────────────────────────────────────────
records_processed = Counter(
    "pt_records_processed_total",
    "Total records processed",
    ["layer", "source"],
)
records_failed = Counter(
    "pt_records_failed_total",
    "Total records failed",
    ["layer", "source", "reason"],
)
