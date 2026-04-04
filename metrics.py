"""
PulseTrack — Prometheus metrics.

Import the instruments and increment/observe them at the relevant boundaries
in pipeline code.
"""
from prometheus_client import Counter, Histogram

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
records_quarantined = Counter(
    "pt_records_quarantined_total",
    "Records sent to quarantine",
    ["layer"],
)

# ── Histograms ──────────────────────────────────────────────────────────
processing_latency = Histogram(
    "pt_processing_latency_seconds",
    "Processing latency",
    ["layer"],
)
batch_size = Histogram(
    "pt_batch_size_records",
    "Batch size in records",
    ["layer"],
)
