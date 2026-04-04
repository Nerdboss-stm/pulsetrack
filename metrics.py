"""
PulseTrack — Prometheus metrics.

Import the instruments and increment/observe them at the relevant boundaries
in pipeline code. Call `start_metrics_server(port)` once per process to expose
/metrics for scraping.
"""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

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

# ── Gauges ──────────────────────────────────────────────────────────────
consumer_lag = Gauge(
    "pt_consumer_lag",
    "Kafka consumer lag",
    ["topic", "partition"],
)
streaming_query_active = Gauge(
    "pt_streaming_query_active",
    "Whether streaming query is running",
    ["query_name"],
)
last_successful_run = Gauge(
    "pt_last_successful_run_timestamp",
    "Timestamp of last successful run",
    ["job_name"],
)


def start_metrics_server(port: int = 8000) -> None:
    """Expose /metrics on `port` for Prometheus scraping."""
    start_http_server(port)
