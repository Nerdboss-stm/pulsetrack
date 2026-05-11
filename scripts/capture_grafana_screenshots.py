#!/usr/bin/env python3
"""
Grafana / CloudWatch dashboard screenshot capture.

Headless screenshot of the 7 panels needed for the scale-test results doc.

Sources (in priority):
  1. Grafana render API (preferred — if PT_GRAFANA_URL is set)
  2. CloudWatch get-metric-widget-image (fallback — works without Grafana)
  3. Skip + emit a README placeholder if neither is configured

For each panel we get a PNG at the test window. Filenames match the
references in ``benchmarks/scale_test_report.py``'s output.

Usage::

    python scripts/capture_grafana_screenshots.py \\
        --output-dir docs/screenshots \\
        --test-start 2026-05-10T14:30:00Z
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path


PANELS = [
    {
        "filename": "throughput.png",
        "grafana_uid": "pulsetrack-throughput",
        "grafana_panel_id": 1,
        "cw_widget": {
            "metrics": [
                ["AWS/Kafka", "BytesInPerSec", "Cluster Name", "pulsetrack-dev-msk"],
                [".", "MessagesInPerSec", ".", "."],
            ],
            "title": "MSK ingress (throughput)",
        },
    },
    {
        "filename": "consumer_lag.png",
        "grafana_uid": "pulsetrack-lag",
        "grafana_panel_id": 1,
        "cw_widget": {
            "metrics": [["pulsetrack", "consumer_lag", "layer", "bronze"]],
            "title": "Kafka consumer-group lag",
        },
    },
    {
        "filename": "silver_processing_latency.png",
        "grafana_uid": "pulsetrack-latency",
        "grafana_panel_id": 1,
        "cw_widget": {
            "metrics": [["pulsetrack", "processing_latency_ms", "layer", "silver"]],
            "title": "Silver microbatch p50/p95/p99 (ms)",
        },
    },
    {
        "filename": "iceberg_file_count.png",
        "grafana_uid": "pulsetrack-files",
        "grafana_panel_id": 1,
        "cw_widget": {
            "metrics": [["pulsetrack", "iceberg_data_files", "table", "silver_sensor"]],
            "title": "Iceberg data-file count",
        },
    },
    {
        "filename": "chaos_recovery.png",
        "grafana_uid": "pulsetrack-chaos",
        "grafana_panel_id": 1,
        "cw_widget": {
            "metrics": [
                ["pulsetrack", "yarn_running_containers", "layer", "silver"],
                ["pulsetrack", "spark_active_tasks", "."],
            ],
            "title": "Silver YARN containers + Spark tasks",
        },
    },
    {
        "filename": "cost_burn.png",
        "grafana_uid": "pulsetrack-cost",
        "grafana_panel_id": 1,
        "cw_widget": {
            "metrics": [["AWS/Billing", "EstimatedCharges", "Currency", "USD"]],
            "title": "AWS estimated charges (lagging)",
        },
    },
    {
        "filename": "prefect_flow_status.png",
        "grafana_uid": "pulsetrack-prefect",
        "grafana_panel_id": 1,
        "cw_widget": None,  # Prefect-only; no CW fallback
    },
]


def grafana_render(panel: dict, test_start: datetime, test_end: datetime, out: Path) -> bool:
    """Use the Grafana render API. Requires PT_GRAFANA_URL + PT_GRAFANA_API_KEY."""
    base = os.environ.get("PT_GRAFANA_URL", "")
    api_key = os.environ.get("PT_GRAFANA_API_KEY", "")
    if not base or not api_key:
        return False

    params = {
        "panelId": panel["grafana_panel_id"],
        "from": int(test_start.timestamp() * 1000),
        "to": int(test_end.timestamp() * 1000),
        "width": 1200,
        "height": 600,
        "tz": "UTC",
    }
    url = f"{base}/render/d-solo/{panel['grafana_uid']}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            out.write_bytes(resp.read())
        return True
    except Exception as e:
        print(f"  grafana render failed for {panel['filename']}: {e}", file=sys.stderr)
        return False


def cloudwatch_widget(panel: dict, test_start: datetime, test_end: datetime, out: Path) -> bool:
    """Use AWS CloudWatch get-metric-widget-image as fallback."""
    if not panel.get("cw_widget"):
        return False
    try:
        import boto3
    except ImportError:
        return False
    cw = boto3.client("cloudwatch")

    widget = {
        "metrics": panel["cw_widget"]["metrics"],
        "title": panel["cw_widget"]["title"],
        "start": test_start.isoformat(),
        "end": test_end.isoformat(),
        "width": 1200,
        "height": 600,
        "stat": "Average",
        "period": 60,
    }
    try:
        resp = cw.get_metric_widget_image(MetricWidget=json.dumps(widget))
        out.write_bytes(resp["MetricWidgetImage"])
        return True
    except Exception as e:
        print(f"  cw widget failed for {panel['filename']}: {e}", file=sys.stderr)
        return False


def write_placeholder(panel: dict, out: Path) -> None:
    """Drop a marker file explaining what should have been captured."""
    txt = (
        f"PLACEHOLDER: {panel['filename']}\n\n"
        f"Source: Grafana panel {panel['grafana_uid']} / panel {panel['grafana_panel_id']}\n"
        f"Or: CloudWatch widget — {panel.get('cw_widget', {}).get('title', 'n/a')}\n\n"
        "Configure either:\n"
        "  PT_GRAFANA_URL + PT_GRAFANA_API_KEY (Grafana render API)\n"
        "  or AWS credentials with CloudWatch:GetMetricWidgetImage permission\n"
        "and re-run scripts/capture_grafana_screenshots.py.\n"
    )
    out.write_text(txt)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="docs/screenshots")
    parser.add_argument(
        "--test-start", required=True, help="ISO-8601 test start (UTC)"
    )
    parser.add_argument(
        "--test-end", default=None, help="ISO-8601 test end (defaults to now)"
    )
    args = parser.parse_args()

    start = datetime.fromisoformat(args.test_start.replace("Z", "+00:00"))
    end = (
        datetime.fromisoformat(args.test_end.replace("Z", "+00:00"))
        if args.test_end
        else datetime.now(timezone.utc)
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[screenshots] window: {start.isoformat()} → {end.isoformat()}")
    print(f"[screenshots] output: {out_dir}")
    print(f"[screenshots] panels: {len(PANELS)}")

    captured = 0
    placeholders = 0
    for panel in PANELS:
        out_path = out_dir / panel["filename"]
        # Priority 1: Grafana
        if grafana_render(panel, start, end, out_path):
            print(f"  ✓ grafana → {out_path}")
            captured += 1
            continue
        # Priority 2: CloudWatch
        if cloudwatch_widget(panel, start, end, out_path):
            print(f"  ✓ cloudwatch → {out_path}")
            captured += 1
            continue
        # Fallback: placeholder
        placeholder_path = out_dir / f"{panel['filename']}.placeholder.txt"
        write_placeholder(panel, placeholder_path)
        print(f"  · placeholder → {placeholder_path}")
        placeholders += 1

    # Always emit a README in the screenshots dir
    readme = out_dir / "README.md"
    if not readme.exists():
        readme.write_text(
            "# Scale test screenshots\n\n"
            f"Generated by `scripts/capture_grafana_screenshots.py` for the test "
            f"window `{start.isoformat()}` → `{end.isoformat()}`.\n\n"
            "PNGs referenced from `docs/scale_test_results.md`. If you see "
            "`.placeholder.txt` files instead, configure `PT_GRAFANA_URL` or "
            "AWS CloudWatch credentials and re-run.\n"
        )

    print(f"[screenshots] captured={captured} placeholders={placeholders}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
