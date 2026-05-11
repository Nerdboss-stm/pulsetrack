"""
Observability CLI — run monitors from the shell.

Usage:
    python -m observability.cli run --spec observability/sql/monitor_spec.yaml
    python -m observability.cli history --table glue_iceberg.pulsetrack_silver_dev.sensor_readings
    python -m observability.cli alert-test
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from observability.alerting import alert_slack, route_alerts
from observability.monitors import MonitorResult, run_all_monitors
from observability.state import persist_results

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _load_spec(path: Path) -> dict:
    """Load the monitor spec from YAML or JSON."""
    body = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml

            return yaml.safe_load(body)
        except ImportError:
            log.error("PyYAML not installed; install with: pip install PyYAML")
            sys.exit(2)
    return json.loads(body)


def cmd_run(args: argparse.Namespace) -> int:
    """Run all monitors declared in the spec, persist + alert."""
    spec = _load_spec(Path(args.spec))
    log.info("loaded spec with %d table(s)", len(spec.get("tables", [])))

    spark = None
    if not args.dry_run:
        try:
            import sys as sys_mod
            sys_mod.path.insert(0, str(Path(__file__).parent.parent))
            from streaming.spark_config import get_spark_session

            spark = get_spark_session("PulseTrack-Observability")
        except Exception as exc:  # noqa: BLE001
            log.error("failed to start Spark — %s; running in stub mode", exc)

    results = run_all_monitors(spec, spark=spark)
    log.info("monitors complete: %d results", len(results))

    if not args.dry_run:
        run_id = persist_results(results, spark=spark)
        log.info("persisted run_id=%s", run_id)

    if not args.no_alert:
        sent = route_alerts(results)
        log.info("alerts routed: %s", sent)

    # Print a summary table.
    print()
    print(f"{'STATUS':<8} {'CHECK':<14} {'TABLE':<60} DETAIL")
    print("-" * 130)
    for r in sorted(results, key=lambda r: (r.status != "error", r.status != "warn")):
        print(
            f"{r.status.upper():<8} {r.check_type:<14} "
            f"{r.table_name[:60]:<60} {r.detail[:50]}"
        )
    error_count = sum(1 for r in results if r.status == "error")
    return 1 if error_count > 0 else 0


def cmd_history(args: argparse.Namespace) -> int:
    """Show the last N monitor_runs rows for a table."""
    log.info("history for %s (last %d runs)", args.table, args.limit)
    try:
        import sys as sys_mod
        sys_mod.path.insert(0, str(Path(__file__).parent.parent))
        from streaming.spark_config import get_spark_session

        spark = get_spark_session("PulseTrack-Observability-History")
        df = spark.sql(f"""
            SELECT run_at, check_type, status, value, threshold, detail
            FROM glue_iceberg.pulsetrack_gold_dev.monitor_runs
            WHERE table_name = '{args.table}'
            ORDER BY run_at DESC
            LIMIT {args.limit}
        """)
        df.show(args.limit, truncate=False)
    except Exception as exc:  # noqa: BLE001
        log.error("history fetch failed: %s", exc)
        return 2
    return 0


def cmd_alert_test(args: argparse.Namespace) -> int:
    """Send a test alert through every channel."""
    test_result = MonitorResult(
        monitor_name="alert_test",
        table_name="(test)",
        check_type="distribution",
        status="warn",
        value=0.42,
        threshold=0.10,
        detail="This is a test alert; ignore.",
    )
    print("Sending test alert...")
    slack_ok = alert_slack(test_result)
    print(f"  slack:     {'OK' if slack_ok else 'SKIPPED/FAILED'}")
    return 0


def main():
    parser = argparse.ArgumentParser(prog="observability")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run monitors from a spec file")
    p_run.add_argument("--spec", required=True, help="Path to monitor spec (yaml/json)")
    p_run.add_argument("--dry-run", action="store_true", help="Don't write to ledger or send alerts")
    p_run.add_argument("--no-alert", action="store_true", help="Skip alert routing")
    p_run.set_defaults(func=cmd_run)

    p_hist = sub.add_parser("history", help="Show monitor_runs history for a table")
    p_hist.add_argument("--table", required=True, help="Table FQN")
    p_hist.add_argument("--limit", type=int, default=20)
    p_hist.set_defaults(func=cmd_history)

    p_alert = sub.add_parser("alert-test", help="Send a test alert to verify channels")
    p_alert.set_defaults(func=cmd_alert_test)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
