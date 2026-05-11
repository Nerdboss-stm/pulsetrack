#!/usr/bin/env python3
"""
Pre-flight credential validator.

Run before any expensive operation (EMR cluster spin-up, 10M scale test,
production deploy). Verifies every credential and service connection
the pipeline depends on.

Exit code is the number of FAIL checks — zero = ready to go.

Usage::

    python scripts/check_credentials.py             # full sweep
    python scripts/check_credentials.py --skip whoop snowflake  # subset
    python scripts/check_credentials.py --json      # machine-readable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


@dataclass
class CheckResult:
    name: str
    status: str  # "PASS" | "FAIL" | "WARN" | "SKIP"
    detail: str = ""
    latency_ms: float = 0.0
    extra: dict = field(default_factory=dict)


# ── Individual checks ─────────────────────────────────────────────────────
def check_aws_profile() -> CheckResult:
    """STS get-caller-identity round-trip."""
    try:
        import boto3

        sts = boto3.client("sts")
        t0 = time.time()
        ident = sts.get_caller_identity()
        ms = (time.time() - t0) * 1000
        return CheckResult(
            name="aws.sts",
            status="PASS",
            detail=f"account={ident['Account']} arn={ident['Arn']}",
            latency_ms=ms,
        )
    except Exception as e:
        return CheckResult(name="aws.sts", status="FAIL", detail=f"{type(e).__name__}: {e}")


def check_aws_region() -> CheckResult:
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    if not region:
        return CheckResult(
            name="aws.region",
            status="FAIL",
            detail="AWS_DEFAULT_REGION not set (expected us-east-1)",
        )
    if region != "us-east-1":
        return CheckResult(
            name="aws.region",
            status="WARN",
            detail=f"region={region} (project expects us-east-1)",
        )
    return CheckResult(name="aws.region", status="PASS", detail=f"region={region}")


def check_s3_bucket() -> CheckResult:
    """Confirm the lakehouse bucket exists + writable (1-byte canary)."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        return CheckResult(name="aws.s3", status="FAIL", detail="boto3 not installed")

    # Get bucket name from terraform output (most reliable)
    import subprocess

    try:
        bucket = subprocess.check_output(
            ["terraform", "output", "-raw", "lakehouse_bucket_name"],
            cwd=REPO_ROOT / "infrastructure",
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return CheckResult(
            name="aws.s3",
            status="FAIL",
            detail="terraform output failed — run `terraform apply` first",
        )

    s3 = boto3.client("s3")
    canary_key = "_preflight/canary.txt"
    t0 = time.time()
    try:
        s3.put_object(Bucket=bucket, Key=canary_key, Body=b"ok")
        s3.delete_object(Bucket=bucket, Key=canary_key)
        ms = (time.time() - t0) * 1000
        return CheckResult(
            name="aws.s3",
            status="PASS",
            detail=f"bucket={bucket} put+delete OK",
            latency_ms=ms,
        )
    except ClientError as e:
        return CheckResult(name="aws.s3", status="FAIL", detail=f"bucket={bucket} {e}")


def check_glue_databases() -> CheckResult:
    """Confirm bronze/silver/gold databases exist."""
    try:
        import boto3
    except ImportError:
        return CheckResult(name="aws.glue", status="FAIL", detail="boto3 not installed")

    glue = boto3.client("glue")
    aws_env = os.environ.get("PT_AWS_ENV", "dev")
    expected = [f"pulsetrack_{layer}_{aws_env}" for layer in ("bronze", "silver", "gold")]
    missing = []
    for db in expected:
        try:
            glue.get_database(Name=db)
        except glue.exceptions.EntityNotFoundException:
            missing.append(db)
        except Exception as e:
            return CheckResult(name="aws.glue", status="FAIL", detail=f"glue api {e}")

    if missing:
        return CheckResult(
            name="aws.glue", status="FAIL", detail=f"missing databases: {missing}"
        )
    return CheckResult(name="aws.glue", status="PASS", detail=f"all 3 dbs present: {expected}")


def check_msk_cluster() -> CheckResult:
    """Confirm MSK Serverless ACTIVE."""
    try:
        import boto3
    except ImportError:
        return CheckResult(name="aws.msk", status="FAIL", detail="boto3 not installed")

    kafka = boto3.client("kafka")
    try:
        clusters = kafka.list_clusters_v2()["ClusterInfoList"]
    except Exception as e:
        return CheckResult(name="aws.msk", status="FAIL", detail=f"list_clusters_v2 {e}")

    pt = [c for c in clusters if "pulsetrack" in c["ClusterName"].lower()]
    if not pt:
        return CheckResult(
            name="aws.msk", status="FAIL", detail="no pulsetrack MSK cluster found"
        )
    c = pt[0]
    state = c["State"]
    status = "PASS" if state == "ACTIVE" else "FAIL"
    return CheckResult(
        name="aws.msk",
        status=status,
        detail=f"name={c['ClusterName']} state={state}",
    )


def check_secret(short_name: str) -> CheckResult:
    """Resolve a secret via pt_secrets — exercises the full 3-tier path."""
    try:
        from pt_secrets import get_secret, SecretsError
    except ImportError as e:
        return CheckResult(name=f"secret.{short_name}", status="FAIL", detail=f"import {e}")

    t0 = time.time()
    try:
        secret = get_secret(short_name)
        ms = (time.time() - t0) * 1000
        return CheckResult(
            name=f"secret.{short_name}",
            status="PASS",
            detail=f"{len(secret)} fields resolved",
            latency_ms=ms,
            extra={"fields": sorted(secret.keys())},
        )
    except SecretsError as e:
        return CheckResult(name=f"secret.{short_name}", status="FAIL", detail=str(e)[:200])


def check_snowflake_connect() -> CheckResult:
    """Connect to Snowflake using secret bundle. Runs SELECT CURRENT_VERSION()."""
    try:
        from pt_secrets import get_secret
        import snowflake.connector  # type: ignore
    except ImportError as e:
        return CheckResult(
            name="snowflake.connect",
            status="FAIL",
            detail=f"snowflake-connector-python not installed: {e}",
        )

    try:
        creds = get_secret("snowflake")
    except Exception as e:
        return CheckResult(
            name="snowflake.connect", status="FAIL", detail=f"secret fetch failed: {e}"
        )

    needed = {"account", "user", "password", "role", "warehouse", "database"}
    if not needed.issubset(creds.keys()):
        return CheckResult(
            name="snowflake.connect",
            status="FAIL",
            detail=f"missing fields: {needed - set(creds.keys())}",
        )

    try:
        t0 = time.time()
        with snowflake.connector.connect(
            account=creds["account"],
            user=creds["user"],
            password=creds["password"],
            role=creds["role"],
            warehouse=creds["warehouse"],
            database=creds["database"],
            login_timeout=15,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT CURRENT_VERSION()")
                ver = cur.fetchone()[0]
        ms = (time.time() - t0) * 1000
        return CheckResult(
            name="snowflake.connect",
            status="PASS",
            detail=f"version={ver}",
            latency_ms=ms,
        )
    except Exception as e:
        return CheckResult(
            name="snowflake.connect", status="FAIL", detail=f"{type(e).__name__}: {e}"
        )


def check_snowflake_views() -> CheckResult:
    """Verify the consumer views from prompt 8 exist and AUTO_REFRESH is on."""
    try:
        from pt_secrets import get_secret
        import snowflake.connector  # type: ignore
    except ImportError:
        return CheckResult(name="snowflake.views", status="SKIP", detail="connector unavailable")

    try:
        creds = get_secret("snowflake")
    except Exception as e:
        return CheckResult(name="snowflake.views", status="SKIP", detail=f"creds: {e}")

    try:
        with snowflake.connector.connect(
            account=creds["account"],
            user=creds["user"],
            password=creds["password"],
            role=creds["role"],
            warehouse=creds["warehouse"],
            database=creds["database"],
            login_timeout=15,
        ) as conn:
            with conn.cursor() as cur:
                # 1. Required views
                cur.execute(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS "
                    "WHERE TABLE_SCHEMA = 'ANALYTICS' "
                    "  AND TABLE_NAME IN ('VW_PATIENT_HEALTH_360', 'VW_ANOMALY_DASHBOARD', 'VW_WHOOP_MY_HEALTH')"
                )
                found_views = {r[0] for r in cur.fetchall()}
                missing = {"VW_PATIENT_HEALTH_360", "VW_ANOMALY_DASHBOARD", "VW_WHOOP_MY_HEALTH"} - found_views

                # 2. Iceberg tables AUTO_REFRESH
                cur.execute(
                    "SELECT TABLE_NAME, AUTO_REFRESH FROM INFORMATION_SCHEMA.ICEBERG_TABLES "
                    "WHERE TABLE_SCHEMA IN ('BRONZE','SILVER','GOLD')"
                )
                iceberg = list(cur.fetchall())
                no_refresh = [r[0] for r in iceberg if r[1] != 'TRUE']

        problems = []
        if missing:
            problems.append(f"missing views: {missing}")
        if no_refresh:
            problems.append(f"AUTO_REFRESH off: {no_refresh}")

        if problems:
            return CheckResult(
                name="snowflake.views", status="WARN", detail="; ".join(problems)
            )
        return CheckResult(
            name="snowflake.views",
            status="PASS",
            detail=f"3 views OK, {len(iceberg)} iceberg tables with AUTO_REFRESH",
        )
    except Exception as e:
        return CheckResult(
            name="snowflake.views",
            status="WARN",
            detail=f"could not validate: {type(e).__name__}: {e}",
        )


def check_anthropic() -> CheckResult:
    """Lightweight ping to Anthropic API (1-token completion)."""
    try:
        from pt_secrets import get_secret_field
        import anthropic  # type: ignore
    except ImportError as e:
        return CheckResult(name="anthropic.ping", status="FAIL", detail=f"import {e}")

    try:
        key = get_secret_field("anthropic", "api_key")
    except Exception as e:
        return CheckResult(name="anthropic.ping", status="FAIL", detail=f"key: {e}")

    try:
        client = anthropic.Anthropic(api_key=key)
        t0 = time.time()
        client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1,
            messages=[{"role": "user", "content": "ok"}],
        )
        ms = (time.time() - t0) * 1000
        return CheckResult(name="anthropic.ping", status="PASS", latency_ms=ms)
    except Exception as e:
        return CheckResult(
            name="anthropic.ping", status="FAIL", detail=f"{type(e).__name__}: {e}"
        )


def check_slack() -> CheckResult:
    """POST a test payload to the Slack webhook."""
    try:
        from pt_secrets import get_secret_field
        import urllib.request
    except ImportError as e:
        return CheckResult(name="slack.webhook", status="FAIL", detail=str(e))

    try:
        url = get_secret_field("slack", "webhook_url")
    except Exception as e:
        return CheckResult(name="slack.webhook", status="WARN", detail=f"url: {e}")

    payload = json.dumps({"text": ":white_check_mark: PulseTrack check_credentials ping"}).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        t0 = time.time()
        resp = urllib.request.urlopen(req, timeout=10)
        ms = (time.time() - t0) * 1000
        status = "PASS" if resp.status == 200 else "FAIL"
        return CheckResult(name="slack.webhook", status=status, detail=f"http {resp.status}", latency_ms=ms)
    except Exception as e:
        return CheckResult(
            name="slack.webhook", status="FAIL", detail=f"{type(e).__name__}: {e}"
        )


def check_whoop_tokens() -> CheckResult:
    """Verify the whoop-tokens secret has a non-expired access_token (no API call)."""
    try:
        from pt_secrets import get_secret
        from datetime import datetime, timezone
    except ImportError as e:
        return CheckResult(name="whoop.tokens", status="FAIL", detail=str(e))

    try:
        toks = get_secret("whoop-tokens")
    except Exception as e:
        return CheckResult(name="whoop.tokens", status="FAIL", detail=str(e)[:200])

    access = toks.get("access_token", "")
    exp = toks.get("expires_at", "")
    if not access:
        return CheckResult(name="whoop.tokens", status="FAIL", detail="access_token empty")
    if not exp:
        return CheckResult(name="whoop.tokens", status="WARN", detail="no expires_at")

    try:
        exp_dt = datetime.fromtimestamp(float(exp), tz=timezone.utc)
        delta = (exp_dt - datetime.now(timezone.utc)).total_seconds()
        if delta < 0:
            return CheckResult(
                name="whoop.tokens",
                status="WARN",
                detail=f"access_token expired {-delta:.0f}s ago — refresh will fire on next API call",
            )
        return CheckResult(
            name="whoop.tokens",
            status="PASS",
            detail=f"access_token valid for {delta:.0f}s",
        )
    except (ValueError, TypeError):
        return CheckResult(
            name="whoop.tokens", status="WARN", detail=f"unparseable expires_at: {exp!r}"
        )


# ── Check registry ────────────────────────────────────────────────────────
def all_checks() -> dict[str, Callable[[], CheckResult]]:
    return {
        "aws-sts": check_aws_profile,
        "aws-region": check_aws_region,
        "aws-s3": check_s3_bucket,
        "aws-glue": check_glue_databases,
        "aws-msk": check_msk_cluster,
        "secret-whoop": lambda: check_secret("whoop"),
        "secret-anthropic": lambda: check_secret("anthropic"),
        "secret-snowflake": lambda: check_secret("snowflake"),
        "secret-slack": lambda: check_secret("slack"),
        "whoop-tokens": check_whoop_tokens,
        "snowflake-connect": check_snowflake_connect,
        "snowflake-views": check_snowflake_views,
        "anthropic-ping": check_anthropic,
        "slack-webhook": check_slack,
    }


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip", nargs="*", default=[], help="Check names to skip")
    parser.add_argument("--only", nargs="*", help="Only run these checks")
    parser.add_argument("--json", action="store_true", help="JSON output, one line per check")
    parser.add_argument("--no-color", action="store_true")
    args = parser.parse_args()

    color = sys.stdout.isatty() and not args.no_color
    GREEN = "\033[32m" if color else ""
    RED = "\033[31m" if color else ""
    YELLOW = "\033[33m" if color else ""
    GREY = "\033[90m" if color else ""
    RESET = "\033[0m" if color else ""

    badges = {
        "PASS": f"{GREEN}PASS{RESET}",
        "FAIL": f"{RED}FAIL{RESET}",
        "WARN": f"{YELLOW}WARN{RESET}",
        "SKIP": f"{GREY}SKIP{RESET}",
    }

    checks = all_checks()
    if args.only:
        checks = {k: v for k, v in checks.items() if k in args.only}
    for s in args.skip:
        checks.pop(s, None)

    fail = 0
    if not args.json:
        print(f"PulseTrack pre-flight ({len(checks)} checks)\n")

    for name, fn in checks.items():
        try:
            res = fn()
        except Exception as e:
            res = CheckResult(name=name, status="FAIL", detail=f"unexpected: {e}")
        if args.json:
            print(json.dumps(res.__dict__, default=str))
        else:
            print(
                f"  [{badges[res.status]}] {res.name:24s}  "
                f"{res.detail}{f'  ({res.latency_ms:.0f}ms)' if res.latency_ms else ''}"
            )
        if res.status == "FAIL":
            fail += 1

    if not args.json:
        print(f"\n{fail} FAIL / {len(checks)} checks")
        if fail == 0:
            print("Ready for the scale test.")
    return fail


if __name__ == "__main__":
    sys.exit(main())
