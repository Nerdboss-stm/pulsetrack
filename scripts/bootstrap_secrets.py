#!/usr/bin/env python3
"""
One-time bootstrap: read local ``.env`` + ``~/.whoop_tokens.json`` and
write the values into AWS Secrets Manager.

Why this exists
---------------
The Terraform ``secrets`` module creates the secret containers but
leaves them empty (TF managing secret VALUES would put them in state,
defeating the purpose). This script populates them once.

Idempotent: re-runs overwrite. Safe to run after rotation.

Usage
-----
    # First run — populate all secrets from your local .env
    python scripts/bootstrap_secrets.py

    # Force-overwrite even if secret already has a value
    python scripts/bootstrap_secrets.py --force

    # Push the WHOOP refresh token from local file (after running OAuth
    # interactively on laptop)
    python scripts/bootstrap_secrets.py --include whoop-tokens

    # Roundtrip-check only — verify reads work, don't write anything
    python scripts/bootstrap_secrets.py --check

    # After confirming the AWS path works, optionally delete the local
    # .env (commented out; never destructive by default)
    python scripts/bootstrap_secrets.py --purge-env
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config import settings  # noqa: E402

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)


# ── Mapping: short-name → JSON payload builder ────────────────────────────
def build_whoop() -> dict[str, str] | None:
    if not settings.whoop_client_id:
        return None
    return {
        "client_id": settings.whoop_client_id,
        "client_secret": settings.whoop_client_secret,
        "redirect_uri": settings.whoop_redirect_uri,
        "account_id": settings.whoop_account_id,
        "user_email": settings.whoop_user_email,
    }


def build_whoop_tokens() -> dict[str, str] | None:
    path = os.path.expanduser(settings.whoop_token_path)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        toks = json.load(f)
    # Normalize to the schema the Terraform module declares.
    return {
        "access_token": str(toks.get("access_token", "")),
        "refresh_token": str(toks.get("refresh_token", "")),
        "expires_at": str(toks.get("expires_at", "")),
    }


def build_anthropic() -> dict[str, str] | None:
    # Anthropic SDK reads ANTHROPIC_API_KEY directly; check both env names.
    key = settings.anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    return {"api_key": key}


def build_snowflake() -> dict[str, str] | None:
    # config.py doesn't have Snowflake fields yet; read PT_SNOWFLAKE_* from env.
    acct = os.environ.get("PT_SNOWFLAKE_ACCOUNT", "")
    if not acct:
        return None
    return {
        "account": acct,
        "user": os.environ.get("PT_SNOWFLAKE_USER", ""),
        "password": os.environ.get("PT_SNOWFLAKE_PASSWORD", ""),
        "role": os.environ.get("PT_SNOWFLAKE_ROLE", ""),
        "warehouse": os.environ.get("PT_SNOWFLAKE_WAREHOUSE", ""),
        "database": os.environ.get("PT_SNOWFLAKE_DATABASE", ""),
        "schema": os.environ.get("PT_SNOWFLAKE_SCHEMA", ""),
    }


def build_slack() -> dict[str, str] | None:
    url = settings.slack_webhook_url
    if not url:
        return None
    return {"webhook_url": url}


def build_pagerduty() -> dict[str, str] | None:
    rk = settings.pagerduty_routing_key
    if not rk:
        # OK to skip — user said no PagerDuty for this run. Mock with placeholder.
        return {"routing_key": "MOCK_NOT_CONFIGURED"}
    return {"routing_key": rk}


BUILDERS = {
    "whoop": build_whoop,
    "whoop-tokens": build_whoop_tokens,
    "anthropic": build_anthropic,
    "snowflake": build_snowflake,
    "slack": build_slack,
    "pagerduty": build_pagerduty,
}


# ── AWS helpers ───────────────────────────────────────────────────────────
def secret_id(env: str, short_name: str) -> str:
    return f"pulsetrack/{env}/{short_name}"


def put_secret(client, env: str, short_name: str, payload: dict, force: bool) -> str:
    sid = secret_id(env, short_name)
    body = json.dumps(payload, separators=(",", ":"))
    try:
        client.put_secret_value(SecretId=sid, SecretString=body)
        return "updated"
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            raise SystemExit(
                f"FATAL: Secret '{sid}' does not exist. Run "
                f"`cd infrastructure && terraform apply` first to create it."
            ) from e
        raise


def check_secret(client, env: str, short_name: str) -> bool:
    """Return True if the secret exists AND has a non-empty value."""
    sid = secret_id(env, short_name)
    try:
        resp = client.get_secret_value(SecretId=sid)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise
    return bool(resp.get("SecretString"))


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=settings.aws_env, help="AWS env (dev/staging/prod)")
    parser.add_argument(
        "--include",
        nargs="+",
        choices=list(BUILDERS),
        default=list(BUILDERS),
        help="Only write these secrets",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing values")
    parser.add_argument("--check", action="store_true", help="Don't write; just verify reads")
    parser.add_argument(
        "--purge-env",
        action="store_true",
        help="DELETE the local .env after successful bootstrap (DANGEROUS)",
    )
    args = parser.parse_args()

    client = boto3.client("secretsmanager")

    if args.check:
        print(f"Roundtrip check for env={args.env}\n")
        all_ok = True
        for short_name in args.include:
            sid = secret_id(args.env, short_name)
            ok = check_secret(client, args.env, short_name)
            status = "OK  " if ok else "MISS"
            print(f"  {status}  {sid}")
            all_ok &= ok
        return 0 if all_ok else 1

    print(f"Bootstrapping secrets to AWS Secrets Manager (env={args.env})\n")
    summary: dict[str, str] = {}

    for short_name in args.include:
        builder = BUILDERS[short_name]
        payload = builder()
        sid = secret_id(args.env, short_name)

        if payload is None:
            summary[short_name] = "SKIP (no value in local env/.env)"
            continue

        if not args.force and check_secret(client, args.env, short_name):
            summary[short_name] = "SKIP (already populated; --force to overwrite)"
            continue

        try:
            status = put_secret(client, args.env, short_name, payload, args.force)
            summary[short_name] = f"OK ({status}, {len(payload)} fields)"
        except SystemExit:
            raise
        except Exception as e:
            summary[short_name] = f"FAIL ({type(e).__name__}: {e})"

    print("Summary:")
    for k, v in summary.items():
        sid = secret_id(args.env, k)
        print(f"  {sid:40s}  {v}")

    failed = [k for k, v in summary.items() if v.startswith("FAIL")]
    if failed:
        print(f"\n{len(failed)} secret(s) failed: {failed}", file=sys.stderr)
        return 1

    if args.purge_env:
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            confirm = input(f"\nDelete {env_path}? type 'yes' to confirm: ")
            if confirm == "yes":
                env_path.unlink()
                print(f"Deleted {env_path}")
            else:
                print("Skipped (no 'yes' input)")

    print("\nDone. Verify with:  python scripts/bootstrap_secrets.py --check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
