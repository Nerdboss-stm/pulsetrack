"""
PulseTrack secrets manager.

Three-tier credential resolution with in-memory caching, structured logging,
and a clear local-dev fallback path.

Resolution order (first hit wins):

    1. AWS Secrets Manager (``pulsetrack/<env>/<service>``)
    2. Process environment (uppercase ``PT_<SERVICE>_<FIELD>`` convention)
    3. ``.env`` via ``config.settings`` (existing pydantic-settings surface)

Cache:
    - In-memory dict, TTL configurable (default 15 min)
    - Per-secret entry, lazily fetched on first ``get_secret(name)``
    - ``prefetch(names)`` warms the cache at startup

Audit:
    - Every call logs name + caller + source + latency (NEVER the value)
    - Structured JSON output via ``logger.get_logger(__name__)``

Local dev:
    - If boto3 isn't installed or the AWS profile isn't configured, the
      Secrets Manager tier is silently skipped (env + .env still work).
    - In tests, pass ``aws_client=None`` to disable AWS resolution entirely.

Boundary contract:
    - All secret values are returned as ``dict[str, str]`` for consistency.
    - Single-string secrets (e.g. ``slack`` webhook URL) are wrapped as
      ``{"webhook_url": "..."}`` — matches the JSON schema the TF module
      provisions.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from config import settings
from logger import get_logger

log = get_logger(__name__)

# Default short-name → ``PT_<UPPER>_<FIELD>`` env-var conventions, used by
# the tier-2 fallback. Order matters only for documentation; we read
# every field independently.
_ENV_FIELD_MAP: dict[str, dict[str, str]] = {
    "whoop": {
        "client_id": "PT_WHOOP_CLIENT_ID",
        "client_secret": "PT_WHOOP_CLIENT_SECRET",
        "redirect_uri": "PT_WHOOP_REDIRECT_URI",
        "account_id": "PT_WHOOP_ACCOUNT_ID",
        "user_email": "PT_WHOOP_USER_EMAIL",
    },
    "whoop-tokens": {
        # Tier-2 fallback: read the on-disk token file the legacy
        # interactive OAuth flow writes (~/.whoop_tokens.json).
        # The fallback is handled in _resolve_env() since the shape
        # doesn't match plain env vars.
    },
    "anthropic": {
        "api_key": "PT_ANTHROPIC_API_KEY",
    },
    "snowflake": {
        "account": "PT_SNOWFLAKE_ACCOUNT",
        "user": "PT_SNOWFLAKE_USER",
        "password": "PT_SNOWFLAKE_PASSWORD",
        "role": "PT_SNOWFLAKE_ROLE",
        "warehouse": "PT_SNOWFLAKE_WAREHOUSE",
        "database": "PT_SNOWFLAKE_DATABASE",
        "schema": "PT_SNOWFLAKE_SCHEMA",
    },
    "slack": {
        "webhook_url": "PT_SLACK_WEBHOOK_URL",
    },
    "pagerduty": {
        "routing_key": "PT_PAGERDUTY_ROUTING_KEY",
    },
}


class SecretsError(Exception):
    """Raised when a secret cannot be resolved from any tier."""


@dataclass
class _CacheEntry:
    value: dict[str, str]
    fetched_at: float
    source: str  # 'aws' | 'env' | 'dotenv'


class SecretsManager:
    """Stateful resolver. Default singleton instance exposed as ``_default``.

    Construct your own for tests or alternative configurations::

        mgr = SecretsManager(aws_client=None, cache_ttl=0)
    """

    def __init__(
        self,
        aws_client: Any = "auto",
        cache_ttl: float = 900.0,  # 15 min
        environment: Optional[str] = None,
    ):
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()
        self._cache_ttl = cache_ttl
        self._env = environment or settings.aws_env

        if aws_client == "auto":
            self._aws = self._build_aws_client()
        else:
            self._aws = aws_client

    # ── AWS Secrets Manager (tier 1) ──────────────────────────────────────
    @staticmethod
    def _build_aws_client() -> Any:
        """Construct a boto3 SecretsManager client, or ``None`` if unavailable.

        We don't want missing boto3 to crash local dev — env + .env still work.
        """
        try:
            import boto3  # noqa: F401  (deferred import; optional dep in test envs)
        except ImportError:
            log.warning("boto3 not installed — AWS Secrets Manager tier disabled")
            return None

        try:
            import boto3

            return boto3.client("secretsmanager")
        except Exception as e:
            log.warning(
                "Could not build SecretsManager client; falling back to env/.env",
                extra={"extra_data": {"error": str(e)}},
            )
            return None

    def _resolve_aws(self, short_name: str) -> Optional[dict[str, str]]:
        """Fetch ``pulsetrack/<env>/<short_name>`` from AWS. Returns None on miss."""
        if self._aws is None:
            return None

        secret_id = f"pulsetrack/{self._env}/{short_name}"
        try:
            resp = self._aws.get_secret_value(SecretId=secret_id)
        except self._aws.exceptions.ResourceNotFoundException:
            return None
        except Exception as e:
            log.warning(
                "AWS Secrets Manager fetch failed; falling back",
                extra={
                    "extra_data": {
                        "secret_id": secret_id,
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }
                },
            )
            return None

        raw = resp.get("SecretString")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.error(
                "SecretString is not valid JSON",
                extra={"extra_data": {"secret_id": secret_id}},
            )
            return None

    # ── Env (tier 2) ──────────────────────────────────────────────────────
    def _resolve_env(self, short_name: str) -> Optional[dict[str, str]]:
        """Resolve from process env. Returns dict if ANY field resolves."""
        if short_name == "whoop-tokens":
            # Special case: read on-disk token file (legacy interactive
            # OAuth flow). New WHOOP token sourcing prefers AWS.
            path = os.path.expanduser(settings.whoop_token_path)
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        return {k: str(v) for k, v in json.load(f).items()}
                except Exception:
                    return None
            return None

        env_map = _ENV_FIELD_MAP.get(short_name, {})
        out: dict[str, str] = {}
        for field, env_var in env_map.items():
            val = os.environ.get(env_var)
            if val:
                out[field] = val
        return out or None

    # ── .env via pydantic-settings (tier 3) ───────────────────────────────
    def _resolve_dotenv(self, short_name: str) -> Optional[dict[str, str]]:
        """Pull from existing ``settings`` (which itself reads ``.env``)."""
        s = settings
        if short_name == "whoop":
            if not s.whoop_client_id:
                return None
            return {
                "client_id": s.whoop_client_id,
                "client_secret": s.whoop_client_secret,
                "redirect_uri": s.whoop_redirect_uri,
                "account_id": s.whoop_account_id,
                "user_email": s.whoop_user_email,
            }
        if short_name == "anthropic":
            if not s.anthropic_api_key:
                return None
            return {"api_key": s.anthropic_api_key}
        if short_name == "slack":
            if not s.slack_webhook_url:
                return None
            return {"webhook_url": s.slack_webhook_url}
        if short_name == "pagerduty":
            if not s.pagerduty_routing_key:
                return None
            return {"routing_key": s.pagerduty_routing_key}
        # Snowflake not in config.py yet (will be added in Phase 1 follow-up
        # update). For now, dotenv tier returns None for Snowflake → env tier
        # must provide PT_SNOWFLAKE_*.
        return None

    # ── Public API ────────────────────────────────────────────────────────
    def get(self, short_name: str, force_refresh: bool = False) -> dict[str, str]:
        """Resolve a secret. Caches in memory; raises on hard miss."""
        now = time.time()

        # Cache hit?
        with self._lock:
            entry = self._cache.get(short_name)
            if entry and not force_refresh and (now - entry.fetched_at) < self._cache_ttl:
                return entry.value

        # Tier 1: AWS
        started = time.time()
        value = self._resolve_aws(short_name)
        source = "aws"

        # Tier 2: env
        if not value:
            value = self._resolve_env(short_name)
            source = "env"

        # Tier 3: .env
        if not value:
            value = self._resolve_dotenv(short_name)
            source = "dotenv"

        if not value:
            raise SecretsError(
                f"Secret '{short_name}' not found in AWS Secrets Manager, "
                f"environment, or .env (env={self._env}). Verify the secret "
                f"exists at 'pulsetrack/{self._env}/{short_name}' or set the "
                f"corresponding PT_ env vars."
            )

        elapsed_ms = (time.time() - started) * 1000
        log.info(
            "secret resolved",
            extra={
                "extra_data": {
                    "secret": short_name,
                    "source": source,
                    "latency_ms": round(elapsed_ms, 1),
                    "fields_count": len(value),
                }
            },
        )

        with self._lock:
            self._cache[short_name] = _CacheEntry(
                value=value, fetched_at=now, source=source
            )
        return value

    def get_field(self, short_name: str, field: str) -> str:
        """Get a single field. Raises KeyError if absent (vs. SecretsError on hard miss)."""
        secret = self.get(short_name)
        if field not in secret:
            raise KeyError(
                f"Secret '{short_name}' has no field '{field}'. "
                f"Available fields: {sorted(secret.keys())}"
            )
        return secret[field]

    def prefetch(self, short_names: list[str]) -> None:
        """Warm the cache for the given secrets. Failures logged + skipped."""
        for name in short_names:
            try:
                self.get(name)
            except SecretsError as e:
                log.warning(
                    "prefetch failed",
                    extra={"extra_data": {"secret": name, "error": str(e)}},
                )

    def invalidate(self, short_name: Optional[str] = None) -> None:
        """Clear cache for one secret (or all). Use after rotation."""
        with self._lock:
            if short_name:
                self._cache.pop(short_name, None)
            else:
                self._cache.clear()


# ── Module-level singleton + thin functional facade ──────────────────────
_default = SecretsManager()


def get_secret(short_name: str, force_refresh: bool = False) -> dict[str, str]:
    """Module-level convenience — uses the default singleton."""
    return _default.get(short_name, force_refresh=force_refresh)


def get_secret_field(short_name: str, field: str) -> str:
    """Module-level convenience — single-field accessor."""
    return _default.get_field(short_name, field)


def prefetch(short_names: list[str]) -> None:
    """Module-level convenience — cache warm-up."""
    _default.prefetch(short_names)
