"""
Concurrency lock for the migration framework — DynamoDB conditional writes.

Mirrors the WHOOP Glacierbase blog post:

    "Glacierbase also acquires a lock before starting a migration,
    preventing concurrent schema updates on the same catalog. If another
    process is running, Glacierbase raises a clear concurrency error to
    guarantee atomic, isolated schema evolution."

DynamoDB is the AWS-native primitive for distributed locks: a single-row
write with ``ConditionExpression: attribute_not_exists(catalog)`` either
succeeds (we hold the lock) or returns ``ConditionalCheckFailedException``
(someone else holds it). DynamoDB's TTL feature auto-expires stale locks
if the holding process crashes without releasing.

Implementation choices:
  * Lock key is the catalog name. One lock per catalog — matches the
    "catalog" granularity in the WHOOP post.
  * Lock value carries holder identity (``USER`` env), pid, hostname,
    acquired-at timestamp. Visible in DynamoDB console for debugging
    "who's holding the lock right now?".
  * Stale-lock TTL is configurable per catalog (``lock.ttlSeconds`` in the
    YAML). Default 1800 = 30 min; long migrations can override.
  * Release is a conditional delete that only deletes our own lock row
    (matched by holder identity) — so a re-acquired lock by another
    process isn't accidentally released by ours after we time out.
  * The lock module is import-isolated from boto3 — boto3 is imported
    only inside ``acquire`` / ``release`` so dry-runs and CI without AWS
    credentials don't fail at import.
"""

from __future__ import annotations

import os
import socket
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class LockConfig:
    """Subset of the catalog YAML's ``lock:`` block."""

    table_name: str
    region: str = "us-east-1"
    ttl_seconds: int = 1800

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LockConfig":
        return cls(
            table_name=data["tableName"],
            region=data.get("region", "us-east-1"),
            ttl_seconds=int(data.get("ttlSeconds", 1800)),
        )


class LockAcquisitionError(RuntimeError):
    """Raised when the lock is held by someone else and we couldn't acquire."""


@dataclass
class LockHandle:
    """Returned by ``acquire`` and consumed by ``release``."""

    catalog: str
    holder: str
    acquired_at: int  # unix epoch seconds


def _holder_id() -> str:
    """Compose the holder identity that goes into the lock row."""
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
    host = socket.gethostname()
    pid = os.getpid()
    return f"{user}@{host}:{pid}"


def acquire(
    catalog: str,
    cfg: LockConfig,
    holder: str | None = None,
) -> LockHandle:
    """Acquire the catalog lock. Raises ``LockAcquisitionError`` if held.

    The lock row schema in DynamoDB:
      * ``catalog`` (partition key, string) — name of the catalog being locked.
      * ``holder`` (string) — who currently holds it.
      * ``acquired_at`` (number) — unix epoch seconds at acquisition.
      * ``expires_at`` (number) — TTL attribute, DynamoDB auto-deletes after this.

    The DynamoDB table must have ``catalog`` as the partition key and
    ``expires_at`` configured as the TTL attribute (both set in
    ``infrastructure/modules/iam/main.tf`` — see ``aws_dynamodb_table.glacierbase_lock``).
    """
    import boto3  # local import — keep boto3 out of CI/import-time

    holder = holder or _holder_id()
    now = int(time.time())
    expires_at = now + cfg.ttl_seconds

    ddb = boto3.client("dynamodb", region_name=cfg.region)
    try:
        ddb.put_item(
            TableName=cfg.table_name,
            Item={
                "catalog": {"S": catalog},
                "holder": {"S": holder},
                "acquired_at": {"N": str(now)},
                "expires_at": {"N": str(expires_at)},
            },
            # Atomic: only succeeds if no current row, OR the existing row
            # has expired (TTL hasn't reaped it yet). ``catalog`` and
            # ``expires_at`` are both reserved keywords in DynamoDB
            # expression language, so we have to alias them via
            # ExpressionAttributeNames.
            ConditionExpression=(
                "attribute_not_exists(#catalog) OR #expires_at < :now"
            ),
            ExpressionAttributeNames={
                "#catalog": "catalog",
                "#expires_at": "expires_at",
            },
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
    except ddb.exceptions.ConditionalCheckFailedException as exc:
        # Read the current holder for the error message.
        try:
            current = ddb.get_item(
                TableName=cfg.table_name,
                Key={"catalog": {"S": catalog}},
                ConsistentRead=True,
            ).get("Item", {})
            current_holder = current.get("holder", {}).get("S", "<unknown>")
            current_acquired = current.get("acquired_at", {}).get("N", "?")
        except Exception:  # noqa: BLE001 — best-effort diagnostic only
            current_holder = "<unknown>"
            current_acquired = "?"
        raise LockAcquisitionError(
            f"catalog {catalog!r} is currently locked by {current_holder} "
            f"(acquired_at={current_acquired}). Another migration run is in "
            f"progress, or the previous run died before releasing — wait "
            f"{cfg.ttl_seconds}s for the TTL to reap the row, or delete it "
            f"manually after confirming no run is active."
        ) from exc

    return LockHandle(catalog=catalog, holder=holder, acquired_at=now)


def release(handle: LockHandle, cfg: LockConfig) -> None:
    """Release the lock — conditional delete on holder identity.

    A lock that's already been TTL-reaped or stolen by another process
    isn't an error to release; we silently treat that as "not ours
    anymore" and return. The conditional delete prevents accidentally
    nuking a lock another process re-acquired after our TTL expired.
    """
    import boto3  # local import

    ddb = boto3.client("dynamodb", region_name=cfg.region)
    try:
        ddb.delete_item(
            TableName=cfg.table_name,
            Key={"catalog": {"S": handle.catalog}},
            ConditionExpression="holder = :holder",
            ExpressionAttributeValues={":holder": {"S": handle.holder}},
        )
    except ddb.exceptions.ConditionalCheckFailedException:
        # Either the row is gone (TTL reap or we never acquired) or another
        # holder has it. Both are non-fatal at release time.
        return
