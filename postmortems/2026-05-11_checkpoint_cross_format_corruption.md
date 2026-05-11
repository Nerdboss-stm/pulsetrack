# 2026-05-11 — Checkpoint cross-format corruption (Iceberg → Delta)

**Severity:** SEV3 (caught in dev, no data loss; would have been SEV2 in prod)
**Authors:** PulseTrack DE
**Anchor commit:** 088b6da (forensics finding from gap-closure batch)
**Status:** Mitigated by isolating checkpoint dirs per table-format

## Summary

The silver streaming query crashed twice during the 2026-05-11 bringup with
`DeltaIllegalStateException: source version 0 incompatible with current
Delta source format version 2`. The original RCA (papered over in commit
600eb90 fix #12 — "stale silver Delta checkpoint") was incomplete. The
forensics-agent run during the gap-closure batch (post-execution
investigation) found the actual cause: **the same checkpoint path was
shared between an Iceberg streaming query (app `_0008`, query id
`213ed26e-dd1e-4fef-8d84-9f6ae60fd801`) and a later Delta streaming query
(app `_0015`).**

App `_0008` (Iceberg silver) wrote offset metadata under
`s3://pulsetrack-lakehouse-dev-03a28ee7/checkpoints/silver_sensors/` in
the Iceberg streaming source's offset format. App `_0015` (Delta silver)
tried to resume from the same checkpoint dir; the Delta-Lake 3.3.2 reader
saw an offset format it couldn't parse and threw
`DeltaIllegalStateException`.

The earlier "clear stale checkpoint" mitigation was correct but missed the
underlying mechanism. Any future run that switches table format on the
same source path will hit this again unless we isolate.

## Detection

Found post-hoc via the gap-closure forensics agent reading EMR S3 logs
at `s3://pulsetrack-lakehouse-dev-03a28ee7/emr-logs/j-T5OF7WBI2I4V/`.
The smoking-gun pair: same `queryId` in both apps' driver logs, same
`spark.checkpointLocation` config value, different `spark.format` values.

## Impact

* 2 EMR step failures during bringup (apps `_0015`, `_0017`).
* ~15 minutes of cluster time burned debugging the "Delta format
  incompatible" error (followed by `aws s3 rm --recursive` of the
  checkpoint).
* No data loss; bronze data was intact; silver query restarted from
  Kafka offset 0 (acceptable since we used `--trigger available_now`).

In production this would have been SEV2: a streaming query that won't
resume = ingestion pipeline freezes = customer-visible freshness lag.

## Root cause

`streaming/spark_config.py` + the per-script `--checkpoint` flag derive
the silver_sensors checkpoint path from a single namespace
(`{settings.lakehouse_base}/checkpoints/silver_sensors/`). This path is
shared regardless of `--format iceberg` vs `--format delta`.

Spark Structured Streaming's checkpoint format is source-specific
(KafkaSource records partition + offset, IcebergSource records snapshot
ID, DeltaSource records source version). When a user changes the
streaming source format and resumes from the same checkpoint dir, the
new source's offset reader sees an unintelligible blob.

## Mitigation (the fix)

**Going forward:** checkpoint paths must encode the table format
explicitly. Two changes:

1. **Convention:** any checkpoint path includes `_{fmt}` suffix:
   `s3://.../checkpoints/silver_sensors_iceberg/`,
   `s3://.../checkpoints/silver_sensors_delta/`. Different format =
   different namespace.

2. **Code enforcement:** `streaming/spark_config.py:get_silver_checkpoint_path(fmt)`
   returns the format-aware path. Migrations from one format to the
   other require deleting (or migrating offsets in) the old path
   explicitly.

For the upcoming Prompt 9 re-run on 4-core Iceberg, the checkpoint paths
are all fresh (terraform destroyed + re-applied the cluster), so this
corruption is automatically resolved. The convention prevents recurrence.

## Action items

* [P0] Document the convention in `docs/streaming_checkpoints.md` (new
  doc, ~80 LOC) so anyone adding a new streaming query uses
  `silver_<table>_<fmt>/`.
* [P1] Implement `get_silver_checkpoint_path(fmt)` helper in
  `streaming/spark_config.py` and migrate the 3 streaming scripts
  (bronze_ingestion, sensor_silver, pharmacy_silver) to use it.
* [P1] Add a pre-flight check to `scripts/check_credentials.py` that
  scans checkpoint dirs for cross-format pollution.
* [P2] Add a regression test in `tests/streaming/test_checkpoint_isolation.py`
  that asserts checkpoint paths differ by format.

## What didn't go well

* The earlier mitigation (commit 600eb90 fix #12) was incomplete — it
  cleared the checkpoint without understanding the root cause. This
  guaranteed recurrence on the next format-switch.
* The forensics agent that found this was launched only after the user
  pushed back on the falsely-claimed "end-to-end complete" verdict. Had
  we stopped to forensic the failure at the time, this would have been
  in the original postmortem.

## What went well

* The S3 EMR log retention preserved both apps' driver logs long enough
  for post-hoc forensics.
* Iceberg + Delta both surface `queryId` in their driver logs, making
  the cross-app correlation possible.
* The eventual fix (format-aware checkpoint paths) is mechanical and
  unblocks any future format migration without ad-hoc intervention.

## Cross-references

* `postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md` —
  the original incident in flight (fix #12).
* `docs/adrs/ADR-007-table-format-by-cluster-size.md` — chose
  Iceberg for ≥4-core; this postmortem motivates the format-aware
  checkpoint convention required to make any format choice safely
  re-runnable.
