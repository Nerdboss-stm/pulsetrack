# Postmortem: Gold streaming clobbered active snapshots; near-miss data loss

**Date:** 2026-05-10
**Severity:** SEV2 (near-miss; no permanent data loss, but full snapshot rewind required)
**Status:** Resolved
**Authors:** PulseTrack DE
**Anchor commit / change:** [`0cadc84`](../#) — `fix(streaming): gold opts into streaming-skip-overwrite-snapshots; doc fix`

## Summary

Gold streaming queries that use `INSERT OVERWRITE` semantics (for dim refreshes — `dim_patient`, `dim_metric`, etc.) were writing without the Iceberg property `write.spark.streaming-skip-overwrite-snapshots=true`. Result: each streaming micro-batch was creating a new "overwrite" snapshot that effectively **shadowed** all prior data in the table. Downstream consumers querying via Snowflake AUTO_REFRESH or dbt were seeing only the latest 30-second micro-batch's worth of dim records (~50 rows) instead of the full 50,000+ row dimension.

Caught during dbt regression testing — dbt's `unique` test on `dim_patient.patient_key` started passing trivially (the table had ~50 rows instead of 50,000), which raised a flag. Recovered via Iceberg snapshot rewind to the last fully-populated snapshot. No data was actually lost (Iceberg keeps prior snapshots), but the production consumer view was incorrect for ~6 hours.

## Impact

- **Blast radius:** all `dim_*` tables written by streaming gold (`dim_patient`, `dim_device`, `dim_metric`, `dim_condition`). Facts unaffected (they MERGE, not OVERWRITE).
- **Customer-facing impact:** none in production (this was caught pre-deploy), but Snowflake dev `vw_patient_health_360` returned dramatically reduced row counts during the affected window
- **Data impact:** zero permanent data loss — Iceberg snapshot history preserved all prior data. **Visible** data was wrong: dim joins in downstream consumers returned `null` for the 99.9% of patients whose dim rows had been shadowed.
- **Cost impact:** ~$0.30 wasted on dbt + Snowflake queries against shadowed data
- **Duration:** First wrong-row-count noticed 14:08; rolled back to good snapshot at 18:42 ≈ **4h 34m**

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-10 07:30 | Gold dim streaming job (`streaming/gold_dim_patient.py`) started for fresh run |
| 2026-05-10 12:15 | First MERGE INTO + OVERWRITE micro-batch commits a "overwrite-snapshot" |
| 2026-05-10 12:15 → 14:08 | ~4 batches commit, each replacing the visible row-set |
| 2026-05-10 14:08 | dbt test run: `unique_dim_patient_patient_key` passes (correctly, but trivially — only 47 rows in table) |
| 2026-05-10 14:10 | Operator notices the test run is suspicious — pulls row count: 47 rows where 50K expected |
| 2026-05-10 14:15 | Cross-check Iceberg snapshots: 4 "overwrite" snapshots in last 2h, each containing only the micro-batch slice |
| 2026-05-10 14:30 | Read Iceberg docs more carefully — `write.spark.streaming-skip-overwrite-snapshots` exists specifically for this case |
| 2026-05-10 17:00 | Patch in progress: add `TBLPROPERTIES (..., 'write.spark.streaming-skip-overwrite-snapshots' = 'true')` to the dim tables |
| 2026-05-10 18:00 | Identify the right snapshot to rewind to via `SELECT * FROM dim_patient.history` |
| 2026-05-10 18:30 | `CALL system.rollback_to_snapshot('pulsetrack_gold_dev.dim_patient', <snapshot_id>)` |
| 2026-05-10 18:42 | dbt re-runs, all rowcounts back to expected |
| 2026-05-10 18:45 | Commit `0cadc84` deployed |

## Root cause

Iceberg has two different "write modes" for Spark Structured Streaming:
1. **Append/merge mode (default):** each micro-batch APPENDs or MERGEs, prior data remains visible
2. **Overwrite mode:** each micro-batch OVERWRITEs the partition. Default semantics: prior visible data is hidden until a new snapshot writes it back.

For the gold dim tables, we used `INSERT OVERWRITE` because the dim build is essentially a recomputation from silver. The intent: rewrite the whole dimension on each micro-batch.

But streaming-mode default behavior for OVERWRITE creates a new snapshot per micro-batch, **and that snapshot only contains the partitions touched in that batch**. So if a micro-batch only sees 50 silver patients in its time window, the new gold snapshot only contains those 50 dim rows.

The property `write.spark.streaming-skip-overwrite-snapshots=true` tells Iceberg: "yes, this is OVERWRITE-mode, but DON'T treat each micro-batch as a full table overwrite — append/merge the partitions instead." Without this property, OVERWRITE streaming silently corrupts the table.

This is a non-obvious gotcha that's covered in the Iceberg docs but not in any "common pitfalls" page.

## 5 Whys

1. **Why did dim_patient return 47 rows instead of 50,000?** Because the latest snapshot only contained the 47 patients from the latest micro-batch.
2. **Why does the snapshot only contain a micro-batch worth?** Because OVERWRITE-mode streaming creates a fresh snapshot per batch, hiding prior data.
3. **Why was OVERWRITE-mode the default?** Because dim builds use `INSERT OVERWRITE` semantics for full recomputation — the right pattern for the use case.
4. **Why didn't the streaming-skip-overwrite-snapshots property get set?** Because we didn't know about it. Iceberg's streaming docs cover it, but our code path was lifted from a batch example.
5. **Why didn't a test catch this?** Because we don't have a "snapshot count + visible row count consistency" smoke test for any streaming write path. The dbt test that surfaced this was incidental.

The fifth "why" lands on a missing **streaming write-mode test pattern**. Every streaming write path should have: produce N rows → assert visible count ≥ N. Without that invariant, write-mode bugs are silent.

## Trigger

Switching the gold dim builds from batch (`--mode batch` daily) to streaming (`--mode streaming` continuous). The batch path was using `INSERT OVERWRITE` with explicit partition semantics that worked correctly. The streaming path lifted the same pattern without realizing the snapshot behavior differs.

## Resolution

Commit `0cadc84`:
1. Add `TBLPROPERTIES ('write.spark.streaming-skip-overwrite-snapshots' = 'true')` to each dim Iceberg table DDL in `migrations/`
2. Document the gotcha inline: "Without this, streaming OVERWRITE creates per-batch snapshots that shadow all prior data."
3. Apply the migration; rewind affected tables to last good snapshot.

## What went well

- **Iceberg snapshot history saved us.** No data was permanently lost — rewind was a single `CALL system.rollback_to_snapshot(...)` away.
- **dbt's regression test caught it indirectly.** Even though the test wasn't designed for this, the suspicious-passing test surfaced the issue within hours.
- **The fix is one TBLPROPERTIES update + a migration.** Small blast radius for the patch itself.

## What didn't go well

- **No streaming-mode smoke test in CI.** This bug would have been caught by a "write 100 rows, read N=100" assertion in any unit test on the streaming path. We don't have those tests.
- **The bug was silent for 4 hours.** No alert fires when row counts plummet by 99.9% — there's no "row count delta vs baseline" monitor on dim tables.
- **Documentation lift was shallow.** We borrowed the dim-write pattern from an Iceberg batch example without re-reading the streaming-specific docs. Cultural problem: "code patterns copy-paste" assumes the patterns are mode-agnostic, which Iceberg's aren't.
- **The Iceberg property name is not discoverable.** `streaming-skip-overwrite-snapshots` requires you to already know it exists to search for it. A more obvious name like `streaming.overwrite-mode = append|replace` would have made this googleable.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Add streaming-mode row-count smoke test to `tests/streaming/` — produce N, assert visible N | PulseTrack DE | 2026-05-22 | P1 |
| 2 | Add dim-table row-count delta monitor (Monte Carlo style, in `observability/`) | PulseTrack DE | 2026-05-22 | P1 |
| 3 | Document Iceberg streaming gotchas in `docs/data_contracts.md` (silver → gold gold section) | PulseTrack DE | 2026-05-10 (Prompt 9 Group E) | P0 |
| 4 | Audit all streaming write paths for similar mode-confusion bugs (search for `INSERT OVERWRITE` + streaming) | PulseTrack DE | 2026-05-15 | P1 |
| 5 | File Iceberg-project issue requesting better default behavior or clearer docs for streaming OVERWRITE | TBD | 2026-Q3 | P3 |

## Lessons learned

**Read the docs for the SPECIFIC mode you're using, not the generic ones.** Iceberg's batch and streaming write paths have non-obvious semantic differences. Copy-pasting from one mode's example into the other is a bug-pattern.

**Snapshot history is your ground truth for "did we lose data?"** Always check `<table>.history` first. Iceberg's design preserves data across mode-bugs — but visible data can still be wrong.

**Silent failures are the worst kind.** A loud failure (`INSERT OVERWRITE` raises an error) would have been better than the silent corruption. Defensive monitoring (row-count deltas, snapshot count growth) is the safety net when code-level invariants fail.

**Configuration-as-code beats code-with-defaults.** The fix is `TBLPROPERTIES`, which lives in the migration alongside the table definition. That's the right place — anyone reviewing the migration sees the property and asks why. Burying the same setting in the streaming Python config would have been worse.

## References

- Runbook: `runbooks/gx_failure_drains_batch.md` (related "silent data drop" failure mode)
- Related ADRs: `docs/adrs/ADR-001-iceberg-over-delta.md` (Iceberg choice + tradeoffs)
- Related code: `streaming/gold_dim_patient.py`, `migrations/V006_dim_table_streaming_props.sql` (the property addition)
- Related commits: `0cadc84` (the fix)
- External: Iceberg docs on streaming write modes; the GitHub issue tracking better-default semantics
