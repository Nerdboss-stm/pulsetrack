# Postmortem: silver streaming query hangs at startup, drops 58K rows on long backfill

**Date:** 2026-05-09
**Severity:** SEV2 (degraded freshness, partial data loss on a backfill — no production exposure yet)
**Status:** Resolved
**Authors:** PulseTrack DE
**Incident commander:** N/A (single-developer team)
**Anchor commit / change:** [`78ddfea`](../#) — `fix(silver-gate): Option 3 - drop event_timestamp window, rely on per-row is_late_arriving + watermark (unblocks long backfills like WHOOP 240-day)`

## Summary

The silver streaming query (`streaming/silver_ingestion.py`) reads bronze, applies the `silver_sensor` GX expectation suite, and writes via MERGE INTO an Iceberg table. During a WHOOP 240-day backfill we observed silver hang at startup — the streaming query started, processed a few batches successfully, and then dropped 58,162 otherwise-valid rows in a single batch. Root cause: the GX suite's `event_timestamp` window check was rejecting the entire batch because **one** row was older than 60 days, and the gate is all-or-nothing.

Resolution: dropped the event_timestamp window expectation from the silver gate, relying instead on per-row `is_late_arriving` flagging (already computed) + the streaming watermark for late-arrival handling. WHOOP 240-day backfill now succeeds.

## Impact

- **Blast radius:** silver sensor layer; gold facts downstream couldn't refresh from the affected batches
- **Customer-facing impact:** none (pre-production), but the backfill failure blocked the WHOOP historical-data import that we needed for dashboard demos
- **Data impact:** 58,162 rows that should have landed in silver were quarantined as "GX validation failed" — even though only 1 row was actually problematic, and that problematic row had `is_late_arriving=true` (legitimate, expected backfill behavior)
- **Cost impact:** ~30 minutes of EMR compute wasted on retries before root cause identified ($0.15)
- **Duration:** From first hang at 19:54 to fix-deployed at 23:38 ≈ **3h 44m**, of which ~2h was unfocused debugging and ~30 min was actual root-cause analysis

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-08 19:54 | Silver streaming query started for WHOOP backfill (240 days of recovery data) |
| 2026-05-08 19:57 | First micro-batch completes successfully (200 rows) |
| 2026-05-08 19:58 | Second micro-batch processes 58,162 rows from bronze |
| 2026-05-08 19:59 | GX validation returns `success=false`; batch quarantined; silver write skipped |
| 2026-05-08 20:00 | Spark logs: `Quarantined 58162 rows (failed expectation: expect_column_values_to_be_between event_timestamp)` |
| 2026-05-08 20:05 | Investigation begins — initial hypothesis: bronze→silver schema mismatch |
| 2026-05-08 21:30 | Rule out schema mismatch (silver projection matches bronze). New hypothesis: GX suite is too strict |
| 2026-05-08 22:15 | Manually run GX suite on the batch: identifies `event_timestamp` window expectation as the failing one |
| 2026-05-08 22:20 | Read the GX docs more carefully: `expect_column_values_to_be_between` with `mostly` param defaults to 100% — one row fails → whole expectation fails → batch quarantined |
| 2026-05-08 23:00 | Discuss 3 options: (1) loosen window to 365d, (2) set `mostly=0.99`, (3) drop the expectation entirely + rely on existing is_late_arriving |
| 2026-05-08 23:30 | Decision: Option 3. Reasoning in docstring: per-row `is_late_arriving` already exists + watermark handles streaming-relevant late arrivals + historical loads are legitimate use case |
| 2026-05-08 23:38 | Commit `78ddfea` deployed; silver re-runs and processes 58,162 rows successfully |
| 2026-05-08 23:45 | Verified: silver row count matches bronze count − dedup window. WHOOP backfill complete by 23:53 |

## Root cause

The silver `expect_column_values_to_be_between` expectation on `event_timestamp` was configured with a `min_value = now() - 60 days` and `max_value = now()`. This was intended to catch obviously-broken event timestamps (e.g., 1970-01-01 from a firmware bug).

But GX expectations default to **100% pass rate** — even one failing row makes the whole expectation fail. And since the silver suite is run as a quality gate (`validate → if not success: quarantine`), one failing row drops the entire batch.

For a streaming pipeline this is mostly fine — micro-batches are small, and a stale event in the live stream is rare. But for a backfill, where bronze contains 240 days of historical data being streamed in chunks, the "first row > 60 days old" was a structural rather than exceptional condition.

The deeper architectural mistake: **using a quality gate (binary pass/fail) for what should have been a quality flag (per-row tagging).** The existing `is_late_arriving` column already does exactly that — flags late rows without rejecting them. The event_timestamp expectation was redundant with `is_late_arriving` AND turned a per-row flag into a per-batch gate.

## 5 Whys

1. **Why did the silver write skip 58K rows?** Because GX returned `success=false` for the batch.
2. **Why did GX return `success=false`?** Because the `event_timestamp` window expectation failed (one row was older than 60 days).
3. **Why did one stale row fail the whole expectation?** Because the expectation defaulted to 100% pass-rate (we didn't set `mostly`).
4. **Why was this expectation in the suite at all?** Because we wanted to catch obviously-bad timestamps. But the per-row `is_late_arriving` flag (already in silver) does this without the batch-level side effect.
5. **Why didn't the design review catch the duplication?** Because the silver gate was added before `is_late_arriving` was implemented; nobody re-audited the GX suite after `is_late_arriving` came in. Tech-debt accumulation in a 1-person project: changes don't get re-reviewed.

## Trigger

WHOOP 240-day backfill. In normal streaming, all events are < 60 days old by definition (live data). Only a backfill — explicitly out-of-spec for the original expectation's design — exposed the bug.

## Resolution

Commit `78ddfea`: drop the `event_timestamp` window expectation from the silver gate. The expectation removal is documented inline:

```python
# Note on event_timestamp: an earlier version of this suite enforced an event
# freshness window (now ± 60 days). That was removed because (1) the gate is
# all-or-nothing per batch — one stale row dropped 58k good rows on backfills,
# (2) per-row freshness is already tracked via ``is_late_arriving`` set in
# ``add_quality_flags``, and (3) historical loads (WHOOP backfill, EHR re-import)
# legitimately produce old event_timestamps. Operators monitoring stale data
# should query ``is_late_arriving`` rather than rely on a hard gate.
```

This documentation pattern (commit + inline rationale) is now the team convention for any gate removal.

## What went well

- The quarantine path worked correctly — those 58K rows were preserved in `s3://$BUCKET/quarantine/`, not lost
- GX validation logs were detailed enough that the failing expectation was identifiable
- Once the root cause was found, the fix took 8 minutes (drop the expectation, redeploy)
- WHOOP backfill resumed cleanly after the fix; no manual replay needed

## What didn't go well

- **Diagnosis took 2 hours** because the initial hypothesis (schema mismatch) sent us down the wrong path. A runbook would have caught this faster.
- **The GX expectation rationale was undocumented.** Without context, we spent time evaluating whether we should keep, loosen, or drop it. With context ("we have is_late_arriving for this"), the decision is obvious.
- **No alerting on quarantine size.** The 58K rows landed in quarantine without paging anyone. We only found out because the WHOOP demo broke. A `quarantine_volume_burst` monitor would have alerted within 1 minute.
- **The all-or-nothing gate semantics were not in any design doc.** Operators (including future-me) need to know that one row can drop a whole batch. This belongs in `docs/data_contracts.md` under "silver gate semantics."

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Add `runbooks/gx_failure_drains_batch.md` documenting this exact failure mode + 4-option recovery | PulseTrack DE | 2026-05-10 (Prompt 9 Group C) | P0 |
| 2 | Add quarantine-burst alert to `observability/monitors.py` (volume threshold + comparison to historical) | PulseTrack DE | 2026-05-15 | P1 |
| 3 | Audit all GX expectations across bronze/silver/gold suites for "is this gate vs. flag?" semantics | PulseTrack DE | 2026-05-22 | P1 |
| 4 | Document silver gate semantics in `docs/data_contracts.md` (newly added in this commit's wake) | PulseTrack DE | 2026-05-10 (Prompt 9 Group E) | P1 |
| 5 | Add a backfill-mode test to CI: simulate 60-day-old event_timestamp, assert silver doesn't drop the batch | PulseTrack DE | 2026-05-29 | P2 |
| 6 | Tag commit `78ddfea` with the postmortem URL when this file is committed | PulseTrack DE | 2026-05-10 (this commit) | P2 |

## Lessons learned

**Gates vs. flags are different design primitives, and conflating them silently drops data.** A gate is binary, batch-scoped, and rejects. A flag is per-row, propagating, and tags. Use gates for invariants that absolutely must hold ("device_id matches regex"). Use flags for quality signals consumers should reason about ("this event is_late_arriving=true; you decide what to do"). When in doubt, prefer flags — they're reversible.

**Validate expectations on backfill, not just live data.** The expectation was correctly tested on live-streaming bronze data, where it always passed. It was never tested with old-event-timestamp data, because that wasn't part of the "normal" test scenario. A backfill test in CI would have caught this.

**Quarantine without alerting is dropping data quietly.** A pile of rows in `s3://$BUCKET/quarantine/` that nobody watches IS data loss, regardless of whether the rows are technically preserved. Every quarantine path needs a volume-based alert.

**1-person projects accumulate undocumented tech debt fast.** Changes don't get re-reviewed because there's no second reviewer. Counter: regular self-audits, ideally driven by a checklist ("when I add column X, re-audit any expectation that touches X-adjacent columns").

## References

- Runbook: `runbooks/gx_failure_drains_batch.md` (new, this prompt)
- Related ADRs: `docs/adrs/ADR-005-streaming-first-hybrid.md` (covers when streaming-only assumptions fail)
- Related code: `data_quality/expectations/silver_sensor_suite.py` (the modified suite)
- Related commits: `78ddfea` (the fix), `e36202c` (the WHOOP-API work that surfaced the backfill scenario)
- External: GX docs on `expect_column_values_to_be_between` and the `mostly` parameter
