# Postmortem: 3 simultaneous Iceberg migration gaps surfaced via dbt smoke test

**Date:** 2026-05-09
**Severity:** SEV2 (multiple latent gaps caught in test; never reached production)
**Status:** Resolved
**Authors:** PulseTrack DE
**Anchor commit / change:** [`8028edb`](../#) — `fix(iceberg,migrations): close 3 gaps from prompt 4`

## Summary

The Prompt-4-to-Prompt-5 dbt project introduction surfaced three independent gaps in the Iceberg + Glacierbase migration setup that had gone unnoticed during prompt-4's production hardening. None were customer-facing (dbt smoke against duckdb fixtures, not prod), but each had a different shape and required a different fix. Filing them as ONE postmortem because they share the root cause: insufficient test coverage of the migration framework's edge cases.

The three gaps were:
1. **DynamoDB reserved-keyword collision** — the Glacierbase lock table used a column named `Status` which is a reserved word in DynamoDB query expressions. Migrations couldn't acquire the lock, blocking all dbt builds.
2. **Iceberg snapshot retention misconfigured** — `expire_snapshots` was running with default `min_snapshots_to_keep` of 1, which would have been a problem in prod (no rollback room) but was masked locally because we never tested rollback.
3. **dbt `unique` test on surrogate keys was structurally impossible** — the `surrogate_key` macro produced 256-char SHA-256 outputs, dbt's default column type for tests was VARCHAR(64). String truncation caused the unique test to spuriously pass via collision.

## Impact

- **Blast radius:** development only. dbt + migration smoke tests were affected.
- **Customer-facing impact:** none — all caught in dev pre-prod
- **Data impact:** none — migrations weren't running in prod yet
- **Cost impact:** ~2 hours of dev time debugging each issue ($0)
- **Duration:** From first symptom 09:30 to all three fixed and merged 16:32 ≈ **7h 02m** (across all three bugs)

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-09 09:30 | dbt smoke test fails: `Unable to acquire Glacierbase lock` |
| 2026-05-09 09:45 | First hypothesis: stale lock from prior run. `aws dynamodb delete-item` to clean. Re-run fails the same way. |
| 2026-05-09 10:20 | Read AWS docs more carefully: `Status` is a reserved word. Migration code was using `Status = :active`, which DynamoDB silently treats as unparseable, returning empty result. |
| 2026-05-09 10:35 | Fix #1 applied: rename `Status` → `LockStatus` in the table schema + query expression |
| 2026-05-09 10:50 | dbt smoke proceeds further. Hits issue #2: `unique` test on `dim_patient.patient_key` passes trivially |
| 2026-05-09 11:20 | Investigation: SHA-256 surrogate keys are 64 hex chars (256 bits). dbt's duckdb adapter default VARCHAR length = 64. Multiple keys longer than that get truncated to identical 64-char prefixes. Collision → unique constraint satisfied. |
| 2026-05-09 12:00 | Fix #2: explicit type annotation on surrogate-key columns: `VARCHAR(128)` (room for hex + prefix). Coordinated change in dbt schema yml + Iceberg table DDL. |
| 2026-05-09 14:15 | dbt smoke passes. Operator runs Glacierbase expire_snapshots manually to clean dev. |
| 2026-05-09 14:30 | Issue #3 surfaces: expire_snapshots deletes everything except the latest snapshot. No rollback room. |
| 2026-05-09 14:45 | Read Iceberg docs: `min_snapshots_to_keep` defaults to 1 (latest only). Production-grade default should be 10. |
| 2026-05-09 15:30 | Fix #3: set `min_snapshots_to_keep = 10` and `older_than = 7 days` on the maintenance Prefect flow's expire_snapshots task |
| 2026-05-09 16:00 | All three fixes integrated into one commit. CI passes. |
| 2026-05-09 16:32 | Commit `8028edb` merged |

## Root cause

Three distinct technical root causes, one shared organizational root cause.

**Technical (per-gap):**
1. DynamoDB reserved-word handling is implicit — no error, just empty result. Common DB-driver gotcha that we should have anticipated.
2. dbt + duckdb default type inference is conservative (VARCHAR(64)) and silent on truncation. Should have explicit type annotations everywhere.
3. Iceberg's `expire_snapshots` defaults prioritize disk-space recovery over rollback safety. Reasonable for batch-only orgs, dangerous for streaming where rollback is the recovery primitive.

**Organizational:**
- Each of these would have been caught by a comprehensive integration test of the migration framework. We had unit tests for individual SQL files but no end-to-end "apply migration → query → rollback → re-apply" smoke that would have exercised all three code paths.

## 5 Whys (composite, applied to the organizational root cause)

1. **Why did three migration bugs ship together?** Because no end-to-end migration smoke test existed.
2. **Why no e2e migration test?** Because we built migrations as single-shot SQL apply, no "round-trip" verification.
3. **Why is round-trip verification not part of the standard test pattern?** Because the existing test patterns are for transformations (silver/gold), where round-trip doesn't apply.
4. **Why didn't we generalize the test pattern to operations like migrations?** Because the migration framework felt "infra not application code" — but it's both.
5. **Why is the boundary between "infra" and "application" loose?** Because in a 1-person project there's no infra-vs-application team split, but the test rigor differs. Infrastructure as code requires application-code-level test discipline.

## Trigger

The Prompt-5 dbt smoke test was the trigger — it was the first time the dbt + migration + Iceberg stack was exercised together. Up until then, each was tested in isolation. The integration is where the latent bugs surfaced.

## Resolution

Commit `8028edb` bundles all three fixes:
1. Glacierbase lock table: rename `Status` → `LockStatus`, update query expressions in `migrations/lock.py`
2. dbt schema annotations: explicit `VARCHAR(128)` on surrogate-key columns in `dbt_project/models/staging/*.yml`
3. Maintenance flow: `min_snapshots_to_keep=10, older_than=7 days` in `orchestration/flows/maintenance_pipeline.py`

Plus: backfill of an integration test scaffold in `tests/migrations/test_migration_roundtrip.py` (apply → query → rollback assertions).

## What went well

- **All three bugs were caught pre-production.** This is the value of the dbt smoke + Glacierbase combination: forced an integration scenario that exposed isolation-test blind spots.
- **Fixes were small and surgical.** Each bug had a single-line or single-block fix, not architectural overhauls.
- **One commit captures all three.** Avoids the "fix-1 broke fix-2 broke fix-3" cascade that can happen with sequential bug fixes.
- **The integration test scaffold was added at the same time.** Prevents regression — future migration framework changes will run through round-trip assertions.

## What didn't go well

- **Three concurrent bugs is a lot.** Suggests a class-of-bug problem: infrastructure-as-code (Glacierbase) was being treated with less test rigor than application code (Spark transforms). Need to align rigor across both.
- **DynamoDB reserved-word issue is well-documented but we didn't pattern-match it.** Reserved-word lists are part of every DB documentation; we should have a "things that can silently break DDL/DML" checklist.
- **dbt + duckdb default-type behavior is silently lossy.** Should have explicit type annotations as a default convention, not an exception for known-problematic columns.
- **Iceberg `min_snapshots_to_keep` default is a footgun.** The default works for batch + bulk-delete patterns; for streaming + rollback-as-recovery, the default is dangerous. Operators need to know this.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Maintain integration test pattern for any migration-framework change (now scaffolded in `tests/migrations/`) | PulseTrack DE | ongoing | P0 |
| 2 | Add DynamoDB reserved-word check to CI: lint our query expressions against AWS's reserved-word list | PulseTrack DE | 2026-05-22 | P2 |
| 3 | Standardize: explicit type annotations on all dbt model columns, no defaults | PulseTrack DE | 2026-05-15 | P1 |
| 4 | Document Iceberg `min_snapshots_to_keep` rationale in `docs/disaster_recovery.md` | PulseTrack DE | 2026-05-10 (Prompt 9 Group E) | P0 |
| 5 | Add an "infrastructure-as-code requires application-code test rigor" section to ADR-004 (migration framework) | PulseTrack DE | 2026-05-10 (Prompt 9 Group G) | P1 |

## Lessons learned

**Bugs cluster in integration boundaries.** Each of these three bugs lived at an integration seam (Glacierbase ↔ DynamoDB, dbt ↔ duckdb, Iceberg ↔ Glacierbase maintenance). Integration tests catch what isolation tests can't.

**Reserved-keyword issues are silent and recurring.** DynamoDB, Snowflake, PostgreSQL, SQL Server — every database has reserved words and each one will let you write a syntactically-valid-looking query that fails semantically. Defensive pattern: prefix every column name with a non-reserved literal, or use the database-specific quoting unconditionally.

**Default settings encode assumptions about the user's use case.** Iceberg's `min_snapshots_to_keep=1` assumes batch operators who want to reclaim space. Streaming operators need rollback room. When using a tool in a non-default mode (streaming, in our case), audit every default.

**Three bugs in one commit isn't ideal but isn't terrible.** It signals "we found a class of problem and addressed all instances." That's better than fixing them one at a time across three weeks. As long as each fix is isolated within the commit (separate files, separate logical changes), the bundling is fine.

## References

- Runbooks: `runbooks/dlq_buildup.md`, `runbooks/silver_cold_start_hang.md` (related test-coverage failures)
- Related ADRs: `docs/adrs/ADR-004-glacierbase-migration-framework.md`, `docs/adrs/ADR-001-iceberg-over-delta.md`
- Related code: `migrations/lock.py`, `dbt_project/models/staging/*.yml`, `orchestration/flows/maintenance_pipeline.py`
- Related commits: `8028edb` (the fix), `0aaf0e1` (the original Glacierbase introduction)
- External: AWS DynamoDB reserved-words list; Iceberg snapshot-management docs
