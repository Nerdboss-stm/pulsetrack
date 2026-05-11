# Postmortem: 8 P0 bugs surgically fixed across Q/O/S layers in one go

**Date:** 2026-05-07
**Severity:** SEV2 (cluster of dev-blocking bugs; no production exposure)
**Status:** Resolved
**Authors:** PulseTrack DE
**Anchor commit / change:** [`a25027e`](../#) — `fix: P0 surgical fixes (Q1 match_method, Q2 deterministic primary_cond, Q3+Q4 --mode argv, O4 metrics ports, S2+S3 docker hardening)`

## Summary

A wave of integration testing during the Prompt-3-to-Prompt-4 transition surfaced 8 separate P0 (dev-blocking) bugs across quality-checks (Q), observability (O), and streaming (S) layers. Filing as one postmortem because:
1. They were discovered in one debugging session
2. They share the root cause of insufficient cross-layer integration testing
3. They were fixed atomically in one commit (intentional — easier rollback if any one fix broke something else)

The 8 bugs were:
- **Q1** — identity bridge `match_method` field returning `null` when MRN match succeeded (string-literal vs enum confusion)
- **Q2** — `primary_condition` selection non-deterministic across runs (ordering by `condition_id` instead of a stable composite key)
- **Q3** — bronze producer's `--mode` argv not parsing (parser default winning over CLI)
- **Q4** — silver Spark job same `--mode` issue
- **O4** — Prometheus metrics ports collided between 4 producer processes (all bound to 8000)
- **S2** — Docker compose Kafka health check failing because TCP-only check was insufficient; needed application-level handshake
- **S3** — Docker compose Zookeeper "4-letter-word" check rejected by ZK 3.7+ unless whitelisted
- Plus a doc fix bundled in (not P0 but in the same area).

## Impact

- **Blast radius:** development environment only. None of these bugs reached an EMR-deployed pipeline.
- **Customer-facing impact:** none
- **Data impact:** during local testing, identity-bridge tests were running with wrong match_method classifications. No persisted data was corrupted (the test fixtures were rebuilt for each run).
- **Cost impact:** ~5 hours of dev time over 2 sessions ($0 AWS)
- **Duration:** symptoms accumulated over ~2 days of dev work; all fixed in one 5h batching session

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-05 | Q1 + Q2 noticed during identity-bridge unit tests (3 reruns showed non-deterministic primary_cond) |
| 2026-05-06 09:00 | Q3 + Q4 noticed: `python ... --mode batch` was being silently overridden to the default. Producer + silver both affected. |
| 2026-05-06 14:00 | O4 noticed when 4 producer processes can't all bind to :8000 — first 1 wins, others crash silently |
| 2026-05-06 16:30 | S2 noticed: docker compose up hangs because Kafka health check passes too early — TCP socket open != broker accepting client API calls |
| 2026-05-06 18:00 | S3 noticed: ZK container logs `4lw command 'srvr' is not in whitelist` — added in ZK 3.7 |
| 2026-05-07 09:30 | Triage: decision to fix all 8 in one atomic commit |
| 2026-05-07 09:30 → 14:56 | Surgical fixes, one bug at a time, with a test for each |
| 2026-05-07 14:56 | Commit `a25027e` merged; full local stack restart passes all checks |

## Root cause

Each of the 8 bugs has its own technical root cause; the organizational pattern is shared.

**Technical (briefly):**
- **Q1:** identity-bridge code used `"mrn"` (string literal) in some places and `MatchMethod.MRN` (enum) in others; the comparison was string-to-enum and never matched.
- **Q2:** `primary_condition` was selected as `SELECT condition_id FROM ... ORDER BY condition_id LIMIT 1` — but `condition_id` was a UUID, so the ordering was random. Fixed: order by `(severity, onset_date, condition_id)` for stable selection.
- **Q3 + Q4:** the argparse default value was set AFTER `parse_args()` was called, in the same function. Default won. Fixed: set defaults in `add_argument(default=...)` not in the consumer code.
- **O4:** producer module hard-coded `start_metrics_server(port=8000)`. Fixed: read from `config.py:metrics_port_*` settings — each producer has its own port number.
- **S2:** docker-compose `healthcheck` used `nc -z` to check TCP socket. Replaced with a Kafka-broker-API ping (`kafka-broker-api-versions --bootstrap-server localhost:9092`).
- **S3:** docker-compose ZK config added `4LW_WHITELIST=stat,ruok,srvr,conf,isro` (the standard 4lw commands plus health check).

**Organizational (shared):**
- **No standard CI step ran the local docker stack end-to-end.** Each module had its own unit tests. None of those tests said "spin up the whole docker compose, verify all 4 producers + bronze + silver all bind ports and serve metrics."
- **Module ownership was implicit.** Producer code, observability config, and docker-compose were all "the developer's" code, but no review checklist said "if you change ports in one, audit all four."

## 5 Whys (composite)

1. **Why did 8 P0 bugs accumulate?** Because the test suite was per-module, not cross-module.
2. **Why no cross-module test?** Because cross-module setup (docker compose + 4 producers + Spark) is expensive and slow.
3. **Why didn't we automate the expensive cross-module test?** Because no CI runner had docker-in-docker + Java + Python + Spark on it.
4. **Why didn't we set up that CI runner?** Because it's a half-day of CI infrastructure work and we kept deferring it.
5. **Why did we keep deferring it?** Because "the dev environment will catch it" — except the dev environment can't catch all 4 producers simultaneously without a manual ritual we didn't run before each PR.

## Trigger

Prompt-4 integration testing — the first time all 4 producers + the full streaming pipeline + the docker stack were brought up together to validate the end-to-end story. Each isolated test had passed; the integration was the trigger.

## Resolution

Commit `a25027e` contains all 8 fixes, each with a one-line PR-ready description in the commit body. The atomic commit was a deliberate choice — partially-fixed states (e.g., Q1 fixed but Q2 not) would have been worse than the original state because Q2 reruns would have looked deterministic in misleading ways.

Each fix has its own test:
- Q1: unit test against MatchMethod enum comparison
- Q2: 100-run determinism test
- Q3/Q4: argparse `--mode batch` round-trip test
- O4: smoke test verifying 4 producers can bind 4 different ports
- S2/S3: docker compose CI smoke (NEW workflow `.github/workflows/docker_compose_smoke.yml`)

## What went well

- **All 8 bugs found in one debugging session.** The cluster effect made the underlying organizational problem (test coverage gap) impossible to ignore.
- **Atomic fix commit kept the state coherent.** No partial fixes hanging around.
- **Each fix shipped with a regression test.** Future changes to those code paths will hit the test before the bug reaches dev again.
- **Documentation updated alongside.** Helps future-me / collaborators understand what each test guards against.

## What didn't go well

- **The 5-hour debugging session was draining.** Sustained context-switching across 8 different bug shapes is cognitively expensive. Better: detect the pattern earlier (after 2-3 bugs), pause, set up cross-module CI, then resume.
- **The docker compose smoke test should have existed since day 1.** Setting it up as a reaction to 8 bugs is "closing the barn door after the horse." For new projects: cross-module smoke on day 1, before feature work.
- **No one bug was complicated.** Each was a small thing. The challenge was finding them, not fixing them. Better detection → less time wasted.
- **Bundling 8 fixes into one commit is hard to review.** Even though I was the only reviewer, the commit body is dense. Future practice: when 8 bugs cluster, split into 8 commits with each having full context, then bundle in a PR.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Add `.github/workflows/docker_compose_smoke.yml` — bring up the local stack, verify all 4 producers + Spark + Schema Registry, run for 60s, tear down | PulseTrack DE | done in commit `ff72f5c` | DONE |
| 2 | Document "Day-1 integration test setup" as part of new-project bootstrap checklist | PulseTrack DE | 2026-05-10 (Prompt 9 onboarding doc) | P0 |
| 3 | Audit all other argparse usage for similar default-shadowing bugs | PulseTrack DE | 2026-05-15 | P1 |
| 4 | Establish project convention: enum comparisons must be enum-to-enum, NEVER string-to-enum (lint rule if possible) | PulseTrack DE | 2026-05-15 | P1 |
| 5 | Standardize: every project component that emits metrics has its own port (config.py) — never share | PulseTrack DE | 2026-05-15 | P2 |

## Lessons learned

**P0 bugs cluster around integration seams.** When 8 P0s land in one session, you're not unlucky — your test coverage has a hole. Find the hole, plug it, move on. Don't fix bugs reactively; fix the meta-pattern.

**Atomic commits are the right call when fixes are mutually dependent.** Q1 and Q2 are independent bugs but Q3 and Q4 are the same argparse pattern in two places — fixing one without the other would be incomplete. Bundle when the bugs share architecture; split when they're truly independent.

**Cross-module CI is non-negotiable for production-grade projects.** Unit tests verify "this function works." Integration tests verify "these functions work together." Cross-module CI verifies "the system works." All three are necessary; none substitutes for the others.

**"Module developer is the reviewer" doesn't work for cross-module changes.** Port allocation is a cross-module concern. Even in a 1-person project, a checklist functioning as "the second reviewer" is the way to enforce this rigor.

## References

- Runbooks: `runbooks/emr_step_failure.md` (general process-restart playbook), `runbooks/dlq_buildup.md` (S2 + S3 docker-stack related)
- Related ADRs: `docs/adrs/ADR-002-emr-over-databricks.md` (covers local dev stack philosophy)
- Related commits: `a25027e` (the fix), `ff72f5c` (the docker_compose_smoke CI follow-up)
- External: argparse docs on default precedence, Kafka health-check best-practice docs, ZK 3.7 4lw whitelist docs
