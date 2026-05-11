# Postmortem: WHOOP client secret discovered in committed `.env`

**Date:** 2026-05-09 (discovered during Prompt 9 exploration)
**Severity:** SEV2 (developer creds in repo history; not production-prod, not customer-facing data)
**Status:** Mitigated (rotation pending — listed in action items)
**Authors:** PulseTrack DE (self-reported)
**Incident commander:** N/A (pre-launch, self-audit)
**Anchor commit / change:** The leak was traced to an early `.env` commit, predating the gitignore entry. Remediation builds on [`4a39930`](https://github.com/.../commit/4a39930) (Secrets Manager migration) + [`62a8514`](https://github.com/.../commit/62a8514) (gitleaks + trufflehog gates).

## Summary

During the Prompt-9 secrets-handling exploration, the read-only `Explore` agent reported that `.env` at the repo root contained `PT_WHOOP_CLIENT_ID`, `PT_WHOOP_CLIENT_SECRET`, `PT_WHOOP_ACCOUNT_ID`, and `PT_WHOOP_USER_EMAIL` in plain text, and that this file was present in git history despite being `.gitignore`'d at the current HEAD. The credentials are for a developer-tier WHOOP app (no customer data, no production access), but the leak is a real exposure that requires rotation + history scrub.

Severity is **SEV2** rather than SEV1 because:
- The leaked app is **developer-tier**, not production
- It can only access **one user's WHOOP account** (the developer's own data — read scopes only)
- The repo is **private** (no public exposure)
- Credentials are NOT yet rotated at time of writing — that's the immediate action item

If this had been a production credential or the repo were public, it would be SEV1 with same-hour rotation + customer comms.

## Impact

- **Blast radius:** One WHOOP developer app's `client_id` + `client_secret`. Read-only scopes on a single account.
- **Customer-facing impact:** None. No customer data accessible with these credentials.
- **Data impact:** None. The WHOOP API is read-only from our side.
- **Cost impact:** None directly. An attacker with these credentials could exhaust the developer-tier rate limit, blocking our legitimate use.
- **Duration:** Credentials were in git history from initial `.env` commit (precise date pending git archaeology) through 2026-05-09. **Window of exposure: ~37 days.**

The blast radius is small but the principle matters: this is exactly the credential-leak shape that recurs in industry, and the response procedure should be the same as if it were production.

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-04-03 ~17:00 | Initial `.env` commit (containing WHOOP creds in plain text). Found via `git log --diff-filter=A -- .env` |
| 2026-04-03 → 2026-05-09 | Credentials present in HEAD for some commits, in history for all. Multiple commits added/removed lines around them. |
| 2026-05-09 14:23 | Prompt-9 Explore agent surfaces the finding while mapping the credentials landscape |
| 2026-05-09 14:25 | Self-classified as SEV2 (per blast-radius analysis above) |
| 2026-05-09 14:30 | Decision: build Secrets Manager path first, then rotate during Phase 2 (T-25m of scale test) |
| 2026-05-09 16:42 | Commit `4a39930` — Secrets Manager TF + Python facade + bootstrap |
| 2026-05-09 16:45 | Commit `62a8514` — gitleaks pre-commit + trufflehog CI gate (prevents recurrence) |
| 2026-05-10 (Phase 2 T-25m) | **PENDING:** rotate WHOOP secret at developer.whoop.com, write new value to Secrets Manager, revoke old |
| 2026-05-10 (Phase 2 T-20m) | **PENDING:** BFG-rewrite git history to scrub `.env` (only after team approval — rewriting history on a shared branch needs explicit OK) |

## Root cause

Two intertwined causes:

1. **`.env` was committed before `.gitignore` was tightened to include it.** A standard early-project mistake — the gitignore entry was added later (post-hoc), but didn't backfill history. Result: every commit between the initial `.env` commit and the gitignore tightening had the file tracked.

2. **No pre-commit secret scanning existed.** The pre-commit config (`.pre-commit-config.yaml`) ran black, ruff, and pre-commit-hooks utilities, but **no gitleaks/trufflehog**. The first commit could have been blocked at developer's machine, eliminating the bug at source.

The combination of "secrets in repo + no scanning" is a class of defect that should be impossible by construction, not relied on by developer vigilance.

## 5 Whys

1. **Why are WHOOP credentials in git history?** Because `.env` was committed in plain text.
2. **Why was `.env` committed?** Because `.gitignore` didn't yet include it, and the developer (early-project rush) didn't realize `.env` was being tracked.
3. **Why didn't `.gitignore` include it?** Because the project bootstrap used a generic Python template that didn't mention `.env` — added later when WHOOP credentials were introduced.
4. **Why didn't pre-commit catch it?** Because no secret-scanning hook was configured at the time.
5. **Why was no secret-scanning hook configured?** Because no checklist for "things every Python repo should have on day 1" exists, and security tools weren't part of the initial scaffolding mindset.

The fifth "why" lands on a missing **project bootstrap checklist** — a doc that says "before your first commit, you must: gitignore .env, install gitleaks pre-commit, set up Secrets Manager." This is a senior-DE-defined org standard that should exist before any junior or contractor starts a new pipeline.

## Trigger

No external trigger. The credentials sat dormant in history until the Prompt-9 secrets exploration agent was specifically prompted to "find every place secrets are read from today" — which led it to inspect `.env` and report on the contents. Had we not done this audit, the credentials would still be there, possibly indefinitely.

This is itself a finding: **security audits should be proactive, not reactive.** A periodic scan of every repo would have caught this in week 2, not week 5.

## Resolution

Two-part fix:

1. **Forward-prevent recurrence (committed):**
   - Commit `4a39930` — Secrets Manager replaces `.env` as the source of truth. Local `.env` becomes the tier-3 fallback only (with a warning log on use).
   - Commit `62a8514` — `gitleaks` pre-commit hook + CI `secret_scan.yml` (gitleaks-action + trufflehog) prevents any future commit from containing a credential pattern.

2. **Remediate the existing leak (Phase 2 T-25m):**
   - Rotate WHOOP secret at developer.whoop.com (generate new `client_secret`, revoke old).
   - `aws secretsmanager put-secret-value --secret-id pulsetrack/dev/whoop --secret-string '...'` with new value.
   - History scrub via BFG: `bfg --delete-files .env` + `git reflog expire --expire=now --all && git gc --prune=now --aggressive`. Force-push to remote. **Only after team approval** for the history rewrite (we're a 1-person team for now, but the principle stands).

## What went well

- The Explore agent **found this proactively** — without that audit, the bug would persist.
- The Secrets Manager + scanning-gate work was already planned (Prompt 9 Group F), so the remediation slotted into existing scope, no surprise work.
- The blast radius was contained: developer-tier credential, read-only scopes, private repo. A worse scenario would have required customer comms.
- We have a clear forward path (rotate + scan + Secrets Manager) and a documented schedule (Phase 2 T-25m).

## What didn't go well

- **The bug existed for 37 days before discovery.** No periodic secret audit existed. A monthly cron of `gitleaks detect --source .` would have caught it in week 1.
- **Severity classification was self-assessed.** In a team setting, this should escalate to a security review even if developer-tier; the convention of "if a credential leaks, page the on-call regardless of believed scope" exists for good reason.
- **The history scrub is deferred.** Even after rotation, the old credentials are still in the git history at the GitHub remote. Anyone with read access (currently 1 person) can still extract them. Full remediation requires the force-push, which requires team coordination.
- **No automated WHOOP-side detection.** If the leaked credential is used by an attacker (rate-limit exhaustion, suspicious user-agent), we'd only notice when our legitimate calls start failing. The WHOOP API doesn't expose audit logs at our tier.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Rotate WHOOP client_secret at developer.whoop.com, write to Secrets Manager, revoke old | PulseTrack DE | 2026-05-10 Phase 2 T-25m | P0 |
| 2 | BFG-rewrite git history to scrub `.env` from history | PulseTrack DE | 2026-05-10 (after rotation) | P0 |
| 3 | Add `runbooks/secret_leak_response.md` describing this exact procedure for future incidents | PulseTrack DE | 2026-05-10 (this commit) | P0 |
| 4 | Verify gitleaks pre-commit + CI gate catch a test credential (red-team self-test) | PulseTrack DE | 2026-05-15 | P1 |
| 5 | Monthly `gitleaks detect --source .` cron — add to maintenance Prefect flow | PulseTrack DE | 2026-05-31 | P1 |
| 6 | Document the project-bootstrap checklist (Day-1 security setup for any new pipeline) | PulseTrack DE | 2026-05-31 | P2 |
| 7 | If team scales: integrate with SOC monitoring (anomalous WHOOP API usage detection) | TBD | 2026-Q3 | P2 |

## Lessons learned

**A `.env`-only credential strategy is a known-failure pattern.** It works on day 1 — minimal friction, no AWS setup — and it fails on day N when someone commits the file. The industry has converged on Secrets Manager + workload identity for a reason. The cost of setting up Secrets Manager properly on day 1 is one afternoon; the cost of a leaked credential is rotation + history scrub + (potentially) customer comms + (potentially) breach disclosure. This trade is asymmetric and the right call is always Secrets Manager.

**Secret scanning should be installed before the first commit.** Not the first credential commit — the first commit. Gitleaks costs ~2 minutes to add; that 2-minute investment up front would have made this postmortem unnecessary.

**Periodic audits catch what daily flow misses.** The Prompt-9 exploration was a one-off audit. A team should have these on a calendar — monthly, automated, scoped. Even with all gates in place, a periodic external check is the belt-and-suspenders that catches the case where the gates themselves are misconfigured.

**Treat the small leaks like the big ones, in process.** Severity calibration is reasonable, but the RESPONSE shouldn't differ much: rotate, scrub, audit, learn, document, prevent. The only difference for SEV1 is the page time and the comms.

## References

- Runbook used: `runbooks/secret_leak_response.md` (newly written as part of this remediation)
- Related runbooks: `runbooks/whoop_oauth_renewal.md` (planned rotation cadence)
- Related ADRs: `docs/adrs/ADR-006-secrets-manager-over-env.md`
- Related commits: `4a39930` (Secrets Manager migration), `62a8514` (scanning gates)
- External: AWS Secrets Manager best practices; OWASP secrets-in-code top-10; GitGuardian state-of-secrets report
- Tooling: gitleaks, trufflehog, BFG repo cleaner
