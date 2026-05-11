# Postmortem: Anthropic + Slack secrets echoed to terminal via shell `source .env`

**Date:** 2026-05-11
**Severity:** SEV2 (real credential leak into ephemeral conversation transcript; scope time-bounded)
**Status:** Mitigated (rotation queued for post-Phase-2 teardown per Path B decision)
**Authors:** PulseTrack DE
**Anchor commit / change:** N/A — runtime incident, not a code change. Triggered during Phase-2 execution of Prompt 9.

## Summary

While running `bootstrap_secrets.py` to populate AWS Secrets Manager, the `PT_SNOWFLAKE_*` env vars were missing from `os.environ` (only loaded into the pydantic `Settings` class). The mitigation chosen was `set -a && source .env && set +a && python3 scripts/bootstrap_secrets.py`. The `set -a` (auto-export) approach causes Bash to evaluate each line of `.env` — and **lines whose values contain shell metacharacters (`:`, `/`, spaces, etc.) are interpreted as commands**, with the value echoed verbatim in the resulting "command not found" error.

The Anthropic API key (`sk-ant-api03-...`) and Slack webhook URL were both echoed in stderr because they contain `:` (from URL scheme) or other characters that triggered the shell to try executing parts of them as commands. Those values are now present in this conversation's transcript log, effectively leaking them to anyone with access to the transcript.

The bootstrap succeeded — the Snowflake secret was correctly pushed to AWS Secrets Manager — but the credentials are also leaked. Rotation is required.

## Impact

- **Blast radius:**
  - **Anthropic API key:** any party with the conversation transcript can issue calls billed to our account. Our budget cap is $1/test, but unauthorized use could exhaust it and could be used to fingerprint our usage patterns.
  - **Slack webhook URL:** any party can post to `#pulsetrack-alerts` channel. Worst case: spam, phishing attempt against operators watching the channel.
  - **WHOOP client_secret:** was already on the rotation queue (from the earlier 2026-05-09 incident); this leak doesn't change anything for WHOOP — already scheduled.
- **Customer-facing impact:** None. No customer data accessible via any of these.
- **Data impact:** None.
- **Cost impact:** Bounded to ~$1 (Anthropic cap) IF the key is exfiltrated and used. Slack: $0.
- **Duration of exposure:** From the moment of the shell-source error (~2026-05-11 05:08 UTC) until rotation completes (estimated +~2 hours, post-Phase-2 teardown).

## Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-11 04:55 | bootstrap_secrets.py ran successfully for 5/6 secrets; Snowflake skipped because `PT_SNOWFLAKE_*` not in `os.environ` (only in `.env` loaded by pydantic-settings, not exported to shell) |
| 2026-05-11 05:08 | Mitigation applied: `set -a && source .env && set +a && python3 scripts/bootstrap_secrets.py --include snowflake` |
| 2026-05-11 05:08 | `set -a` (auto-export) triggers shell evaluation of every `.env` line. Lines with `:`-containing values (Anthropic key, Slack URL, scopes) parse as commands → bash logs "command not found: <value>" to stderr |
| 2026-05-11 05:08 | Snowflake bootstrap step still succeeded — `os.environ` got the values via the auto-export path |
| 2026-05-11 05:08 | Operator (me) saw the echoed values in the bash output, recognized as a credential leak |
| 2026-05-11 05:09 | Classified as SEV2 per blast-radius analysis (small scope: dev-tier keys + $1 cap + read-only API) |
| 2026-05-11 05:09 | Path B chosen: continue Phase 2, rotate after teardown |
| 2026-05-11 ~07:00 (planned) | Rotate Anthropic + Slack webhook + WHOOP (the latter was already on the rotation queue) |

## Root cause

Two separate problems combined:

1. **bootstrap_secrets.py reads `os.environ` for Snowflake (not Settings):** because Snowflake fields aren't in the pydantic `Settings` class in `config.py`, the script falls back to direct `os.environ.get("PT_SNOWFLAKE_*")` calls. Pydantic loads `.env` into `Settings` but does NOT auto-export to the shell's `os.environ`. So the values were "in the .env file" but invisible to `os.environ.get()`.

2. **The mitigation used `set -a` (Bash's auto-export):** which works by *literally executing* each line as a variable assignment. For values like `https://hooks.slack.com/services/...` the colon-after-https makes Bash parse it as `https=://hooks.slack.com/services/...` (assignment), then it sees the next colon and tries to execute the URL fragments. The "command not found" error logs the exact value being interpreted.

The right pattern was either:
- Use `python-dotenv` inside `bootstrap_secrets.py` to load `.env` independently of pydantic
- Use `export $(grep -v '^#' .env | xargs -d '\n')` (no shell evaluation, just clean assignments)
- Use a quoted/escaped form of `set -a`

The chosen pattern (`set -a && source .env`) is the most fragile because it's lossy: it works for simple key=value lines but corrupts any line with shell metacharacters in the value, and worse — *echoes the secret value to stderr while corrupting*.

## 5 Whys

1. **Why was the Anthropic key echoed to the terminal?** Because `set -a && source .env` evaluated lines containing URL-like values as shell commands, which fails and prints the value in the error message.
2. **Why was `set -a && source .env` chosen?** Because `bootstrap_secrets.py` reads Snowflake fields from `os.environ` (not from pydantic Settings), and the user's `.env` values weren't auto-exported.
3. **Why does `bootstrap_secrets.py` read from `os.environ` and not from Settings?** Because the Snowflake fields were never added to `config.py`'s `Settings` class — they were treated as "external" vars.
4. **Why was Snowflake left out of Settings?** Because Settings was designed for "core" pipeline vars (Kafka, Iceberg paths, etc.) and Snowflake felt like a downstream concern that was bolted on later (prompt 8).
5. **Why didn't a code review catch the inconsistency?** Because it's a 1-developer project with no second reviewer, and there's no automated lint rule for "every PT_ env var should be in Settings AND read via Settings everywhere."

The fifth "why" lands on a missing **convention enforcement**: every secret pathway should go through `pt_secrets.manager.get_secret(...)`, never through `os.environ.get()` directly. The bootstrap script is the one exception (it needs raw env to write *into* Secrets Manager), but even then it should load via `python-dotenv` to avoid shell-sourcing.

## Trigger

Running `bootstrap_secrets.py --include snowflake` after the initial 5/6-success bootstrap returned a SKIP for Snowflake. The operator (me) chose the fastest path forward (`set -a && source .env`) without thinking through the shell-metacharacter implications.

## Resolution

**Forward-prevent (planned, post-Phase-2):**
1. Update `scripts/bootstrap_secrets.py` to use `python-dotenv` for loading `.env` instead of relying on shell sourcing:
   ```python
   from dotenv import load_dotenv
   load_dotenv()  # populates os.environ from .env without shell evaluation
   ```
2. Add Snowflake fields to `config.py`'s `Settings` class so they flow through `pt_secrets.manager.get_secret()` like the others.
3. Document in `runbooks/secret_leak_response.md` the additional pattern: "Never use `set -a && source .env` — values with `:` / `/` / spaces will echo to stderr."

**Immediate mitigation (Path B):**
- Acknowledged leak in real time. Continued Phase 2 (the bootstrap was already in Secrets Manager).
- Rotation scheduled for post-Phase-2 teardown:
  - Anthropic: regenerate at https://console.anthropic.com/settings/keys, update `pulsetrack/dev/anthropic`, revoke old key.
  - Slack: regenerate webhook at https://api.slack.com/apps/, update `pulsetrack/dev/slack`, revoke old URL.
  - WHOOP: already on the queue (separate 2026-05-09 incident).

## What went well

- **Detection was immediate** — the values appeared in the same terminal window where I was working. Caught in the moment, not in a post-hoc audit.
- **The bootstrap operation still succeeded** — the Snowflake secret correctly landed in AWS Secrets Manager despite the shell error noise.
- **Scope was correctly bounded by Severity-2 classification** — Path A (pause + rotate immediately) was considered but Path B (continue + rotate after) was chosen based on real risk analysis ($1 max Anthropic cap, dev-tier WHOOP, Slack chat-only). Not all leaks need a 3-AM page.
- **The leak was self-documented** — the postmortem (this file) was queued before the rotation step, keeping the response auditable.

## What didn't go well

- **The `set -a && source .env` pattern is fundamentally lossy and shouldn't have been chosen.** It's a well-known Bash gotcha that I should have anticipated.
- **The `bootstrap_secrets.py` script's reliance on `os.environ` for Snowflake (vs. Settings) is the deeper bug.** Without that, the shell-sourcing workaround wouldn't have been needed.
- **The Snowflake config gap (not in `Settings`) was added in prompt 8 and never reviewed for consistency** with the other PT_* vars. Tech-debt accumulation in a 1-person project.
- **No automated test** caught that running `bootstrap_secrets.py` against a fresh `.env` would skip Snowflake silently. A CI test ("bootstrap with valid .env should populate all 6 secrets") would have surfaced this.
- **Severity classification was self-assessed.** In a team setting, this would page security regardless of believed scope — there's a reason that convention exists. As a 1-person team, I made the call, but a team-of-N would have escalated.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Rotate Anthropic API key (regenerate, update Secrets Manager, revoke old) | PulseTrack DE | 2026-05-11 (post-teardown) | P0 |
| 2 | Rotate Slack webhook URL | PulseTrack DE | 2026-05-11 (post-teardown) | P0 |
| 3 | Rotate WHOOP client_secret (combined with existing 2026-05-09 rotation) | PulseTrack DE | 2026-05-11 (post-teardown) | P0 |
| 4 | Replace `set -a && source .env` with `python-dotenv` in `bootstrap_secrets.py` | PulseTrack DE | 2026-05-15 | P1 |
| 5 | Add `PT_SNOWFLAKE_*` fields to `config.py`'s `Settings` class | PulseTrack DE | 2026-05-15 | P1 |
| 6 | Add CI test: `pytest -k test_bootstrap_secrets` that verifies all 6 secrets populate from a fixture `.env` | PulseTrack DE | 2026-05-22 | P2 |
| 7 | Add `runbooks/secret_leak_response.md` entry: "Don't use `set -a && source .env`" | PulseTrack DE | 2026-05-15 | P2 |
| 8 | Add CloudTrail watch for unexpected Anthropic API key origins (future SOC2 work) | PulseTrack DE | 2026-Q3 | P3 |

## Lessons learned

**`set -a && source .env` is a known-bad pattern in Bash for any file that contains URLs, paths, or strings with metacharacters.** It works only for simplest key=plain_string cases. The convention should be: use `python-dotenv` or `dotenv-cli` or `direnv`, never bash sourcing.

**Bootstrap scripts that need raw env should load it deterministically.** The `bootstrap_secrets.py` script's job is to *write* env contents to Secrets Manager. It should load `.env` itself with `python-dotenv`, not depend on the shell having pre-exported the values.

**Tech-debt accumulates fastest in 1-developer projects.** Without a second reviewer, conventions drift. `Settings` was the convention; Snowflake was added without following it. The fix isn't more discipline — it's an automated lint rule that says "no `os.environ.get('PT_*')` outside `pt_secrets/manager.py`."

**Severity classification should be conservative and the response should be uniform.** Whether the leak is dev-tier or prod, the procedure (rotate, scrub, audit, postmortem) is the same. The only difference is response time. Path A vs. Path B in this case was a calculated risk based on real cost/scope — but a team would have defaulted to Path A regardless.

## References

- Runbook: `runbooks/secret_leak_response.md` (will be updated with the new pattern)
- Related ADRs: `docs/adrs/ADR-006-secrets-manager-over-env.md` (the original "no .env" decision)
- Related code: `scripts/bootstrap_secrets.py`, `config.py:Settings`, `pt_secrets/manager.py`
- Related postmortems: `postmortems/2026-05-09_whoop_secret_in_git.md` (the prior leak in git history)
- External: Bash `set -a` documentation, OWASP "Secrets in CI/CD" guidance
