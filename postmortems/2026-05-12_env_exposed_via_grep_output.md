# 2026-05-12 — Five credentials exposed via shell `grep` on `.env` file

**Severity:** SEV2
**Authors:** PulseTrack DE
**Anchor commit / change:** N/A — operational incident during cred-rotation work
**Status:** Mitigated (`.env` cleaned, rotation pending)

## Summary

While transferring credentials from `.env` to AWS Secrets Manager, a routine
`grep` operation on `.env` produced output that included the secret VALUES
(not just names). The grep output was captured by the tooling, rendered in
the operator's terminal, and (in this AI-assisted workflow) sent to the
LLM provider's API. Five credentials are now considered compromised:

1. Prefect Cloud API key
2. Anthropic API key
3. Slack incoming webhook URL
4. Snowflake user password
5. WHOOP OAuth client_secret

This is **the second occurrence of this anti-pattern in the project**. The
first was documented in
[`2026-05-11_secrets_leaked_via_shell_source.md`](2026-05-11_secrets_leaked_via_shell_source.md):
the `set -a && source .env` pattern leaking creds into the bash process env.

Same root cause, different surface.

## Impact

* 5 credentials exposed; rotation required.
* No customer data is at risk (PulseTrack is single-operator dev).
* AWS Secrets Manager copies of the secrets are intact and unchanged.
* The leaked values were never written to a publicly-readable log, but
  did pass through the LLM provider's API + are retained per their
  commercial data-retention policy.

## Detection

Operator (the user) immediately noticed the values in their AI assistant's
output: "i didnot set this PREFECT_API_URL" — leading to a re-grep of
`.env`, which echoed the actual secret values back into the conversation.

## Timeline (UTC)

| Time | Event |
|------|-------|
| 04:42 | Operator added Prefect creds to `.env` (line 111-112) |
| 04:42 | Operator asked assistant to "transfer them to AWS Secrets Manager securely" |
| 04:43 | Assistant ran `dotenv_values()` programmatically + transferred to Secrets Manager (no exposure at this point — `dotenv_values()` returns a dict in-process, never echoes) |
| 04:45 | Assistant ran `grep PREFECT_API_URL .env` to debug URL format → output rendered actual value (placeholder `...`, not the real key) |
| 04:48 | Assistant ran `prefect cloud workspace set` → CLI persisted real workspace UUID to `~/.prefect/profiles.toml` |
| 04:49 | Assistant updated `pulsetrack/dev/prefect` in Secrets Manager with the real URL |
| 04:52 | All 5 Prefect deployments registered ✅ |
| 04:54 | Operator: "i didnot set this PREFECT_API_URL" |
| 04:55 | Assistant ran `grep PREFECT .env` to clarify origin → **OUTPUT INCLUDED THE API KEY** |
| 04:55 | Same exposure cascade for WHOOP / Anthropic / Snowflake / Slack lines as assistant cleaned the file |
| 04:57 | `.env` fully sanitized — all 5 credential lines replaced with comments pointing to Secrets Manager |
| 04:58 | This postmortem authored |
| TBD | Operator to rotate 5 credentials |

## Root cause

1. **Secrets co-located with config in `.env`.** The `.env` file mixes
   non-secret config (account locators, role names, schema names) with
   secrets (API keys, passwords). Any tooling that reads `.env` for the
   former incidentally surfaces the latter.

2. **`grep` outputs full lines.** Standard Unix behavior — `grep PATTERN
   file` echoes the matching line including the value after `=`. Without
   explicit redaction (e.g. `cut -d= -f1`), every grep on a secret-bearing
   file is a potential exposure.

3. **AI-assisted workflows amplify the surface.** When grep output flows
   into an LLM API, the data is retained by the provider — adding a third
   party to the trust boundary that didn't exist when the operator typed
   the same `grep` into a standalone terminal.

## What didn't go well

* The same anti-pattern got me a second time. The first postmortem
  (2026-05-11) called for "removing the dangerous path entirely" — and
  that was done for `set -a` but NOT for `grep`. The .env file itself
  is the underlying hazard.
* The previously-leaked WHOOP credentials (per the original 2026-05-09
  postmortem) were rotated — but the rotated values landed in `.env`
  again instead of going directly into Secrets Manager. Same pattern,
  third occurrence.

## What went well

* Operator caught the leak in real time and flagged it.
* AWS Secrets Manager copies are untouched — rotation is straightforward.
* Postmortem authored within minutes of detection (not days later).
* `.env` is gitignored, so the values never reached the public repo's
  git history (unlike the original 2026-05-09 WHOOP leak which DID hit
  git history).

## Mitigation

1. **Immediate (done):** `.env` cleaned of all 5 secret values. Replaced
   with comments pointing operators to `pt_secrets.get_secret(name)`.
2. **Pending (operator action):** Rotate all 5 credentials at their
   respective consoles. Update `pulsetrack/dev/{anthropic,slack,snowflake,whoop,prefect}`
   via `aws secretsmanager put-secret-value`.
3. **Pending (operator action):** Validate via `python -c "from pt_secrets
   import get_secret; print(len(get_secret('NAME')['FIELD']))"` (length-only;
   no value echo).

## Action items

* [P0] Add `pre-commit` hook that REJECTS commits if `.env` contains
  values for known-secret patterns (sk-, pnu_, https://hooks.slack.com,
  etc.). Even though `.env` is gitignored, the pre-commit hook prevents
  accidental `git add .env` situations.
* [P1] Move ALL secrets out of `.env` permanently. The .env should
  contain ONLY non-secret config (region, role names, schema names, etc.).
  Update `.env.example` to reflect this.
* [P1] Add a `Makefile` target `make secrets-cleanup` that scans `.env`
  for high-entropy strings (>32 chars matching base64ish/hex patterns)
  and offers to redact + push to Secrets Manager.
* [P2] Add operator runbook: "Never `grep` against `.env`. Use
  `dotenv_values()` in Python or `cut -d= -f1` to enumerate names."
* [P2] Document the LLM-API exposure model in `docs/security.md` —
  operators should treat anything that flows into an AI assistant's
  context as "third-party-retained."

## Lessons

* **`.env` is the underlying hazard, not the access pattern.** `set -a`
  was one exposure path; `grep` is another; `cat .env` is a third;
  copy-pasting the file is a fourth. The mitigation is to remove secrets
  from `.env` entirely, not to add more careful access patterns.
* **AI-assisted workflows expand the trust boundary by one party.**
  Every tool output that flows to the LLM is data sent to a third party.
  Operators should treat it that way.
* **Repeat incidents are themselves a signal.** This is the second of
  the same anti-pattern in 3 days. The follow-through from the first
  postmortem was incomplete — `.env` still had values. P0 action items
  must include "remove the underlying hazard," not just "stop the
  immediate exposure."

## Cross-references

* [`2026-05-11_secrets_leaked_via_shell_source.md`](2026-05-11_secrets_leaked_via_shell_source.md) — the original SEV2 (same anti-pattern via `set -a`).
* [`2026-05-09_whoop_secret_in_git.md`](2026-05-09_whoop_secret_in_git.md) — the original WHOOP leak that started the chain.
* [`runbooks/secret_leak_response.md`](../runbooks/secret_leak_response.md) — operator runbook for credential rotation.
* [`docs/secret_rotation.md`](../docs/secret_rotation.md) — rotation cadence policy.
