# ADR-006: AWS Secrets Manager over .env Files

## Status
Accepted

## Date
2026-05-10

## Context

On 9 May 2026, a routine `.gitignore` audit surfaced that a commit several
months prior had captured a `.env` file containing live WHOOP OAuth client
credentials and the user-token JSON. The credentials had been in git
history — and in any clone of the repository — for the entire intervening
period. (See `postmortems/2026-05-09_whoop_secret_in_git.md` for the full
incident timeline, response, and credential rotation log.)

This wasn't a tooling failure. `.gitignore` was correctly listing `.env` at
the time of the leaking commit — the file had been force-added with `git add -f`
during a debugging session and the developer forgot to revert before
pushing. The failure mode is structural: **a credential-handling strategy
that depends on every developer doing the right thing every time eventually
loses to a tired developer doing the wrong thing once.**

The forces driving the redesign:

- We need credentials at runtime for: WHOOP OAuth, Anthropic API, Snowflake,
  Slack webhook, PagerDuty, OpenFDA (no auth needed).
- We need an audit trail: who/when/what accessed any given secret.
- We need rotation to be cheap (preferably automated).
- We need workload identity — no long-lived access-key-id/secret-access-key
  pairs in the loop.
- We need a recurrence-prevention gate: even if someone tries to commit a
  new `.env` with live credentials, the system should refuse.
- Local development must still work for contributors without AWS access.

## Decision

Migrate to **AWS Secrets Manager** as the canonical secret store, with a
**three-tier resolution facade** in `pt_secrets/manager.py` that preserves
local-dev workflows:

```
Tier 1: AWS Secrets Manager     pulsetrack/<env>/<service>     (production)
   ↓ (miss)
Tier 2: Process environment     PT_<SERVICE>_<FIELD>           (CI, ad-hoc)
   ↓ (miss)
Tier 3: .env via pydantic       config.settings                (local dev)
```

Concrete additions:

- **Customer-managed KMS key** (envelope encryption, 1-year auto-rotation)
  for all PulseTrack secrets. EMR EC2 role gets `kms:Decrypt` only — not
  `kms:Encrypt` — so a compromised EMR node can read secrets but cannot
  ship a new ciphertext blob.
- **CloudTrail data events** on `GetSecretValue` calls (audit).
- **IAM scope**: EMR EC2 role can `GetSecretValue` on all PulseTrack secrets,
  but `PutSecretValue` is scoped to `whoop-tokens` only (the producer's
  refresh-token writeback path). A compromised producer cannot rewrite
  other secrets.
- **30-day recovery window** in prod, 7-day in dev — accidental
  `terraform destroy` doesn't instantly nuke live credentials.
- **In-memory cache** in the Python facade (default TTL 15 min) to keep
  AWS-API cold-start latency off the hot path.
- **Recurrence-prevention gates**: pre-commit `gitleaks` hook +
  CI-side `trufflehog` gate. A new attempt to commit a `.env` with a
  detectable secret pattern is blocked at commit time *and* at PR time.

## Consequences

**Positive**:
- Full audit trail via CloudTrail. We can answer "who fetched the WHOOP
  client secret in the last 90 days?" with a CloudTrail Insights query.
- Rotation-friendly. Updating a value in Secrets Manager + calling
  `manager.invalidate(short_name)` on consumers is the whole rotation
  procedure. No code changes, no redeployments.
- Workload identity. EMR EC2 role pattern means no AWS-key pairs anywhere.
  IAM principal is the identity.
- Recurrence prevention is structural. Even a developer who tries to
  commit a `.env` with live credentials is blocked by the gitleaks pre-commit
  hook locally and the trufflehog gate in CI.
- Local dev still works. Contributors without AWS credentials hit Tier 3
  (`.env`) — same workflow as before, except the `.env` file now must be
  populated locally and cannot contain secret patterns that gitleaks
  flags.

**Negative**:
- One more service dependency. AWS Secrets Manager outages affect us.
  Mitigated by the in-memory cache and the env/dotenv fallback tiers, but
  a cold-start during a Secrets Manager outage would fail.
- Cold-start latency ~50–150ms per uncached secret. Mitigated by the
  15-min in-memory cache and by `prefetch([...])` at flow startup.
- Small operational surface for managing the secrets: the
  `scripts/bootstrap_secrets.py` script populates empty TF-created secrets
  on first deploy; ongoing rotation is a manual console operation (Phase 2:
  Lambda-based rotation).

## Alternatives Considered

- **HashiCorp Vault**: rejected. Another service to operate (HA + storage
  backend + auth backend + audit backend). The AWS-native integrations
  (KMS envelope, IAM principal, CloudTrail audit) come for free with
  Secrets Manager and would have to be rebuilt on top of Vault.
- **Doppler / 1Password Secrets / Akeyless**: rejected. Vendor lock-in to
  a non-AWS provider, with a network hop on the secret resolution path.
  Also: Secrets Manager + KMS is HIPAA-eligible AWS service; the
  third-party stores would each need their own BAA evaluation.
- **Continue with `.env` + better `.gitignore` + developer education**:
  rejected. This is the strategy that just failed. Improving developer
  education doesn't change the structural failure mode. The pre-commit
  hook is a better gate than education, but it works equally well as a
  *belt* on top of Secrets Manager (the *suspenders*).
- **AWS Parameter Store with SecureString**: rejected, but it was close.
  Parameter Store is cheaper and adequate for non-rotating config. We
  picked Secrets Manager because (a) rotation Lambdas are a roadmap
  feature we want to grow into, (b) Secrets Manager has explicit JSON
  schema support that maps cleanly to our `dict[str, str]` boundary
  contract, (c) recovery-window semantics protect against accidental
  destroy in a way Parameter Store doesn't.

## References

- `/Users/nerdboss-stm/pulsetrack-cm/pt_secrets/manager.py` — Python facade
  with three-tier resolution.
- `/Users/nerdboss-stm/pulsetrack-cm/infrastructure/modules/secrets/main.tf`
  — Terraform module (KMS key, secrets, IAM policy, EMR attachment).
- `/Users/nerdboss-stm/pulsetrack-cm/scripts/bootstrap_secrets.py` —
  bootstrap script that populates TF-created empty secrets.
- `/Users/nerdboss-stm/pulsetrack-cm/.gitleaks.toml` — pre-commit secret
  scanning config.
- `/Users/nerdboss-stm/pulsetrack-cm/postmortems/2026-05-09_whoop_secret_in_git.md`
  — incident postmortem (forward reference; to be written as part of the
  incident response).
- Commit `4a39930` — "feat(secrets): AWS Secrets Manager migration — TF
  module + Python facade + bootstrap".
- Commit `62a8514` — "chore(security): pre-commit gitleaks + CI trufflehog
  gate".
- Related: ADR-002 (EMR — IAM principal pattern relies on the EMR EC2 role).
