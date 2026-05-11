# Runbook: Credential leak — immediate response

**Severity ladder:**
- SEV3: low-impact leak, no prod blast radius (expired test key in a never-merged PR; sandbox-only Slack webhook). Rotate within 24h, no postmortem.
- SEV2 (DEFAULT): any credential in a public commit, CI log, or chat message. Assume compromised. Rotate within 1h + postmortem.
- SEV1: production credential (`pulsetrack/prod/*`) OR broad blast radius (AWS IAM key, Snowflake role with write on prod, Anthropic key on billing-attached account). Page within 5 min, rotate within 30 min, audit within 1h.

**On-call response SLA:** SEV2 → ack 15 min, rotate 1h. SEV1 → page 5 min, rotate 30 min, audit 1h.

## TL;DR (30-second triage)

```bash
# Classify + locate in history + rotate at source + push to Secrets Manager + invalidate cache:
gitleaks detect --source . --config .gitleaks.toml -v --redact --no-banner
git log --all --full-history -p -S "<leaked-string-prefix>" | head -50
# (rotate at the provider — see per-service section below — then:)
aws secretsmanager put-secret-value --secret-id "pulsetrack/<env>/<service>" --secret-string '<json>'
python -c "from pt_secrets.manager import _default; _default.invalidate()"
```
The `pt_secrets.manager` has a 15-min TTL — without invalidate, producers use the dead credential for up to 15 min.

## Classification matrix (first 5 minutes)

Two axes: **what** leaked and **where**.

| What leaked | Severity | Time-to-rotate |
|-------------|----------|----------------|
| WHOOP `client_secret` or `refresh_token` | SEV2 | 1h |
| Snowflake password (non-prod role) | SEV2 | 1h |
| Snowflake password (prod role with write) | **SEV1** | 30 min |
| Anthropic `sk-ant-*` | SEV2 | 1h |
| Slack webhook URL | SEV3 (post-only, low blast) | 24h |
| AWS IAM access key | **SEV1** | 30 min |
| KMS key material | **SEV1** + audit | 30 min |
| `PT_*` env var with placeholder value | SEV3 (often false positive) | 24h |

**Where ≠ public.** A credential dropped into a CI log, Datadog log, Sentry breadcrumb, or chat message is as compromised as one on GitHub. SEV2 default. The only thing that demotes severity is a verifiable non-public path you control end-to-end.

## Symptoms (what triggered the page)

- GitHub Security alert: "Secret scanning detected a secret in commit `<sha>`"
- CI failure on `.github/workflows/secret_scan.yml` — `gitleaks` or `trufflehog` job failed (runs on every PR + push to main + Monday 08:00 UTC weekly sweep)
- Pre-commit `gitleaks` hook failed locally (the good outcome — incident-free)
- Manual report ("I just pasted a token in #general")
- Provider notification: WHOOP / Anthropic / Snowflake / GitHub emailed about anomalous activity
- AWS GuardDuty / IAM Access Analyzer alert on a credential used from an unexpected IP/region

## Diagnosis (commands to run first)

Confirm it's real, not a false positive. The `.gitleaks.toml` allowlist (lines 47-67) carves out `runbooks/`, `postmortems/`, `docs/`, and placeholder patterns (`<<FILL>>`, `YOUR_*_HERE`, `sk-ant-EXAMPLE`).
```bash
gitleaks detect --source . --config .gitleaks.toml -v --redact --no-banner
git show <commit-sha> -- path/to/file
```

A SHA-256 match in `dbt_project/` or `snowflake/` is `whoop-client-secret` (`.gitleaks.toml:20`) firing on dbt's `surrogate_key` macro output — allowlist at line 26 should suppress; if not, update the regex.

Find every occurrence (use the first 8 chars as the fingerprint — never the full value):
```bash
LEAK_PREFIX=$(echo -n "<full-leaked-value>" | head -c 8)
grep -rIn "$LEAK_PREFIX" . --exclude-dir=.git
git log --all --full-history --source -p -S "<full-leaked-value>" | head -100
aws logs filter-log-events \
    --log-group-name /aws/emr/pulsetrack-dev \
    --filter-pattern "$LEAK_PREFIX" \
    --start-time $(date -u -d '7 days ago' +%s)000
```

Blast radius per credential:
| Credential | Capability if exfiltrated |
|------------|---------------------------|
| WHOOP `client_secret` | Forge OAuth for our users; impersonate the integration |
| WHOOP `refresh_token` | Pull all WHOOP data for the bound account until rotated (30-day TTL) |
| Snowflake user/password | Any query the role grants — `SHOW GRANTS TO ROLE <role>`. `OWNERSHIP` on prod DB → SEV1 |
| Anthropic API key | Bill our account; rate-limit prod. Check usage dashboard last 24h |
| AWS IAM access key | Whatever the policy allows. Check attached policies + recent CloudTrail |
| Slack webhook | Post-only into one channel. No read. |

## Recovery (step-by-step, ranked by service)

### Step 1: Rotate at the source (1h for SEV2, 30 min for SEV1)

Do this BEFORE anything else — git-history scrubbing is meaningless if the live credential still works.

**WHOOP** (`pulsetrack/<env>/whoop` or `whoop-tokens`):
1. WHOOP developer portal → rotate client secret (invalidates all refresh tokens issued under it).
2. Update `pulsetrack/<env>/whoop`:
   ```bash
   aws secretsmanager put-secret-value --secret-id "pulsetrack/${PT_AWS_ENV}/whoop" \
       --secret-string '{"client_id":"...","client_secret":"<NEW>","redirect_uri":"http://localhost:8765/callback","account_id":"...","user_email":"..."}'
   ```
3. Run `scripts/whoop_auth_bootstrap.sh` to re-mint tokens (full procedure: `runbooks/whoop_oauth_renewal.md`).

**Snowflake** (`pulsetrack/<env>/snowflake`):
```sql
-- Snowflake session as ACCOUNTADMIN/SECURITYADMIN:
ALTER USER pulsetrack_loader SET PASSWORD = '<new-strong-password>' MUST_CHANGE_PASSWORD = FALSE;
-- If the role is suspect, drop unexpected tasks/streams:
SHOW TASKS IN DATABASE pulsetrack_prod;
SHOW STREAMS IN DATABASE pulsetrack_prod;
```
```bash
aws secretsmanager put-secret-value --secret-id "pulsetrack/${PT_AWS_ENV}/snowflake" \
    --secret-string '{"account":"...","user":"pulsetrack_loader","password":"<NEW>","role":"...","warehouse":"...","database":"...","schema":"..."}'
```

**Anthropic** (`pulsetrack/<env>/anthropic`): `console.anthropic.com/settings/keys` → revoke → create new →
```bash
aws secretsmanager put-secret-value --secret-id "pulsetrack/${PT_AWS_ENV}/anthropic" \
    --secret-string '{"api_key":"sk-ant-<NEW>"}'
```

**Slack** (`pulsetrack/<env>/slack`): Slack app dashboard → revoke webhook URL → generate new →
```bash
aws secretsmanager put-secret-value --secret-id "pulsetrack/${PT_AWS_ENV}/slack" \
    --secret-string '{"webhook_url":"<NEW>"}'
```

**AWS IAM access key** (rare — we use IAM roles for EMR/Prefect):
```bash
aws iam update-access-key --access-key-id AKIA... --status Inactive --user-name <user>
aws iam create-access-key --user-name <user>          # if a new one is needed
aws iam delete-access-key --access-key-id AKIA... --user-name <user>     # after 24h
```

### Step 2: Invalidate caches and force re-read

`pt_secrets/manager.py` caches for 15 min (`cache_ttl=900.0` at line 108). Without invalidation, producers/workers use the dead credential for up to 15 min.

```bash
sudo systemctl restart prefect-worker    # easiest path
# Or programmatic:
python -c "from pt_secrets.manager import _default; _default.invalidate()"
# EMR producers:
ssh hadoop@$MASTER_DNS "tmux send-keys -t whoop-producer C-c; sleep 2; tmux send-keys -t whoop-producer 'python -m data_generators.whoop_api.producer' Enter"
```

### Step 3: Audit access (within 1h of rotation)

CloudTrail for AWS API actions:
```bash
aws cloudtrail lookup-events \
    --start-time "$(date -u -d '14 days ago' +%Y-%m-%dT%H:%M:%SZ)" \
    --lookup-attributes AttributeKey=AccessKeyId,AttributeValue=AKIA... --max-results 50
# Secrets Manager GetSecretValue (CMK in infrastructure/modules/secrets/main.tf enables data events):
aws cloudtrail lookup-events \
    --start-time "$(date -u -d '14 days ago' +%Y-%m-%dT%H:%M:%SZ)" \
    --lookup-attributes AttributeKey=ResourceName,AttributeValue="pulsetrack/prod/whoop-tokens"
```

Flag any `sourceIPAddress` outside our VPC CIDRs, `userAgent` that isn't `boto3/...` / Prefect agent, or events outside scheduled flow-run windows.

Service-specific logs:
- **Snowflake:** `SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE USER_NAME = 'PULSETRACK_LOADER' AND EVENT_TIMESTAMP > DATEADD(day, -14, CURRENT_TIMESTAMP())`
- **Anthropic:** console.anthropic.com → Usage tab, filter by API key (only available pre-revocation)
- **WHOOP:** no per-token access log; assume worst-case for the exposure window
- **Slack webhook:** post-only and unauthenticated — just visually inspect the channel for unexpected posts

If audit shows unauthorized access, escalate to SEV1 and expand the postmortem into a security-incident postmortem (`postmortems/YYYY-MM-DD_security_<service>.md`) with explicit data-exposure scope.

### Step 4: Scrub git history (ONLY if leaked in a commit)

History rewriting on shared branches breaks every clone. Don't run BFG / `git filter-repo` on `main` without team approval — coordinate in `#data-platform-ops` first.

- Feature branch, not merged, < 10 commits since: scrub.
- `main`: scrub only after agreement; everyone re-clones.
- Public repo: scrub immediately, **but Step 1 rotation is what actually matters** — the credential is already compromised.

BFG procedure (preferred over `git filter-branch`):
```bash
git clone --mirror git@github.com:your-org/pulsetrack-cm.git pulsetrack-cm.bfg
cd pulsetrack-cm.bfg
echo '<full-leaked-value>' > replacements.txt
java -jar bfg.jar --replace-text replacements.txt    # replaces with ***REMOVED***
git reflog expire --expire=now --all && git gc --prune=now --aggressive
git push --force                                     # destructive — team must be ready
```

After force-push: optionally add the rewrite SHA to `.gitleaks.toml` `commits = [...]` (line 71) to suppress future scans on the artifact. Every developer must `git fetch && git reset --hard origin/<branch>`.

### Step 5: Confirm the scanning gate would catch this next time

The point of the postmortem isn't blame — it's "would our gates catch this exact pattern?" Compare the leaked credential against the rules in `.gitleaks.toml`:

| Rule id | Pattern (line) | Catches |
|---------|----------------|---------|
| `pulsetrack-env-literal` | `PT_[A-Z_]+\s*=\s*[A-Za-z0-9+/=_-]{16,}` (line 17) | `.env` shapes |
| `whoop-client-secret` | `[A-Fa-f0-9]{64}` (line 23) | WHOOP secret format |
| `anthropic-api-key` | `sk-ant-[A-Za-z0-9_-]{40,}` (line 32) | Anthropic |
| `snowflake-password` | `PT_SNOWFLAKE_PASSWORD\s*=\s*\S+` (line 38) | Snowflake |
| `slack-webhook` | `https://hooks\.slack\.com/services/[A-Z0-9/]+` (line 44) | Slack |

If the credential's shape isn't covered, **add a rule** in the postmortem PR, then re-run `gitleaks detect --source . --config .gitleaks.toml` to confirm it fires.

Verify `secret_scan.yml` actually ran on the offending commit:
```bash
gh run list --workflow=secret_scan.yml --branch=<branch> --limit 5
gh run view <run-id>
```
If not (skipped hook, bypassed protection), action item: tighten branch protection to require the workflow.

### Step 6: Write the postmortem

Create `postmortems/YYYY-MM-DD_<short-cause>.md` with: **Timeline** (first leak → detection → rotation → audit), **What was exposed** (type, env, window), **Blast radius**, **Root cause**, **Detection gap** (which gate(s) didn't fire, and why), **Action items** with owners + due dates.

Reference the originating incident — `postmortems/2026-05-09_whoop_secret_in_git.md` — which motivated the whole Secrets Manager design (`infrastructure/modules/secrets/main.tf:6-9` calls this out).

## Verification (how you know it's fixed)

1. **New secret reads correctly:** `aws secretsmanager get-secret-value --secret-id "pulsetrack/${PT_AWS_ENV}/<service>" --query SecretString --output text | jq 'keys'`
2. **Old credential is dead** (test against the service):
   - WHOOP: `curl -H "Authorization: Bearer <old>" https://api.prod.whoop.com/developer/v2/cycle` → 401
   - Snowflake: `snowsql -u pulsetrack_loader -P <old>` → auth fail
   - Anthropic: `curl -H "x-api-key: <old>" https://api.anthropic.com/v1/messages -d '{}'` → 401
3. **Producer picks up new credential:** `prefect deployment run "whoop-poll/whoop-poll"` succeeds; bronze receives new rows.
4. **CI gate clean:** `gitleaks detect --source . --config .gitleaks.toml -v` and `trufflehog git file://. --only-verified --fail` both exit 0.
5. **Audit shows no anomalous use** — or, if it does, escalate to SEV1 and expand the postmortem.

## Prevention (post-incident hardening)

1. **Tighten the gate that missed this.** Add a new gitleaks rule or document why the existing rule didn't fire (regex too narrow, allowlist too generous, file excluded).
2. **Pre-commit adoption.** Every developer must have `gitleaks` installed locally (`pre-commit install` after cloning). Add to onboarding checklist.
3. **No-secret-in-env discipline.** Anything Secrets-Manager-resolvable should not be in `.env`. The `pt_secrets` tier-3 fallback (`pt_secrets/manager.py:204-232`) is dev-only convenience; prod uses tier 1 (AWS).
4. **KMS-bound logging.** Confirm CloudTrail data events stay on for the secrets CMK (`infrastructure/modules/secrets/main.tf`) — this is what made our last incident traceable in 5 min instead of 5h.
5. **Rotate everything on the same identity.** If a developer's IAM key leaks, also rotate their Snowflake password — one compromise implies the other should be assumed compromised.

## Related postmortems

- `postmortems/2026-05-09_whoop_secret_in_git.md` — the originating incident; the entire Secrets Manager + gitleaks + trufflehog stack exists because of this

## Related runbooks

- `runbooks/whoop_oauth_renewal.md` — the rotation path for WHOOP specifically, also used after a WHOOP credential leak
- `runbooks/prefect_flow_stuck.md` — if a flow gets stuck immediately after a rotation, it's typically the 15-min `pt_secrets` cache holding the dead credential; force-restart the worker
