# PulseTrack Secret Rotation

The credential-rotation playbook. Per-secret cadence, step-by-step rotation, audit procedure, and a break-glass path for when Secrets Manager itself is unavailable.

**Audience:** the DE rotating a credential (scheduled or on-suspicion); the eng lead doing the monthly access-audit; anyone tracking down a "who fetched this secret?" question.

---

## 1. What's rotated

Everything PulseTrack touches that has a credential. Stored in AWS Secrets Manager under `pulsetrack/<env>/<service>`, KMS-encrypted with the customer-managed CMK `alias/pulsetrack-{env}-secrets`. See `infrastructure/modules/secrets/main.tf` for the TF source.

| Secret | Cadence | Owner | Rotation method |
|---|---|---|---|
| `pulsetrack/<env>/snowflake` (password) | 90 days | DE rotation | Manual; rotation Lambda is Phase-2 |
| `pulsetrack/<env>/whoop-tokens` (refresh_token) | 25-30 days | Producer process | **Auto-rotated** — producer writes-back on each token-refresh API call |
| `pulsetrack/<env>/whoop` (client_secret) | 1 year, or on suspected leak | DE rotation | Manual via WHOOP developer dashboard |
| `pulsetrack/<env>/anthropic` (api_key) | 90 days | DE rotation | Manual via Anthropic console |
| `pulsetrack/<env>/slack` (webhook_url) | On suspicion only | DE rotation | Manual via Slack admin |
| `pulsetrack/<env>/pagerduty` (routing_key) | Annually (when un-mocked) | DE rotation | Manual via PagerDuty admin |
| AWS IAM access keys | **Never** | n/a | We use EMR instance profiles — no long-lived IAM keys exist by policy |
| KMS keys | 1 year (auto) | n/a | AWS-managed auto-rotation enabled on the secrets CMK |

**Why these cadences:**

- **Snowflake 90 days** — matches Snowflake's recommended cadence. Password rotation breaks any running query, so we batch + announce.
- **WHOOP refresh-token 25-30 days** — WHOOP issues 30-day refresh tokens; our producer refreshes ~daily as a safety margin. The 25-day floor accounts for our 5-day grace period before WHOOP would reject it.
- **WHOOP client_secret 1 year** — long-lived OAuth app credential. No reason to rotate more often unless leaked (and one *was* leaked — see `postmortems/2026-05-09_whoop_secret_in_git.md`; rotated immediately on detection).
- **Anthropic 90 days** — standard API-key cadence; Anthropic doesn't auto-expire.
- **Slack on-suspicion** — webhook URLs are scoped to a single channel; low-risk if leaked (someone could spam our alerts channel, not exfiltrate data).
- **PagerDuty annually** — currently mocked; cadence applies post-Phase-2.
- **KMS auto** — AWS handles rotation transparently; we don't see the new key material.

---

## 2. Pre-rotation checklist

Before rotating anything (especially Snowflake password, which has highest blast radius):

- [ ] **Announce in `#pulsetrack-eng`** 24h ahead for scheduled rotations; immediately for emergency rotations
- [ ] **Identify all consumers** of the secret:
  ```bash
  rg "get_secret\(['\"](snowflake|whoop|anthropic|slack|pagerduty)['\"]\)" .
  rg "pulsetrack/.*/snowflake" infrastructure/ scripts/
  ```
- [ ] **Verify Secrets Manager is healthy:**
  ```bash
  aws secretsmanager describe-secret --secret-id pulsetrack/dev/snowflake
  # Should return a JSON document with no errors
  ```
- [ ] **Confirm the in-memory cache TTL** on `pt_secrets/manager.py:_default` — it's 15 min, so rotations propagate within 15 min OR you call `secrets.invalidate("<short_name>")` to force-refresh immediately
- [ ] **Have a rollback plan.** For Snowflake: keep the old password until the new one is verified working. For WHOOP: keep both client_secrets in WHOOP's "active" state until rotation completes.

---

## 3. Per-credential rotation procedure

### 3.1 Snowflake password (90 days, manual)

```bash
# 1. Generate a new strong password (locally — never share):
NEW_PW=$(python3 -c 'import secrets, string; print("".join(secrets.choice(string.ascii_letters + string.digits + "!@#$%^&*") for _ in range(40)))')

# 2. Set it in Snowflake (rotation user, not the same user!):
# Run this in the Snowflake worksheet — the connection-creating user differs
# from the connection-using user.
snowsql -a $ACCOUNT -u $ADMIN_USER -q "
    ALTER USER pulsetrack_loader SET PASSWORD = '$NEW_PW'
"

# 3. Update Secrets Manager:
aws secretsmanager get-secret-value --secret-id pulsetrack/dev/snowflake \
    --query SecretString --output text \
    | jq --arg pw "$NEW_PW" '.password = $pw' \
    | aws secretsmanager put-secret-value --secret-id pulsetrack/dev/snowflake \
        --secret-string file:///dev/stdin

# 4. Invalidate the in-memory cache (so running processes pick up the new password):
python3 -c "from pt_secrets.manager import _default; _default.invalidate('snowflake'); print('Cache invalidated')"

# 5. Verify a fresh connection works:
python3 scripts/check_credentials.py --service snowflake
# Should print snowflake.connection PASS

# 6. Test downstream consumers (Snowflake views in the BI dashboard):
python3 -c "from snowflake.test_views import smoke_test; smoke_test()"
# Or click around in the Snowflake worksheet for 5 min — does refresh work?

# 7. Document in Linear:
# - Rotation date
# - New password expiry date (today + 90 days)
# - Linear ticket for next scheduled rotation
```

**Rollback:** if the new password is rejected:
```bash
# Restore the previous version (Secrets Manager retains the last version)
aws secretsmanager describe-secret --secret-id pulsetrack/dev/snowflake \
    --query 'VersionIdsToStages'
# Note the AWSPREVIOUS version-id
aws secretsmanager update-secret-version-stage \
    --secret-id pulsetrack/dev/snowflake \
    --version-stage AWSCURRENT \
    --move-to-version-id <previous-version-id> \
    --remove-from-version-id <current-version-id>
```

### 3.2 WHOOP refresh-token (auto-rotated by producer)

**You don't do this manually unless the producer is broken.** The producer (running `data_generators/whoop_producer.py`) calls WHOOP's token endpoint roughly daily, receives a new refresh_token, and writes it back via:

```python
# pseudo-code in the producer
new_tokens = whoop_refresh_token(current_refresh_token)
# tokens = {"access_token": "...", "refresh_token": "...", "expires_at": "..."}
secrets_client.put_secret_value(
    SecretId="pulsetrack/dev/whoop-tokens",
    SecretString=json.dumps(new_tokens),
)
```

This is why the EMR EC2 role has scoped `PutSecretValue` permission ONLY on `whoop-tokens` (`infrastructure/modules/secrets/main.tf:UpdateWhoopTokensOnly`). A compromised producer can't rewrite other secrets.

**Manual recovery** (if the producer hasn't run in > 25 days and the refresh-token has expired):
```bash
# Re-run interactive OAuth on laptop, then push the new tokens up:
bash scripts/whoop_auth_bootstrap.sh

# This writes ~/.whoop_tokens.json; then push to Secrets Manager:
python3 scripts/bootstrap_secrets.py --include whoop-tokens
```

### 3.3 WHOOP client_secret (1 year or on suspicion)

```bash
# 1. WHOOP developer dashboard: https://developer.whoop.com/
#    → app → Settings → "Generate new client secret"
#    → copy the new secret (only shown once)

NEW_SECRET="<paste>"

# 2. Update Secrets Manager:
aws secretsmanager get-secret-value --secret-id pulsetrack/dev/whoop \
    --query SecretString --output text \
    | jq --arg s "$NEW_SECRET" '.client_secret = $s' \
    | aws secretsmanager put-secret-value --secret-id pulsetrack/dev/whoop \
        --secret-string file:///dev/stdin

# 3. Invalidate cache:
python3 -c "from pt_secrets.manager import _default; _default.invalidate('whoop')"

# 4. Verify the WHOOP poll works (uses the new secret to refresh tokens):
python3 -m data_generators.whoop_producer --once
# Should fetch + emit ≥ 1 event without 401

# 5. Revoke the OLD client_secret in the WHOOP dashboard
# (WHOOP keeps both active during a rotation window — go remove the old one)

# 6. Document the rotation date + reason in Linear
```

### 3.4 Anthropic API key (90 days)

```bash
# 1. Anthropic console: https://console.anthropic.com/settings/keys
#    → Create new key → name it "pulsetrack-prod-$(date +%Y%m%d)"
#    → copy the key (only shown once)

NEW_KEY="<paste>"

# 2. Update Secrets Manager:
echo "{\"api_key\": \"$NEW_KEY\"}" \
    | aws secretsmanager put-secret-value --secret-id pulsetrack/dev/anthropic \
        --secret-string file:///dev/stdin

# 3. Invalidate cache:
python3 -c "from pt_secrets.manager import _default; _default.invalidate('anthropic')"

# 4. Verify:
python3 -c "from ai.anomaly_explainer import ping; ping()"
# Should print "Anthropic ping OK"

# 5. Revoke the old key in the Anthropic console (delete the previous key by name)
```

### 3.5 Slack webhook (on suspicion)

```bash
# 1. Slack admin: https://api.slack.com/apps → your app → "Incoming Webhooks"
#    → Revoke old webhook
#    → Create new webhook for the same channel

NEW_URL="<paste>"

# 2. Update Secrets Manager:
echo "{\"webhook_url\": \"$NEW_URL\"}" \
    | aws secretsmanager put-secret-value --secret-id pulsetrack/dev/slack \
        --secret-string file:///dev/stdin

# 3. Test:
python3 -c "from observability.alerting import notify_slack; notify_slack('test rotation OK')"
# Should arrive in the channel
```

---

## 4. Audit

Monthly review by the on-call DE (or anyone curious). What we look for: unexpected callers, off-hour reads, or volume anomalies.

### 4.1 CloudTrail filter — who's reading secrets?

CloudTrail logs `GetSecretValue` as a data event. The Secrets Manager module enables data-event logging on the customer-managed KMS key.

```bash
# Last 7 days, all GetSecretValue calls
aws cloudtrail lookup-events \
    --lookup-attributes AttributeKey=EventName,AttributeValue=GetSecretValue \
    --start-time "$(date -u -v-7d +%Y-%m-%dT%H:%M:%SZ)" \
    --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --max-results 500 \
    | jq -r '.Events[] | [.EventTime, .Username, .CloudTrailEvent | fromjson | .requestParameters.secretId] | @csv'
```

### 4.2 What "normal" looks like

| Principal | Expected secrets fetched | Frequency |
|---|---|---|
| `EMR_EC2_DefaultRole-pulsetrack-dev` (the EMR instance profile) | whoop, whoop-tokens, anthropic, snowflake, slack, pagerduty | Multiple times per hour (in-memory cache every 15 min) |
| Your IAM user (admin) | All secrets | Only during rotations or debugging |
| `pulsetrack-ci-runner` (if set up) | None — CI doesn't read prod secrets | n/a |

Red flags:

- A principal you don't recognize
- Off-hours reads on a weekday when nobody's actively debugging
- Volume spike (10× the baseline calls per hour) — usually means a service is in a retry loop with no caching
- Read of a secret by a service that shouldn't need it (e.g., `dim_date.py` reading Anthropic)

### 4.3 Monthly procedure

First Monday of each month:

```bash
# Pull 30 days of GetSecretValue calls into a CSV
aws cloudtrail lookup-events ... --start-time "$(date -u -v-30d +%Y-%m-%dT%H:%M:%SZ)" \
    > /tmp/secrets_audit.json

# Aggregate by principal × secret
jq -r '.Events[] | [.Username, (.CloudTrailEvent | fromjson | .requestParameters.secretId)] | @csv' /tmp/secrets_audit.json \
    | sort | uniq -c | sort -rn
```

Eyeball the top 10. Anything unexpected → open a Linear ticket and investigate. File the output in `docs/audit/secrets-YYYY-MM.csv` for the record.

---

## 5. Break-glass — when Secrets Manager itself is down

Rare (AWS-side incident or KMS key issue) but the procedure must exist.

### 5.1 Symptom

- `pt_secrets/manager.py` falls through tier-1 (AWS), tier-2 (env), tier-3 (.env) and raises `SecretsError`
- AWS console: Secrets Manager service health page shows degradation
- CloudWatch alarm `pulsetrack-{env}-secrets-fetch-failure-rate` is firing

### 5.2 Break-glass procedure

1. **Verify the outage is real.** Try fetching from another AWS service in the same region; if all of us-east-1 is down, this is § 3.4 of `disaster_recovery.md` (region outage) not just secrets.

2. **Use the temporary console-stored credential.** Each DE has a personal `~/.pulsetrack-breakglass/` directory (gitignored) with copies of the critical secrets retrieved from Secrets Manager at last sync. These have a 1h TTL after Secrets Manager comes back — they auto-clear.

   **TODO: this directory + sync script doesn't exist yet.** Phase-2 task:
   ```bash
   # Future: scripts/breakglass_sync.sh
   # Fetches all secrets to ~/.pulsetrack-breakglass/ with 1h-from-now timestamp
   # Files: ~/.pulsetrack-breakglass/snowflake.json etc.
   # The pt_secrets manager would read these as a tier-0 fallback during
   # known-outage windows.
   ```

3. **Manually inject into env vars.** The tier-2 fallback in `pt_secrets/manager.py:_resolve_env` reads `PT_<SERVICE>_<FIELD>` env vars. Set them for the running process:
   ```bash
   export PT_SNOWFLAKE_USER="pulsetrack_loader"
   export PT_SNOWFLAKE_PASSWORD="<the-current-password>"
   export PT_SNOWFLAKE_ACCOUNT="<account>"
   # ... and others as needed
   ```

4. **Document the break-glass event** in `postmortems/YYYY-MM-DD_secrets_manager_outage.md`. Note: every env-var-exposed secret should be cleared from your shell after the outage resolves (`unset PT_SNOWFLAKE_PASSWORD`).

5. **Post-outage:** rotate the credentials that were temporarily exposed in env vars (the threat model assumes shell history could leak them). Follow § 3.

### 5.3 What we never do

- **Never** commit a secret to a config file during a break-glass. Even temporarily.
- **Never** Slack-DM a credential to a teammate. Use the AWS console once it's back.
- **Never** disable the AWS-side encryption to "speed up" recovery. The KMS-encrypted cipher text in Secrets Manager is fine; the API was just unavailable.

---

## 6. References

- `pt_secrets/manager.py` — the 3-tier resolver
- `scripts/bootstrap_secrets.py` — populate Secrets Manager from `.env` (one-time + on rotation)
- `scripts/whoop_auth_bootstrap.sh` — interactive WHOOP OAuth flow (writes `~/.whoop_tokens.json`)
- `infrastructure/modules/secrets/main.tf` — KMS + Secrets Manager TF
- `scripts/check_credentials.py` — pre-flight sanity for all secrets
- `postmortems/2026-05-09_whoop_secret_in_git.md` — the real-world reason this module exists
- `docs/on_call.md` § 6 — postmortem template
- `docs/disaster_recovery.md` § 3.3 — account-compromise procedure (all secrets rotate)
