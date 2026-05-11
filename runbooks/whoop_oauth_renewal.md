# Runbook: WHOOP OAuth token renewal

**This is a scheduled-maintenance procedure, NOT an incident response.** It becomes an incident only if the rotation is missed and the producer starts 401-ing.

**Severity ladder (if missed):**
- SEV3: `expires_at` within 7 days, producer still functioning. Schedule rotation in next business window.
- SEV2: WHOOP producer logging `401 Unauthorized` or `invalid_grant` for > 30 min. WHOOP data is gapping in bronze; downstream `fact_vital_*` will show missing windows.
- SEV1: WHOOP producer 401-ing AND `whoop-poll` Prefect deployment also failing AND backfill window > 24h. Recovery requires backfill once auth is restored (see Case B).

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# 1. When does the refresh token expire?
aws secretsmanager get-secret-value \
    --secret-id "pulsetrack/${PT_AWS_ENV:-dev}/whoop-tokens" \
    --query SecretString --output text \
    | jq '{expires_at: .expires_at, expires_human: (.expires_at | tonumber | strftime("%Y-%m-%d %H:%M UTC"))}'

# 2. Is the producer 401-ing right now?
kubectl logs -l app=whoop-producer --since=15m | grep -i '401\|invalid_grant'
# or on EMR:
ssh hadoop@$MASTER_DNS "tail -200 /tmp/whoop-producer.log | grep -i '401\|invalid_grant'"

# 3. If rotation is needed, do it now (will open a browser tab on YOUR laptop):
AWS_PROFILE=pulsetrack PT_AWS_ENV=dev ./scripts/whoop_auth_bootstrap.sh
```

That script is the dedicated, idempotent tool — DO NOT manually edit Secrets Manager or hand-craft tokens.

## Why this runbook exists

WHOOP OAuth tokens have two layers of expiry:

| Token | TTL | Auto-rotated by |
|-------|-----|-----------------|
| **Access token** | ~1h | `data_generators/whoop_api/auth.py:_refresh_tokens` (called when `_is_expired()` is true; see `auth.py:204-215`). Writes back to `pulsetrack/<env>/whoop-tokens` via the EMR EC2 role's scoped `PutSecretValue` (`infrastructure/modules/secrets/main.tf:152-159`). Hands-off. |
| **Refresh token** | ~30 days | NOT auto-rotated. WHOOP's consent screen requires a browser; EMR master nodes have none. A developer laptop runs the interactive authorization-code flow every ~25 days. |

`scripts/whoop_auth_bootstrap.sh` is the dedicated tool:
1. Invokes `python3 -m data_generators.whoop_api.auth` — opens browser to `settings.whoop_oauth_authorize_url`, runs localhost listener at `settings.whoop_redirect_uri`, exchanges code for tokens, writes `~/.whoop_tokens.json` (chmod 600).
2. Uploads `{access_token, refresh_token, expires_at}` to `pulsetrack/<env>/whoop-tokens` via `aws secretsmanager put-secret-value`.
3. Roundtrip read-back, comparing the access-token prefix.

## Symptoms (what triggered the page)

- WHOOP producer log: `requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: .../developer/v2/cycle`
- WHOOP refresh: `{"error": "invalid_grant", "error_description": "Refresh token expired"}` from `auth.py:158`
- `whoop-poll` deployment runs failing in `run_whoop_poller`
- `notify_slack` from `whoop_poll_pipeline.py:62` posting `WHOOP poll failed. Detail: 401 Unauthorized`
- `pulsetrack_bronze_<env>.sensor_readings` has no new rows from `source='whoop'` in the last 30 min
- Calendar reminder for the 25-day rotation (the boring, on-purpose path)

## Diagnosis (commands to run first)

Check current expiry:
```bash
SECRET_ID="pulsetrack/${PT_AWS_ENV:-dev}/whoop-tokens"
aws secretsmanager get-secret-value --secret-id "$SECRET_ID" \
    --query SecretString --output text \
    | jq '{
        expires_at_human: (.expires_at | tonumber | strftime("%Y-%m-%d %H:%M UTC")),
        days_remaining: (((.expires_at | tonumber) - now) / 86400 | floor)
    }'
```

Interpretation:
- `> 5 days` → no action; access-token refresh will keep working
- `1-5 days` → schedule rotation in next business window; calendar reminder leaked through
- `<= 0` → refresh token expired; proceed to Case B

Confirm the producer's error:
```bash
# Prefect path (every 15 min):
prefect flow-run ls --deployment-name whoop-poll/whoop-poll --state Failed --limit 5
prefect flow-run inspect <run_id>
# Continuous EMR path (run_loop instead of poll):
ssh hadoop@$MASTER_DNS "tail -200 /tmp/whoop-producer.log"
```

Look for `401`, `invalid_grant`, or `WhoopAuthError` (from `auth.py:38` — refresh path itself failed).

Confirm cause is auth, not WHOOP outage:
```bash
curl -sS -o /dev/null -w "%{http_code}\n" https://api.prod.whoop.com/developer/v2/cycle
# 401 = our auth. 503/504 = WHOOP-side; wait, page if persistent.
```

## Recovery (ranked by likelihood, fastest first)

### Case A: Planned rotation (refresh token within 7 days of expiry)

Calendar-reminder path; producer still works; rotating preemptively.

```bash
# On your laptop (browser available; AWS_PROFILE has PutSecretValue on whoop-tokens):
cd /Users/nerdboss-stm/pulsetrack-cm
AWS_PROFILE=pulsetrack PT_AWS_ENV=dev ./scripts/whoop_auth_bootstrap.sh
```

The script opens a browser tab → click **Allow** → localhost callback at `http://localhost:8765/callback` receives the code → tokens written to `~/.whoop_tokens.json` and uploaded to `pulsetrack/dev/whoop-tokens` → roundtrip check. Expected last line: `Roundtrip OK (access_token prefix matches: <8chars>…)`.

Repeat per environment (`PT_AWS_ENV=staging`, then `prod`). Each env is a separate browser flow — distinct Secrets Manager entries even though the OAuth client is shared.

**No producer restart needed.** `get_access_token()` (`auth.py:197`) refreshes when `_is_expired()`; the `pt_secrets` cache has a 15-min TTL. New tokens pick up within 15 min, or force sooner:
```python
from pt_secrets.manager import _default
_default.invalidate("whoop-tokens")
```

### Case B: Producer 401-ing right now (refresh token already expired)

Same script, plus backfill:
```bash
# 1. Rotate (Case A).
cd /Users/nerdboss-stm/pulsetrack-cm
AWS_PROFILE=pulsetrack PT_AWS_ENV=dev ./scripts/whoop_auth_bootstrap.sh

# 2. Identify the gap from the per-endpoint offset file:
cat ~/.whoop_poll_offsets.json     # producer.py:46-63

# 3. Manual catch-up with widened lookback:
prefect deployment run "whoop-poll/whoop-poll" --param lookback_days=7 --param interval_minutes=60
```

The producer is idempotent on the endpoint-offset file, so re-fetching a partially-fetched window is safe — duplicates land in bronze and are MERGE'd at silver.

### Case C: Script fails at "Secret does not exist"

`FATAL: Secret 'pulsetrack/dev/whoop-tokens' does not exist.` — Terraform `secrets` module hasn't been applied:
```bash
cd /Users/nerdboss-stm/pulsetrack-cm/infrastructure
terraform workspace select dev
terraform apply -target=module.secrets
```
Then re-run `whoop_auth_bootstrap.sh`.

### Case D: Browser flow doesn't return (callback never received)

Script hangs after `Opening browser for WHOOP authorization`. Causes:
- **Port 8765 bound:** `lsof -i :8765` → kill the squatter
- **Corporate VPN intercepts the localhost callback:** disable VPN for the auth step
- **Redirect-URI mismatch:** the WHOOP developer console's registered URI doesn't match `settings.whoop_redirect_uri` (default `http://localhost:8765/callback`); fix in the WHOOP portal

The OAuth callback server times out after 5 min (`auth.py:106-117`).

## Verification (how you know it's fixed)

1. **Secret written:** the script's roundtrip check already passed.
2. **New expiry ~30 days out:**
   ```bash
   aws secretsmanager get-secret-value --secret-id pulsetrack/${PT_AWS_ENV:-dev}/whoop-tokens \
       --query SecretString --output text | jq '.expires_at | tonumber | strftime("%Y-%m-%d %H:%M UTC")'
   ```
3. **Producer recovers within 15 min** (one `pt_secrets` cache TTL):
   ```bash
   prefect deployment run "whoop-poll/whoop-poll"
   prefect flow-run ls --deployment-name whoop-poll/whoop-poll --limit 1   # status='ok'
   ```
4. **Bronze WHOOP rows flowing again** — Athena: `SELECT MAX(event_timestamp) FROM pulsetrack_bronze_dev.sensor_readings WHERE source = 'whoop'` returns within the last 15 min.
5. **No `notify_slack warn` posts** referencing WHOOP for the next hour.

## Prevention (the scheduled-rotation discipline)

1. **Calendar reminder every 25 days.** Recurring event: "PulseTrack WHOOP token rotation — `scripts/whoop_auth_bootstrap.sh` for dev/staging/prod". 5-day buffer before the 30-day expiry. Owner: data-platform on-call.
2. **Pre-expiry warning flow.** Add to `orchestration/flows/` a small flow that reads `expires_at` daily and posts at T-7 days:
   ```python
   @flow(name="whoop-token-expiry-warn")
   def whoop_token_expiry_warn(warn_days: int = 7):
       secret = get_secret("whoop-tokens")
       days_left = (int(secret["expires_at"]) - time.time()) / 86400
       if days_left < warn_days:
           notify_slack.fn(
               message=f"WHOOP refresh token expires in {days_left:.1f} days. "
                       f"Run scripts/whoop_auth_bootstrap.sh.",
               severity="warn",
           )
   ```
   Register in `orchestration/deployments.py` with `cron="0 14 * * *"`.
3. **Rotation ack.** After Case A, post in `#data-platform-ops`: "WHOOP tokens rotated; new expiry YYYY-MM-DD; next rotation due YYYY-MM-DD." (audit trail Secrets Manager doesn't surface)
4. **Multi-env wrapper.** Add `scripts/rotate_whoop_all_envs.sh` looping dev/staging/prod so one calendar event covers everything.

## Related postmortems

- `postmortems/2026-05-09_whoop_secret_in_git.md` — the originating incident where the `.env`-committed-to-history exposure forced this whole AWS Secrets Manager design

## Related runbooks

- `runbooks/secret_leak_response.md` — if the leaked token IS the WHOOP refresh token (rotate via this runbook then audit)
- `runbooks/prefect_flow_stuck.md` — `whoop-poll` deployment failures often present as "stuck flow" before the underlying 401 is identified
