#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# WHOOP OAuth bootstrap.
#
# Problem this solves:
#   The WHOOP OAuth flow requires a browser. EMR master nodes don't have
#   one. Pre-Secrets-Manager, the workflow was: run OAuth on laptop,
#   manually scp ``~/.whoop_tokens.json`` to EMR. That's brittle.
#
# This script:
#   1. Runs the interactive OAuth flow locally (opens a browser tab)
#   2. Persists tokens to ``~/.whoop_tokens.json`` (legacy path)
#   3. Uploads the same tokens to AWS Secrets Manager (pulsetrack/<env>/whoop-tokens)
#   4. Producers on EMR fetch from Secrets Manager via pt_secrets.get_secret_field
#
# The producer's refresh-token rotation writes new tokens back to Secrets Manager
# automatically — so this script only needs to run once every ~25 days (before
# the refresh token expires).
#
# Usage:
#   AWS_PROFILE=pulsetrack ./scripts/whoop_auth_bootstrap.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Verify dependencies up front
command -v python3 >/dev/null || { echo "FATAL: python3 not on PATH"; exit 1; }
command -v aws >/dev/null || { echo "FATAL: aws CLI not installed"; exit 1; }

ENV="${PT_AWS_ENV:-dev}"
SECRET_ID="pulsetrack/${ENV}/whoop-tokens"

echo "▶ Step 1: Running interactive WHOOP OAuth flow (a browser tab will open)"
echo "  Click 'Allow' on the WHOOP authorization page. The flow auto-closes."
echo ""

# This module writes ~/.whoop_tokens.json on success.
python3 -m data_generators.whoop_api.auth

TOKEN_PATH="$HOME/.whoop_tokens.json"
if [[ ! -f "$TOKEN_PATH" ]]; then
    echo "FATAL: $TOKEN_PATH not created. OAuth flow must have failed."
    exit 1
fi

echo ""
echo "▶ Step 2: Uploading tokens to AWS Secrets Manager (${SECRET_ID})"

# Read tokens, build the JSON payload the secret expects.
PAYLOAD=$(python3 -c "
import json, sys
with open('$TOKEN_PATH') as f:
    toks = json.load(f)
payload = {
    'access_token': str(toks.get('access_token', '')),
    'refresh_token': str(toks.get('refresh_token', '')),
    'expires_at': str(toks.get('expires_at', '')),
}
print(json.dumps(payload, separators=(',', ':')))
")

# Idempotent: put-secret-value works whether the secret has a prior value or not.
# If the secret doesn't exist at all, terraform apply must have been skipped.
if ! aws secretsmanager describe-secret --secret-id "$SECRET_ID" >/dev/null 2>&1; then
    echo "FATAL: Secret '$SECRET_ID' does not exist."
    echo "       Run: cd infrastructure && terraform apply"
    exit 1
fi

aws secretsmanager put-secret-value \
    --secret-id "$SECRET_ID" \
    --secret-string "$PAYLOAD" \
    >/dev/null

echo ""
echo "▶ Step 3: Roundtrip check"
RETRIEVED=$(aws secretsmanager get-secret-value \
    --secret-id "$SECRET_ID" \
    --query SecretString --output text)

if [[ -z "$RETRIEVED" ]]; then
    echo "FATAL: Read-back returned empty value"
    exit 1
fi

# Compare access_token field (first 8 chars only — never log full token)
LOCAL_PREFIX=$(python3 -c "import json; print(json.load(open('$TOKEN_PATH'))['access_token'][:8])")
REMOTE_PREFIX=$(python3 -c "import json,sys; print(json.loads('''$RETRIEVED''')['access_token'][:8])")

if [[ "$LOCAL_PREFIX" != "$REMOTE_PREFIX" ]]; then
    echo "FATAL: Roundtrip mismatch — local access_token prefix '$LOCAL_PREFIX' "
    echo "       differs from remote '$REMOTE_PREFIX'"
    exit 1
fi

echo "✓ Roundtrip OK (access_token prefix matches: ${LOCAL_PREFIX}…)"
echo ""
echo "Done. Producers on EMR will fetch from $SECRET_ID."
echo "Refresh-token rotation runs automatically (every ~24h on token refresh)."
echo "Next manual run: ~25 days from now (refresh token TTL)."
