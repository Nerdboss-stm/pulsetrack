# ─────────────────────────────────────────────────────────────────────────────
# Secrets module — AWS Secrets Manager + KMS for PulseTrack credentials.
#
# Why this exists:
#   Until now every credential lived in a local `.env` file (gitignored —
#   but the `.env` file with WHOOP creds was committed to history in an
#   earlier commit; see postmortems/2026-05-09_whoop_secret_in_git.md).
#
#   This module is the production-grade replacement. Every secret lives
#   in AWS Secrets Manager, encrypted by a customer-managed KMS key
#   (vs. AWS-managed: gives us audit + rotation control), with
#   GetSecretValue logged via CloudTrail data events.
#
#   The EMR EC2 role gets read access only — no Get/Put/Delete on the
#   KMS key itself, so a compromised EMR node can read secrets but can't
#   exfiltrate the encryption key.
#
# Inputs:
#   - name_prefix:        ``pulsetrack-{environment}``
#   - environment:        dev / staging / prod
#   - emr_ec2_role_name:  name of the existing EMR EC2 IAM role (so we
#                         can attach the read-policy without circular
#                         dependency with the iam module)
#
# Outputs:
#   - secret ARNs for each secret (for downstream apps)
#   - kms key ARN
#
# Reference: AWS Secrets Manager best-practice doc + WHOOP-public-blog
# pattern (their data platform uses Secrets Manager for the same reasons).
# ─────────────────────────────────────────────────────────────────────────────

# ── KMS key for secret envelope encryption ────────────────────────────────
resource "aws_kms_key" "secrets" {
  description             = "PulseTrack ${var.environment} — Secrets Manager envelope encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true # 1-year auto-rotation per AWS default

  # Restrict key admin to account root; service principals (Secrets Manager,
  # CloudTrail) get usage rights via the key policy below.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableRootAccountAdmin"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowSecretsManagerService"
        Effect    = "Allow"
        Principal = { Service = "secretsmanager.amazonaws.com" }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey",
        ]
        Resource = "*"
      },
    ]
  })

  tags = {
    Name = "${var.name_prefix}-secrets"
  }
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/${var.name_prefix}-secrets"
  target_key_id = aws_kms_key.secrets.key_id
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ── Secrets ───────────────────────────────────────────────────────────────
# We create empty secrets — values are populated via:
#   $ python scripts/bootstrap_secrets.py
# (or manually in the Secrets Manager console for the first-time setup).
#
# Force-destroy is false in prod environments — accidental TF destroy
# shouldn't wipe live credentials. Recovery window is 30 days.

locals {
  secrets = {
    whoop = {
      description = "WHOOP OAuth client credentials + user identity"
      json_schema = "client_id, client_secret, redirect_uri, account_id, user_email"
    }
    whoop-tokens = {
      description = "WHOOP OAuth access + refresh tokens (rotated by producer on refresh)"
      json_schema = "access_token, refresh_token, expires_at"
    }
    anthropic = {
      description = "Anthropic Claude API key (used by ai/*)"
      json_schema = "api_key"
    }
    snowflake = {
      description = "Snowflake connection bundle"
      json_schema = "account, user, password, role, warehouse, database, schema"
    }
    slack = {
      description = "Slack webhook URL for observability alerts"
      json_schema = "webhook_url"
    }
    pagerduty = {
      description = "PagerDuty Events API v2 routing key (currently mocked — see runbook)"
      json_schema = "routing_key"
    }
  }
}

resource "aws_secretsmanager_secret" "this" {
  for_each = local.secrets

  name        = "pulsetrack/${var.environment}/${each.key}"
  description = "${each.value.description}. JSON schema: ${each.value.json_schema}"
  kms_key_id  = aws_kms_key.secrets.arn

  # Recovery window: 30 days in prod, 7 in dev. Stops "oops terraform destroy"
  # from being instantly catastrophic.
  recovery_window_in_days = var.environment == "prod" ? 30 : 7

  # Forbid replication for now — single-region by default. Multi-region
  # replication is a Phase 2 concern (DR cross-region).

  tags = {
    Service  = each.key
    Rotation = "manual" # rotation Lambdas are a Phase-2 enhancement
  }
}

# ── IAM policy: read-only on the secrets + decrypt-only on the KMS key ────
data "aws_iam_policy_document" "secrets_read" {
  # Read secret values
  statement {
    sid    = "ReadSecretValues"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = [for s in aws_secretsmanager_secret.this : s.arn]
  }

  # Update secret values — needed for WHOOP refresh-token write-back.
  # Scoped to the whoop-tokens secret ONLY, so a compromised producer
  # can't rewrite other secrets.
  statement {
    sid    = "UpdateWhoopTokensOnly"
    effect = "Allow"
    actions = [
      "secretsmanager:PutSecretValue",
      "secretsmanager:UpdateSecret",
    ]
    resources = [aws_secretsmanager_secret.this["whoop-tokens"].arn]
  }

  # Decrypt with the customer-managed KMS key (necessary because the
  # secret is encrypted with it). No kms:Encrypt — Secrets Manager
  # service principal does that on write-back via its own service policy.
  statement {
    sid    = "DecryptWithSecretsKey"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]
    resources = [aws_kms_key.secrets.arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "secrets_read" {
  name        = "${var.name_prefix}-secrets-read"
  description = "Read PulseTrack secrets + decrypt with the secrets-KMS key"
  policy      = data.aws_iam_policy_document.secrets_read.json
}

# ── Attachment to EMR EC2 role ────────────────────────────────────────────
# Decoupled from the iam module: the iam module exposes the role name via
# the new ``emr_ec2_role_name`` output, and this module attaches the
# policy. No circular dependency.
resource "aws_iam_role_policy_attachment" "emr_secrets_read" {
  role       = var.emr_ec2_role_name
  policy_arn = aws_iam_policy.secrets_read.arn
}
