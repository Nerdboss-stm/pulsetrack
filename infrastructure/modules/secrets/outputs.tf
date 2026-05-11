output "kms_key_arn" {
  description = "ARN of the customer-managed KMS key used to encrypt secrets"
  value       = aws_kms_key.secrets.arn
}

output "kms_key_alias" {
  description = "Friendly alias for the KMS key"
  value       = aws_kms_alias.secrets.name
}

output "secret_arns" {
  description = "Map of secret short-name → ARN. Use this for IAM scoping in downstream modules."
  value       = { for k, s in aws_secretsmanager_secret.this : k => s.arn }
}

output "secret_names" {
  description = "Map of secret short-name → full Secrets Manager name (e.g. pulsetrack/dev/whoop)"
  value       = { for k, s in aws_secretsmanager_secret.this : k => s.name }
}

output "read_policy_arn" {
  description = "IAM policy ARN granting read access to all secrets + the WHOOP-token write-back. Attached to the EMR EC2 role by this module; export here so other modules (Prefect worker, Lambda rotators) can attach as well."
  value       = aws_iam_policy.secrets_read.arn
}
