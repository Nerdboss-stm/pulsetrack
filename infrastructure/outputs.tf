output "lakehouse_bucket_name" {
  description = "S3 bucket name for the PulseTrack lakehouse"
  value       = module.storage.bucket_name
}

output "lakehouse_bucket_arn" {
  description = "S3 bucket ARN for the PulseTrack lakehouse"
  value       = module.storage.bucket_arn
}

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = module.compute.cluster_id
}

output "emr_master_dns" {
  description = "Public DNS of EMR master node (use for SSH and Spark UI)"
  value       = module.compute.master_public_dns
}

output "msk_bootstrap_brokers" {
  description = "MSK Serverless bootstrap brokers (SASL/IAM)"
  value       = module.kafka.bootstrap_brokers_sasl_iam
}

output "glue_bronze_db" {
  description = "Glue catalog database name for Bronze layer"
  value       = module.catalog.bronze_database_name
}

output "glue_silver_db" {
  description = "Glue catalog database name for Silver layer"
  value       = module.catalog.silver_database_name
}

output "glue_gold_db" {
  description = "Glue catalog database name for Gold layer"
  value       = module.catalog.gold_database_name
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "alert_topic_arn" {
  description = "SNS topic for ops alerts"
  value       = module.monitoring.alert_topic_arn
}

output "glacierbase_lock_table" {
  description = "DynamoDB table backing the Glacierbase migration concurrency lock"
  value       = module.iam.glacierbase_lock_table_name
}

output "secrets_kms_key_arn" {
  description = "Customer-managed KMS key encrypting all PulseTrack secrets"
  value       = module.secrets.kms_key_arn
}

output "secret_names" {
  description = "Map of secret short-name → full Secrets Manager name. Use these as the SecretId for boto3 calls."
  value       = module.secrets.secret_names
}

output "secret_arns" {
  description = "Map of secret short-name → ARN, for any module needing fine-grained scoping"
  value       = module.secrets.secret_arns
}

# ── Glue Schema Registry ──────────────────────────────────────────────────
output "glue_registry_name" {
  description = "AWS Glue Schema Registry name. Use as PT_GLUE_REGISTRY_NAME for cloud producers."
  value       = module.schema_registry.registry_name
}

output "glue_registry_arn" {
  description = "ARN of the Glue Schema Registry. CloudTrail audits target this resource."
  value       = module.schema_registry.registry_arn
}

output "glue_schema_names" {
  description = "Map of logical name → Glue schema name (sensor_reading, pharmacy_event)."
  value       = module.schema_registry.schema_names
}
