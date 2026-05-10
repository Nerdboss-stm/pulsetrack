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
