output "cluster_arn" {
  value = aws_msk_serverless_cluster.pulsetrack.arn
}

output "cluster_name" {
  value = aws_msk_serverless_cluster.pulsetrack.cluster_name
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap brokers (SASL/IAM) for Kafka clients"
  value       = aws_msk_serverless_cluster.pulsetrack.bootstrap_brokers_sasl_iam
}
