output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.pulsetrack.dashboard_name
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.emr.name
}
