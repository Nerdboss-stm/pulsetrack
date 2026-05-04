resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"

  tags = {
    Name = "${var.name_prefix}-alerts"
  }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─── Budget alert: 80% of monthly cap ───────────────────────────────────────

resource "aws_budgets_budget" "pulsetrack" {
  name         = "${var.name_prefix}-budget"
  budget_type  = "COST"
  limit_amount = var.budget_limit_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name = "TagKeyValue"
    values = [
      "user:Project$PulseTrack"
    ]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# ─── CloudWatch dashboard ───────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "pulsetrack" {
  dashboard_name = "${var.name_prefix}-overview"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ElasticMapReduce", "AppsRunning", "JobFlowId", var.emr_cluster_id],
            [".", "AppsPending", ".", "."],
            [".", "AppsCompleted", ".", "."],
            [".", "AppsFailed", ".", "."]
          ]
          period = 300
          stat   = "Average"
          region = var.aws_region
          title  = "EMR Applications"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ElasticMapReduce", "ContainerAllocated", "JobFlowId", var.emr_cluster_id],
            [".", "ContainerPending", ".", "."],
            [".", "MemoryAllocatedMB", ".", "."],
            [".", "MemoryAvailableMB", ".", "."]
          ]
          period = 300
          stat   = "Average"
          region = var.aws_region
          title  = "EMR Cluster Capacity"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", var.lakehouse_bucket, "StorageType", "StandardStorage"],
            [".", "NumberOfObjects", ".", ".", ".", "AllStorageTypes"]
          ]
          period = 86400
          stat   = "Average"
          region = var.aws_region
          title  = "Lakehouse S3 size & object count"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ElasticMapReduce", "HDFSUtilization", "JobFlowId", var.emr_cluster_id],
            [".", "IsIdle", ".", "."]
          ]
          period = 300
          stat   = "Average"
          region = var.aws_region
          title  = "EMR Health"
        }
      }
    ]
  })
}

# ─── Alarms ─────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "emr_apps_failed" {
  alarm_name          = "${var.name_prefix}-emr-apps-failed"
  alarm_description   = "Any EMR application failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "AppsFailed"
  namespace           = "AWS/ElasticMapReduce"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobFlowId = var.emr_cluster_id
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr/${var.name_prefix}"
  retention_in_days = 14
}
