variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "aws_region" {
  description = "AWS region (used in dashboard widgets)"
  type        = string
}

variable "alert_email" {
  description = "Email for budget and alarm notifications"
  type        = string
}

variable "budget_limit_usd" {
  description = "Monthly AWS budget cap in USD"
  type        = string
}

variable "emr_cluster_id" {
  description = "EMR cluster ID for dashboard widgets and alarms"
  type        = string
}

variable "lakehouse_bucket" {
  description = "S3 bucket name for dashboard widgets"
  type        = string
}

variable "msk_cluster_arn" {
  description = "MSK Serverless cluster ARN for dashboard widgets"
  type        = string
  default     = ""
}
