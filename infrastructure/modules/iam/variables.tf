variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "lakehouse_bucket" {
  description = "S3 bucket ARN for the lakehouse (used in IAM policy resource scopes)"
  type        = string
}
