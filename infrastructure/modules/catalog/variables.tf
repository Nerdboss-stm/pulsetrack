variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "lakehouse_bucket" {
  description = "S3 bucket name for lakehouse"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN that Glue crawlers assume"
  type        = string
}
