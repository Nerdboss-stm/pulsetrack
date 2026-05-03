variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs (MSK Serverless requires at least 2 across distinct AZs)"
  type        = list(string)
}

variable "kafka_security_group_id" {
  description = "Security group for MSK clients"
  type        = string
}
