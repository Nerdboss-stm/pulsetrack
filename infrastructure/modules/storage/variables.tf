variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "suffix" {
  description = "Random suffix for global S3 bucket uniqueness"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}
