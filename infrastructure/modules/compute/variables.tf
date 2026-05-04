variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "release_label" {
  description = "EMR release label"
  type        = string
}

variable "master_instance_type" {
  description = "EC2 instance type for EMR master"
  type        = string
}

variable "core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
}

variable "core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
}

variable "core_spot_bid_price" {
  description = "Spot bid price for core nodes (USD/hour). Empty string disables spot."
  type        = string
}

variable "ebs_volume_size_gb" {
  description = "EBS volume size per core node (GB)"
  type        = number
}

variable "idle_timeout_seconds" {
  description = "Auto-terminate after this many idle seconds"
  type        = number
}

variable "subnet_id" {
  description = "Subnet ID for EMR (must be in supported AZ)"
  type        = string
}

variable "emr_security_group_id" {
  description = "Security group for EMR master/core (managed by EMR)"
  type        = string
}

variable "service_security_group_id" {
  description = "Security group for EMR service access"
  type        = string
}

variable "key_pair_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
}

variable "service_role_arn" {
  description = "EMR service role ARN"
  type        = string
}

variable "instance_profile_arn" {
  description = "EMR EC2 instance profile ARN"
  type        = string
}

variable "bootstrap_bucket" {
  description = "S3 bucket containing bootstrap.sh"
  type        = string
}

variable "bootstrap_object_key" {
  description = "S3 object key for bootstrap.sh inside bootstrap_bucket"
  type        = string
}

variable "lakehouse_bucket" {
  description = "S3 bucket name for lakehouse warehouse path"
  type        = string
}
