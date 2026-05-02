variable "aws_region" {
  description = "AWS region to deploy PulseTrack infrastructure into"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be one of: dev, prod"
  }
}

variable "alert_email" {
  description = "Email address that receives budget and operational alerts"
  type        = string

  validation {
    condition     = can(regex("^[^@]+@[^@]+\\.[^@]+$", var.alert_email))
    error_message = "alert_email must be a valid email address"
  }
}

variable "key_pair_name" {
  description = "Existing EC2 key pair name for SSH access to EMR master"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the new VPC"
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.42.1.0/24", "10.42.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.42.11.0/24", "10.42.12.0/24"]
}

variable "emr_release_label" {
  description = "EMR release label. emr-7.2.0 ships Spark 3.5.1 and Iceberg 1.6.1"
  type        = string
  default     = "emr-7.2.0"
}

variable "emr_master_instance_type" {
  description = "EC2 instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 2
}

variable "emr_core_spot_bid_price" {
  description = "Spot bid price (USD/hour) for EMR core instances. Empty string disables spot."
  type        = string
  default     = "0.08"
}

variable "emr_ebs_volume_size_gb" {
  description = "EBS volume size (GB) per EMR core node"
  type        = number
  default     = 64
}

variable "emr_idle_timeout_seconds" {
  description = "Auto-terminate the cluster after this many idle seconds (60 min - 1 week range)"
  type        = number
  default     = 7200
}

variable "budget_limit_usd" {
  description = "Monthly AWS budget cap (USD). Alert fires at 80%."
  type        = string
  default     = "40"
}
