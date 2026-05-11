variable "name_prefix" {
  description = "Resource name prefix, e.g. ``pulsetrack-dev``. Used for the registry name and the IAM policy."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod). Stamped as a tag on every resource."
  type        = string
}

variable "emr_ec2_role_name" {
  description = <<-EOT
    Name of the EMR EC2 IAM role (NOT the instance profile name).
    Exported by the ``iam`` module as ``emr_ec2_role_name``.
    The ``glue-sr`` read+write policy is attached to this role so producers +
    the Spark streaming consumer can look up Glue Schema Registry schemas at runtime.
    Pass empty string to skip the attachment (e.g. in CI where no EMR exists).
  EOT
  type        = string
  default     = ""
}
