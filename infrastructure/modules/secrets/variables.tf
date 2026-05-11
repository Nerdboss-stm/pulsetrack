variable "name_prefix" {
  description = "Resource name prefix, e.g. ``pulsetrack-dev``"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "emr_ec2_role_name" {
  description = <<-EOT
    Name of the EMR EC2 IAM role (NOT the instance profile name).
    Exported by the ``iam`` module as ``emr_ec2_role_name``.
    We attach the ``secrets-read`` policy to this role so producers /
    Spark jobs running on EMR can call ``GetSecretValue``.
  EOT
  type        = string
}
