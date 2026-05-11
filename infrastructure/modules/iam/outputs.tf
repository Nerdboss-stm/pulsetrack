output "emr_service_role_arn" {
  value = aws_iam_role.emr_service.arn
}

output "emr_instance_profile_arn" {
  value = aws_iam_instance_profile.emr_ec2.arn
}

output "emr_instance_profile_name" {
  value = aws_iam_instance_profile.emr_ec2.name
}

output "emr_ec2_role_name" {
  description = "Name of the EMR EC2 IAM role (NOT the instance profile). Needed by the secrets module to attach the secrets-read policy."
  value       = aws_iam_role.emr_ec2.name
}

output "emr_ec2_role_arn" {
  description = "ARN of the EMR EC2 IAM role"
  value       = aws_iam_role.emr_ec2.arn
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "glacierbase_lock_table_name" {
  description = "DynamoDB table backing the Glacierbase migration lock"
  value       = aws_dynamodb_table.glacierbase_lock.name
}

output "glacierbase_lock_table_arn" {
  value = aws_dynamodb_table.glacierbase_lock.arn
}
