output "emr_service_role_arn" {
  value = aws_iam_role.emr_service.arn
}

output "emr_instance_profile_arn" {
  value = aws_iam_instance_profile.emr_ec2.arn
}

output "emr_instance_profile_name" {
  value = aws_iam_instance_profile.emr_ec2.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}
