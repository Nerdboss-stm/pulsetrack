output "bucket_name" {
  value = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}

output "bucket_regional_domain_name" {
  value = aws_s3_bucket.lakehouse.bucket_regional_domain_name
}

output "bootstrap_object_key" {
  value = aws_s3_object.emr_bootstrap.key
}
