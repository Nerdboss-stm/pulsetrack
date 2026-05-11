output "registry_name" {
  description = "Glue Schema Registry name (use as ``registry_name`` arg to schemas/glue_registry.py:register_or_get_schema)."
  value       = aws_glue_registry.this.registry_name
}

output "registry_arn" {
  description = "ARN of the Glue Schema Registry."
  value       = aws_glue_registry.this.arn
}

output "sensor_reading_schema_arn" {
  description = "ARN of the sensor_reading schema. The schema-version-id (UUID) is fetched at runtime by the Python client and is NOT a terraform output."
  value       = aws_glue_schema.sensor_reading.arn
}

output "pharmacy_event_schema_arn" {
  description = "ARN of the pharmacy_event schema."
  value       = aws_glue_schema.pharmacy_event.arn
}

output "policy_arn" {
  description = "ARN of the read + register-version IAM policy. Already attached to the EMR EC2 role; export for other consumers (Lambda processors, Glue jobs)."
  value       = aws_iam_policy.glue_sr.arn
}

output "schema_names" {
  description = "Map of logical name → Glue schema name."
  value = {
    sensor_reading = aws_glue_schema.sensor_reading.schema_name
    pharmacy_event = aws_glue_schema.pharmacy_event.schema_name
  }
}
