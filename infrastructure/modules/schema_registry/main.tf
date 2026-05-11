# ─────────────────────────────────────────────────────────────────────────────
# Schema Registry module — AWS Glue Schema Registry for PulseTrack Kafka.
#
# Why this exists:
#   Cloud producers (batch_scale_producer.py, openfda_producer.py,
#   whoop_api/producer.py) and the bronze decoder (streaming/bronze_ingestion.py)
#   need a registry to:
#     (a) version Avro schemas with BACKWARD compatibility enforcement
#     (b) embed a registry-known schema-version-id in each Kafka message
#     (c) let the bronze decoder look up the schema for the message at runtime
#
#   docker-compose runs Confluent OSS Schema Registry locally for dev. In cloud
#   we use AWS Glue Schema Registry — free at our scale, IAM-native, and
#   CloudTrail-audited.
#
# Wire-format compatibility:
#   - Local (Confluent SR):  \x00 + 4-byte schema_id + Avro bytes
#   - Cloud (Glue SR):       header_version (1B) + compression (1B) +
#                            16-byte schema-version-UUID + Avro bytes
#   The bronze decoder uses ``schemas/registry.py`` which sniffs the prefix
#   byte to pick the right path.
#
# This module:
#   1. Provisions a Glue registry named ``pulsetrack-{env}-schemas``.
#   2. Registers ``sensor_reading`` + ``pharmacy_event`` Avro schemas, reading
#      the .avsc files at terraform plan time.
#   3. Sets compatibility = BACKWARD (matches docs/data_contracts.md §2.4).
#   4. Emits an IAM policy granting the EMR EC2 role read + register-version
#      permissions, attached automatically.
# ─────────────────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

# Repo root is two levels above this module.
locals {
  repo_root          = "${path.module}/../../.."
  sensor_avsc_path   = "${local.repo_root}/schemas/sensor_reading.avsc"
  pharmacy_avsc_path = "${local.repo_root}/schemas/pharmacy_event.avsc"

  # file() reads at plan time → terraform fails fast if schemas move,
  # which is the desired behavior.
  sensor_avsc_body   = file(local.sensor_avsc_path)
  pharmacy_avsc_body = file(local.pharmacy_avsc_path)
}

# ── Glue registry ─────────────────────────────────────────────────────────
resource "aws_glue_registry" "this" {
  registry_name = "${var.name_prefix}-schemas"
  description   = "PulseTrack ${var.environment} Avro schemas. Backed by AWS Glue Schema Registry."

  tags = {
    Name        = "${var.name_prefix}-schemas"
    Environment = var.environment
    Component   = "schema-registry"
  }
}

# ── Schemas ───────────────────────────────────────────────────────────────
# BACKWARD compatibility: new versions must be readable by old consumers.
resource "aws_glue_schema" "sensor_reading" {
  schema_name       = "sensor_reading"
  registry_arn      = aws_glue_registry.this.arn
  data_format       = "AVRO"
  compatibility     = "BACKWARD"
  schema_definition = local.sensor_avsc_body
  description       = "SensorReading Avro schema (wearable + simulator + whoop_api on topic sensor_readings)."

  tags = {
    Name        = "${var.name_prefix}-sensor-reading"
    Environment = var.environment
    Topic       = "sensor_readings"
  }
}

resource "aws_glue_schema" "pharmacy_event" {
  schema_name       = "pharmacy_event"
  registry_arn      = aws_glue_registry.this.arn
  data_format       = "AVRO"
  compatibility     = "BACKWARD"
  schema_definition = local.pharmacy_avsc_body
  description       = "PharmacyEvent Avro schema (openFDA + pharmacy adapter on topic pharmacy_events)."

  tags = {
    Name        = "${var.name_prefix}-pharmacy-event"
    Environment = var.environment
    Topic       = "pharmacy_events"
  }
}

# ── IAM policy — read + register-version on this registry's schemas ───────
data "aws_iam_policy_document" "glue_sr" {
  statement {
    sid    = "GlueSchemaRegistryRead"
    effect = "Allow"
    actions = [
      "glue:GetRegistry",
      "glue:ListRegistries",
      "glue:GetSchema",
      "glue:GetSchemaByDefinition",
      "glue:GetSchemaVersion",
      "glue:GetSchemaVersionsDiff",
      "glue:ListSchemas",
      "glue:ListSchemaVersions",
      "glue:QuerySchemaVersionMetadata",
    ]
    resources = [
      aws_glue_registry.this.arn,
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:schema/${aws_glue_registry.this.registry_name}/*",
    ]
  }

  # Producers register new versions during schema evolution. Server-side
  # BACKWARD compatibility check is the safety net.
  statement {
    sid    = "GlueSchemaRegistryWrite"
    effect = "Allow"
    actions = [
      "glue:RegisterSchemaVersion",
      "glue:PutSchemaVersionMetadata",
      "glue:CheckSchemaVersionValidity",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:schema/${aws_glue_registry.this.registry_name}/*",
    ]
  }
}

resource "aws_iam_policy" "glue_sr" {
  name        = "${var.name_prefix}-glue-sr"
  description = "Read + register-version on the ${aws_glue_registry.this.registry_name} Glue Schema Registry."
  policy      = data.aws_iam_policy_document.glue_sr.json
}

# Attach to EMR EC2 role (same pattern as modules/secrets — decoupled).
resource "aws_iam_role_policy_attachment" "emr_glue_sr" {
  count      = var.emr_ec2_role_name == "" ? 0 : 1
  role       = var.emr_ec2_role_name
  policy_arn = aws_iam_policy.glue_sr.arn
}
