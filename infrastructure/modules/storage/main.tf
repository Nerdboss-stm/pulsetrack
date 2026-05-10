resource "aws_s3_bucket" "lakehouse" {
  bucket        = "pulsetrack-lakehouse-${var.environment}-${var.suffix}"
  force_destroy = false

  tags = {
    Name = "${var.name_prefix}-lakehouse"
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Lifecycle policy — scoped expirations per medallion convention.
#
# Production-correct lifecycle: never blanket-expire data paths. The earlier
# version had a single rule deleting EVERYTHING after 30 days, which would
# silently shred bronze/silver/gold data after a month. Scope expirations to
# ops-only paths: checkpoints, dlq, quarantine, smoke-test artifacts, EMR
# logs.
#
# Medallion data (bronze/, silver/, gold/, iceberg/warehouse/) lives
# indefinitely — Iceberg manages snapshot retention via
# ``writer.vacuum(retention_hours=...)`` calls and per-table
# ``expire_snapshots`` configurations.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse_lifecycle" {
  bucket = aws_s3_bucket.lakehouse.id

  # Streaming checkpoints — short-lived state.
  rule {
    id     = "expire-checkpoints"
    status = "Enabled"
    filter { prefix = "checkpoints/" }
    expiration { days = 30 }
  }

  # Dead-letter queue — keep 90 days for forensic re-processing windows.
  rule {
    id     = "expire-dlq"
    status = "Enabled"
    filter { prefix = "dlq/" }
    expiration { days = 90 }
  }

  # Quarantine — 90 days for operator review of failed-validation rows.
  rule {
    id     = "expire-quarantine"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration { days = 90 }
  }

  # Smoke-test artifacts — disposable.
  rule {
    id     = "expire-smoke-test"
    status = "Enabled"
    filter { prefix = "smoke-test/" }
    expiration { days = 7 }
  }

  # EMR step / driver / executor logs — short retention; CloudWatch keeps
  # the longer-term archive.
  rule {
    id     = "expire-emr-logs"
    status = "Enabled"
    filter { prefix = "emr-logs/" }
    expiration { days = 30 }
  }

  # Failed multipart uploads (anywhere in the bucket) — abort after a week.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # Defensive: if versioning is ever turned on, prune old noncurrent
  # versions. Today versioning is "Disabled" so this rule is a no-op.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# TLS-only bucket policy — reject non-HTTPS access. Modern S3 baseline;
# required by most healthcare/HIPAA-shaped compliance regimes. Doesn't
# break Iceberg/Spark/EMR access since they all use HTTPS by default.
data "aws_iam_policy_document" "lakehouse_tls_only" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lakehouse_tls_only" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = data.aws_iam_policy_document.lakehouse_tls_only.json
}

# Layout (created lazily on first write — listed here for reference):
#   bronze/sensor_readings/rid=<reversed_device_id>/dt=<date>/
#   silver/sensor_readings/
#   gold/fact_vital_daily_summary/date_key=<yyyymmdd>/
#   iceberg/warehouse/
#   checkpoints/
#   dlq/
#   quarantine/
#   bootstrap/

# Bootstrap script uploaded so EMR can fetch it on cluster launch.
resource "aws_s3_object" "emr_bootstrap" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "bootstrap/bootstrap.sh"
  source = "${path.module}/../compute/bootstrap.sh"
  etag   = filemd5("${path.module}/../compute/bootstrap.sh")

  content_type = "text/x-shellscript"
}

# ── CloudWatch Request Metrics on the lakehouse bucket ────────────────────
#
# Enables per-bucket request-level CloudWatch metrics under the
# AWS/S3 namespace: AllRequests, GetRequests, PutRequests, 4xxErrors,
# 5xxErrors (the 503 SlowDown counter we care about for partition
# benchmarks), FirstByteLatency, TotalRequestLatency.
#
# Metrics propagate to CloudWatch with ~15 min delay. For benchmarks
# in benchmarks/s3_partition_benchmark.py, query CloudWatch via
# ``aws cloudwatch get-metric-statistics --namespace AWS/S3 ...``
# 20 min after the run completes.
#
# Filter scoped to the ``benchmarks/`` prefix so the production
# bronze/silver/gold paths aren't measured (cuts CloudWatch cost and
# keeps the benchmark numbers clean — no production traffic noise).
resource "aws_s3_bucket_metric" "benchmark" {
  bucket = aws_s3_bucket.lakehouse.id
  name   = "benchmark-prefix"

  filter {
    prefix = "benchmarks/"
  }
}

# Bucket-wide metrics (no filter) — captures everything for ops
# observability. Free first 1k filters; this is one of them.
resource "aws_s3_bucket_metric" "bucket_wide" {
  bucket = aws_s3_bucket.lakehouse.id
  name   = "bucket-wide"
}
