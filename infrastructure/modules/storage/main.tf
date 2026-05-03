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

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse_lifecycle" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "cleanup-after-30-days"
    status = "Enabled"

    filter {} # apply to all objects

    expiration {
      days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
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
