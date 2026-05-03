resource "aws_glue_catalog_database" "bronze" {
  name        = "pulsetrack_bronze_${var.environment}"
  description = "PulseTrack Bronze layer (raw ingestion)"

  location_uri = "s3://${var.lakehouse_bucket}/bronze/"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "pulsetrack_silver_${var.environment}"
  description = "PulseTrack Silver layer (cleaned, conformed)"

  location_uri = "s3://${var.lakehouse_bucket}/silver/"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "pulsetrack_gold_${var.environment}"
  description = "PulseTrack Gold layer (snowflake schema)"

  location_uri = "s3://${var.lakehouse_bucket}/gold/"
}

# Crawlers — kept disabled by default (run manually via `aws glue start-crawler`)
# to avoid unexpected charges. Each crawler points at its layer prefix.
resource "aws_glue_crawler" "bronze" {
  name          = "${var.name_prefix}-bronze-crawler"
  database_name = aws_glue_catalog_database.bronze.name
  role          = var.glue_role_arn

  s3_target {
    path = "s3://${var.lakehouse_bucket}/bronze/"
  }

  schedule = null # operator-triggered

  tags = { Layer = "bronze" }
}

resource "aws_glue_crawler" "silver" {
  name          = "${var.name_prefix}-silver-crawler"
  database_name = aws_glue_catalog_database.silver.name
  role          = var.glue_role_arn

  s3_target {
    path = "s3://${var.lakehouse_bucket}/silver/"
  }

  schedule = null

  tags = { Layer = "silver" }
}

resource "aws_glue_crawler" "gold" {
  name          = "${var.name_prefix}-gold-crawler"
  database_name = aws_glue_catalog_database.gold.name
  role          = var.glue_role_arn

  s3_target {
    path = "s3://${var.lakehouse_bucket}/gold/"
  }

  schedule = null

  tags = { Layer = "gold" }
}
