-- ============================================================================
-- 02_create_storage_integration.sql
-- ============================================================================
-- Storage integration = the bridge between Snowflake and our S3 lakehouse.
-- Snowflake assumes an IAM role in our AWS account to read/write to the
-- pulsetrack-lakehouse-* bucket. No long-lived access keys.
--
-- Setup:
--   1. Create this STORAGE INTEGRATION in Snowflake.
--   2. Run ``DESC INTEGRATION`` to get STORAGE_AWS_IAM_USER_ARN and
--      STORAGE_AWS_EXTERNAL_ID.
--   3. In AWS, create an IAM role with a trust policy:
--        Principal: { AWS: <STORAGE_AWS_IAM_USER_ARN> }
--        Condition: { StringEquals: { sts:ExternalId: <STORAGE_AWS_EXTERNAL_ID> } }
--   4. Attach an S3-read+write policy to that role.
--   5. Update STORAGE_AWS_ROLE_ARN below with the new role ARN.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ── Storage integration ─────────────────────────────────────────────────────
CREATE OR REPLACE STORAGE INTEGRATION PULSETRACK_S3
    TYPE                       = EXTERNAL_STAGE
    STORAGE_PROVIDER           = 'S3'
    ENABLED                    = TRUE
    STORAGE_AWS_ROLE_ARN       = 'arn:aws:iam::960341592614:role/snowflake-pulsetrack-s3'
    STORAGE_ALLOWED_LOCATIONS  = ('s3://pulsetrack-lakehouse-dev-03a28ee7/')
    COMMENT                    = 'PulseTrack S3 lakehouse — assumes role snowflake-pulsetrack-s3';

-- After running, capture the AWS user + external ID for the AWS-side
-- trust policy:
--   DESC INTEGRATION PULSETRACK_S3;

-- ── External volume for Iceberg ─────────────────────────────────────────────
-- Snowflake's Iceberg integration needs an external-volume that points at
-- the same S3 bucket the EMR Spark pipeline writes through. This is what
-- lets Snowflake READ the Iceberg tables Spark writes — single source of
-- truth on S3 + Glue, two engines pointing at the same data.
CREATE OR REPLACE EXTERNAL VOLUME PULSETRACK_VOL
    STORAGE_LOCATIONS = (
        (
            NAME = 'pulsetrack-lakehouse-dev'
            STORAGE_PROVIDER  = 'S3'
            STORAGE_BASE_URL  = 's3://pulsetrack-lakehouse-dev-03a28ee7/iceberg/warehouse/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::960341592614:role/snowflake-pulsetrack-s3'
        )
    )
    ALLOW_WRITES = FALSE
    COMMENT      = 'PulseTrack Iceberg warehouse on S3 — read-only from Snowflake';

-- ── Catalog integration ─────────────────────────────────────────────────────
-- Tells Snowflake to use AWS Glue as the Iceberg catalog. Now ``SELECT *
-- FROM iceberg_table`` in Snowflake resolves the metadata via Glue,
-- reads the data files via the external volume above.
CREATE OR REPLACE CATALOG INTEGRATION PULSETRACK_GLUE
    CATALOG_SOURCE     = GLUE
    CATALOG_NAMESPACE  = 'pulsetrack_silver_dev'
    TABLE_FORMAT       = ICEBERG
    GLUE_AWS_ROLE_ARN  = 'arn:aws:iam::960341592614:role/snowflake-pulsetrack-glue'
    GLUE_CATALOG_ID    = '960341592614'
    GLUE_REGION        = 'us-east-1'
    ENABLED            = TRUE
    COMMENT            = 'AWS Glue catalog as Snowflake Iceberg catalog source';

-- ── Verify ──────────────────────────────────────────────────────────────────
-- DESC INTEGRATION PULSETRACK_S3;
-- DESC EXTERNAL VOLUME PULSETRACK_VOL;
-- DESC CATALOG INTEGRATION PULSETRACK_GLUE;
