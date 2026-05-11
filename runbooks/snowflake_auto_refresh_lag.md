# Runbook: Snowflake Iceberg AUTO_REFRESH lag

**Severity ladder:**
- SEV3: a single Snowflake-side Iceberg table is > 10 min stale vs S3 last-write
- SEV2: any GOLD-schema Iceberg table is > 30 min stale, OR > 1 table is stale across schemas
- SEV1: AUTO_REFRESH has been failing for all Iceberg tables in `PULSETRACK` > 1 hour (dashboards dark, analyst pages firing)

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

PulseTrack's primary integration is single-source-of-truth on S3+Glue with two compute engines (EMR Spark writes, Snowflake reads). `snowflake/setup/04_create_iceberg_tables.sql` creates `ICEBERG TABLE` objects with `AUTO_REFRESH=TRUE` over the same Glue catalog the streaming pipeline writes. When AUTO_REFRESH stops, Snowflake keeps serving the last refreshed snapshot — queries succeed, but they return stale data. There is no error to react to unless the freshness monitor catches it.

Four failure modes:
1. **Storage-integration role drift** — the IAM role `snowflake-pulsetrack-s3` (`snowflake/setup/02_create_storage_integration.sql`) has its trust policy revoked or its S3 read permissions removed
2. **S3 event-notification disabled** — the bucket-side event subscription that nudges Snowflake to refresh is gone (or never re-created after a Terraform run)
3. **AUTO_REFRESH suspended due to repeated failures** — Snowflake auto-pauses after sequential refresh errors
4. **Catalog integration `PULSETRACK_GLUE` lost Glue read permission** — `snowflake-pulsetrack-glue` role can't list tables anymore

## TL;DR (30-second triage)

```sql
-- 1. Are any Iceberg tables in PULSETRACK stale?
USE WAREHOUSE PULSETRACK_WH;
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    LAST_REFRESHED_AT,
    TIMESTAMPDIFF('minute', LAST_REFRESHED_AT, CURRENT_TIMESTAMP()) AS age_min
FROM SNOWFLAKE.INFORMATION_SCHEMA.ICEBERG_TABLE_REFRESH_HISTORY
WHERE TABLE_CATALOG = 'PULSETRACK'
ORDER BY age_min DESC;
```

```bash
# 2. What's the actual S3 last-write for the same table?
aws s3 ls s3://pulsetrack-lakehouse-${PT_AWS_ENV:-dev}-03a28ee7/iceberg/warehouse/pulsetrack_bronze_dev.db/sensor_readings/metadata/ \
    --recursive | sort -k1,2 | tail -5
```

```sql
-- 3. Is AUTO_REFRESH still enabled on the table that's stale?
DESC ICEBERG TABLE PULSETRACK.BRONZE.SENSOR_READINGS;
-- Look for AUTO_REFRESH=TRUE in the output
```

## Symptoms

- Freshness monitor on `glue_iceberg.pulsetrack_gold_dev.fact_vital_daily_summary` reports OK on EMR-side, but Snowflake-side query of `PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY` returns row counts that haven't moved
- `INFORMATION_SCHEMA.ICEBERG_TABLE_REFRESH_HISTORY` shows `LAST_REFRESH_STATUS = 'FAILED'` repeatedly
- Snowflake Snowsight notification: "Iceberg table refresh failure"
- Analyst Slack thread: "Why does the Snowflake dashboard not show today's data?"
- Refresh task `PULSETRACK.GOLD.REFRESH_ICEBERG_METADATA` (scheduled `5 MINUTE` in `04_create_iceberg_tables.sql`) shows error history

## Diagnosis

### 1. Identify which tables are stale and by how much

```sql
USE ROLE PULSETRACK_RW;
USE WAREHOUSE PULSETRACK_WH;

SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    LAST_REFRESH_STATUS,
    LAST_REFRESHED_AT,
    LAST_REFRESH_ERROR,
    TIMESTAMPDIFF('minute', LAST_REFRESHED_AT, CURRENT_TIMESTAMP()) AS age_min
FROM SNOWFLAKE.INFORMATION_SCHEMA.ICEBERG_TABLE_REFRESH_HISTORY
WHERE TABLE_CATALOG = 'PULSETRACK'
  AND LAST_REFRESHED_AT < DATEADD('minute', -10, CURRENT_TIMESTAMP())
ORDER BY age_min DESC;
```

The `LAST_REFRESH_ERROR` column is where the actual cause lives. Common values:
- `"AccessDenied"` / `"403 Forbidden"` → IAM role lost permission (case A or D)
- `"NoSuchKey"` → S3 path moved (Terraform drift) or external volume points at the wrong prefix
- `"GlueClientException: AccessDeniedException"` → catalog integration's Glue role broken
- `null` with a long `age_min` and no recent failure → S3 event notifications stopped firing (case B)

### 2. Compare Snowflake's last-refresh to S3's last-write

The "right" answer for `LAST_REFRESHED_AT` is "within ~1 min of the latest `metadata/*.json` file in S3" (Iceberg writes a new metadata json on every snapshot).

```bash
TABLE=sensor_readings
SCHEMA=pulsetrack_bronze_dev
aws s3 ls s3://pulsetrack-lakehouse-${PT_AWS_ENV:-dev}-03a28ee7/iceberg/warehouse/$SCHEMA.db/$TABLE/metadata/ \
    | grep -E '\.metadata\.json$' | sort -k1,2 | tail -1
```

If the S3 timestamp is recent but Snowflake's `LAST_REFRESHED_AT` is hours old, AUTO_REFRESH has stopped reacting to S3 events.

### 3. Check the storage and catalog integrations are still valid

```sql
DESC INTEGRATION PULSETRACK_S3;
-- Look at:
--   ENABLED                       = TRUE
--   STORAGE_AWS_ROLE_ARN          = arn:aws:iam::960341592614:role/snowflake-pulsetrack-s3
--   STORAGE_AWS_EXTERNAL_ID       = (note the value)
--   STORAGE_AWS_IAM_USER_ARN      = (note the value)

DESC EXTERNAL VOLUME PULSETRACK_VOL;
-- Look for ALLOW_WRITES=FALSE and the same role arn

DESC CATALOG INTEGRATION PULSETRACK_GLUE;
-- ENABLED=TRUE, GLUE_AWS_ROLE_ARN=arn:...:role/snowflake-pulsetrack-glue
```

Then on the AWS side, verify the trust policy still trusts the captured `STORAGE_AWS_IAM_USER_ARN` with the captured `STORAGE_AWS_EXTERNAL_ID`:

```bash
aws iam get-role --role-name snowflake-pulsetrack-s3 \
    --query 'Role.AssumeRolePolicyDocument' --output json | jq .

# Confirm policies attached
aws iam list-attached-role-policies --role-name snowflake-pulsetrack-s3
aws iam list-role-policies --role-name snowflake-pulsetrack-s3
```

You're looking for: `Principal.AWS == STORAGE_AWS_IAM_USER_ARN` and `Condition.StringEquals."sts:ExternalId" == STORAGE_AWS_EXTERNAL_ID`. If they don't match (Snowflake rotates these on `CREATE OR REPLACE`), the assume-role fails.

### 4. Verify S3 event notification subscription

AUTO_REFRESH relies on S3 event notifications hitting Snowflake's SQS queue. The queue ARN was set up by Snowflake when the integration was created; check the bucket subscription:

```bash
aws s3api get-bucket-notification-configuration \
    --bucket pulsetrack-lakehouse-${PT_AWS_ENV:-dev}-03a28ee7
# Look for a QueueConfiguration with QueueArn containing "snowflakecomputing"
# and Filter.Key.FilterRules pointing at the iceberg/warehouse prefix
```

If `QueueConfigurations` is empty or doesn't include Snowflake's queue, S3 events aren't reaching Snowflake. This is the most common cause of "tables aren't refreshing but nothing in the error log".

## Recovery (ranked fastest first)

### Case A: Manual refresh — proves the path still works (and unblocks the dashboard for the next 5 min)

If you need to unblock an analyst RIGHT NOW while you diagnose:

```sql
USE ROLE PULSETRACK_RW;
USE WAREHOUSE PULSETRACK_WH;

ALTER ICEBERG TABLE PULSETRACK.BRONZE.SENSOR_READINGS REFRESH;
ALTER ICEBERG TABLE PULSETRACK.SILVER.SENSOR_READINGS REFRESH;
ALTER ICEBERG TABLE PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY REFRESH;
-- ... repeat for any other stale tables

-- Verify
SELECT TABLE_SCHEMA, TABLE_NAME, LAST_REFRESHED_AT, LAST_REFRESH_STATUS
FROM SNOWFLAKE.INFORMATION_SCHEMA.ICEBERG_TABLE_REFRESH_HISTORY
WHERE TABLE_CATALOG = 'PULSETRACK'
ORDER BY LAST_REFRESHED_AT DESC;
```

If manual refresh **succeeds** → integrations are healthy, only AUTO_REFRESH wiring is broken (cases B, C). Move to Case B/C.

If manual refresh **fails** → check `LAST_REFRESH_ERROR` and move to Case D.

### Case B: Re-enable S3 event notifications

If diagnosis step 4 showed the notification config is missing, Snowflake provides the queue ARN via:

```sql
DESC NOTIFICATION INTEGRATION PULSETRACK_S3_EVENTS;
-- (if you have one) OR
SHOW NOTIFICATION INTEGRATIONS;
```

Re-attach via AWS CLI (replace `<sf-queue-arn>` with the value):

```bash
cat > /tmp/notif.json << 'EOF'
{
  "QueueConfigurations": [
    {
      "Id": "snowflake-pulsetrack-iceberg-events",
      "QueueArn": "<sf-queue-arn>",
      "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "prefix", "Value": "iceberg/warehouse/"}
          ]
        }
      }
    }
  ]
}
EOF

aws s3api put-bucket-notification-configuration \
    --bucket pulsetrack-lakehouse-${PT_AWS_ENV:-dev}-03a28ee7 \
    --notification-configuration file:///tmp/notif.json
```

This is the path you take when Terraform was re-applied and dropped the notification block, or when someone manually replaced the bucket notification config for another integration and wiped Snowflake's.

### Case C: AUTO_REFRESH was toggled off (or auto-suspended)

If `DESC ICEBERG TABLE` shows `AUTO_REFRESH = FALSE`, flip it back on:

```sql
ALTER ICEBERG TABLE PULSETRACK.BRONZE.SENSOR_READINGS  SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.SILVER.SENSOR_READINGS  SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.SILVER.EHR_CONDITIONS   SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.SILVER.EHR_MEDICATIONS  SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.SILVER.PHARMACY_FILLS   SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.SILVER.IDENTITY_BRIDGE  SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.DIM_PATIENT        SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.DIM_DEVICE         SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.DIM_METRIC         SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.DIM_DATE           SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.FACT_VITAL_READING SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.FACT_LAB_RESULT    SET AUTO_REFRESH = TRUE;
ALTER ICEBERG TABLE PULSETRACK.GOLD.FACT_PHARMACY_FILL SET AUTO_REFRESH = TRUE;
```

Also resume the scheduled refresh task that 04_create_iceberg_tables.sql leaves suspended by default:

```sql
ALTER TASK PULSETRACK.GOLD.REFRESH_ICEBERG_METADATA RESUME;
SHOW TASKS IN SCHEMA PULSETRACK.GOLD;  -- confirm state='STARTED'
```

### Case D: Repair the storage / catalog integration

If manual `ALTER ICEBERG TABLE ... REFRESH` failed with an access error:

**D1. Storage integration role drift (most common after Snowflake `CREATE OR REPLACE`):**

```sql
DESC INTEGRATION PULSETRACK_S3;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values
```

Update the IAM role trust policy:

```bash
cat > /tmp/trust.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"}}
  }]
}
EOF

aws iam update-assume-role-policy \
    --role-name snowflake-pulsetrack-s3 \
    --policy-document file:///tmp/trust.json
```

Wait ~30 seconds for IAM propagation, then re-test:

```sql
ALTER ICEBERG TABLE PULSETRACK.BRONZE.SENSOR_READINGS REFRESH;
```

**D2. Glue catalog role missing permission:**

```bash
aws iam list-attached-role-policies --role-name snowflake-pulsetrack-glue
# Verify a policy with glue:GetTable, glue:GetTables, glue:GetDatabases on
# arn:aws:glue:us-east-1:960341592614:database/pulsetrack_*
# AND glue:GetCatalogs

# If missing, attach the canonical policy
aws iam attach-role-policy \
    --role-name snowflake-pulsetrack-glue \
    --policy-arn arn:aws:iam::960341592614:policy/snowflake-pulsetrack-glue-read
```

**D3. Catalog integration disabled:**

```sql
ALTER CATALOG INTEGRATION PULSETRACK_GLUE SET ENABLED = TRUE;
ALTER INTEGRATION PULSETRACK_S3           SET ENABLED = TRUE;
```

After any of D1–D3, re-run the manual refresh from Case A and confirm `LAST_REFRESH_STATUS='SUCCESS'`.

## Verification

1. **Manual refresh succeeds across all tables:**
   ```sql
   SELECT TABLE_SCHEMA, TABLE_NAME, LAST_REFRESH_STATUS, LAST_REFRESHED_AT
   FROM SNOWFLAKE.INFORMATION_SCHEMA.ICEBERG_TABLE_REFRESH_HISTORY
   WHERE TABLE_CATALOG = 'PULSETRACK'
   ORDER BY LAST_REFRESHED_AT DESC;
   -- Every row should show status='SUCCESS' and a recent timestamp
   ```

2. **Snowflake row count matches EMR-side:**
   ```sql
   -- Snowflake side
   SELECT COUNT(*) FROM PULSETRACK.SILVER.SENSOR_READINGS;
   ```
   ```bash
   # EMR side
   spark-sql -e "SELECT COUNT(*) FROM glue_iceberg.${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings"
   ```
   The two should agree (within one snapshot's worth of new writes).

3. **AUTO_REFRESH actually firing:** wait 5 min after a known EMR write, query `LAST_REFRESHED_AT`, confirm it advanced without you calling `ALTER ... REFRESH`.

4. **Scheduled task running:**
   ```sql
   SELECT *
   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
   WHERE NAME = 'REFRESH_ICEBERG_METADATA';
   -- STATE='SUCCEEDED' rows every 5 min
   ```

## Prevention

- **Pin the bucket notification config in Terraform** under `infrastructure/` — if `aws s3api put-bucket-notification-configuration` keeps getting wiped by infrastructure drift, that's the root cause for half the SEV2s here. Add a `terraform plan` smoke check to the pre-deploy script.
- **`DESC INTEGRATION PULSETRACK_S3` after every `CREATE OR REPLACE`** — capture the new `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` and propagate to the IAM trust policy before clicking "approve" on the Snowflake PR. The integration *will* rotate these.
- **Freshness probes on Snowflake-side tables** in addition to the EMR-side ones — `observability/sql/monitor_spec.yaml` should include a probe against `SNOWFLAKE.INFORMATION_SCHEMA.ICEBERG_TABLE_REFRESH_HISTORY` so this runbook fires before an analyst notices.
- **Resume `REFRESH_ICEBERG_METADATA` as part of every deploy** — the task is created SUSPENDED in `04_create_iceberg_tables.sql`. A deploy that runs the SQL but forgets the `ALTER TASK ... RESUME` line is silently broken from minute one.
- **Add an alert on `LAST_REFRESH_STATUS != 'SUCCESS'`** — there's no built-in Snowflake alert, but a `CREATE ALERT` on the INFORMATION_SCHEMA view fires through the same SNS topic (`arn:aws:sns:us-east-1:960341592614:pulsetrack-${PT_AWS_ENV}-alerts`) the rest of `observability/alerting.py` uses.

## Related postmortems

- *(none yet)*

## Related runbooks

- `runbooks/kafka_consumer_lag.md` — downstream symptom mentioned in its "Symptoms" section; if Snowflake AUTO_REFRESH lags only because the upstream stream lagged, fix the stream first
- `runbooks/schema_drift.md` — a bronze-side `ALTER TABLE ADD COLUMN` requires Snowflake to refresh metadata; if `AUTO_REFRESH=FALSE` the new column won't appear
- `runbooks/gx_failure_drains_batch.md` — silver gate failures stall EMR-side writes, which makes Snowflake AUTO_REFRESH *appear* broken (LAST_REFRESHED_AT is fresh, row counts are stale)
