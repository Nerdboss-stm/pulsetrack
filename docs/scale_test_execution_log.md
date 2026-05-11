
===============================================================================
PulseTrack scale test — 2026-05-11T05:13:34Z
===============================================================================


── [05:13:34Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (280ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (434ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T05:13:38.452449+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 260.5, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (261ms)
{"timestamp": "2026-05-11T05:13:38.541292+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 88.7, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (89ms)
{"timestamp": "2026-05-11T05:13:38.624257+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 82.8, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (83ms)
{"timestamp": "2026-05-11T05:13:38.696435+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 72.0, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (72ms)
{"timestamp": "2026-05-11T05:13:38.787979+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 90.7, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 172531s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1517ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (714ms)
  [PASS] slack.webhook             http 200  (297ms)

0 FAIL / 14 checks
Ready for the scale test.

── [05:13:45Z] T-29m Verifying terraform state ──
  cluster_id=j-2M0QZS6DNOHK1
  master_dns=ec2-44-192-7-58.compute-1.amazonaws.com
  msk=boot-koh4qhsc.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [05:13:48Z] T-25m Apply pending Glacierbase migrations ──
usage: migrations [-h] [--catalog CATALOG]
                  {run,pending,dry-run,validate,status,rollback,create} ...
migrations: error: the following arguments are required: cmd
  WARN: migrations failed or already applied (continuing)

── [05:13:49Z] T-20m Sync project to EMR master via S3 ──
Completed 1.0 MiB/5.2 MiB (3.3 MiB/s) with 1 file(s) remainingCompleted 2.0 MiB/5.2 MiB (2.3 MiB/s) with 1 file(s) remainingCompleted 3.0 MiB/5.2 MiB (3.3 MiB/s) with 1 file(s) remainingCompleted 4.0 MiB/5.2 MiB (4.2 MiB/s) with 1 file(s) remainingCompleted 5.0 MiB/5.2 MiB (5.0 MiB/s) with 1 file(s) remainingCompleted 5.2 MiB/5.2 MiB (4.0 MiB/s) with 1 file(s) remainingupload: ../../../tmp/pulsetrack-scale-test.tar.gz to s3://pulsetrack-lakehouse-dev-03a28ee7/code/pulsetrack-scale-test.tar.gz
  Uploaded 5.2M tarball

── [05:13:53Z] T-15m Start streaming bronze (sensor) ──

aws: [ERROR]: An error occurred (ParamValidation): Parameter validation failed:
Unknown parameter in [0]: "HadoopJarStep", must be one of: Type, Name, ActionOnFailure, Jar, Args, MainClass, Properties, LogUri, EncryptionKeyArn

===============================================================================
PulseTrack scale test — 2026-05-11T05:15:08Z
===============================================================================


── [05:15:08Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (285ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (397ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T05:15:11.709381+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 257.8, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (258ms)
{"timestamp": "2026-05-11T05:15:11.802149+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 92.2, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (93ms)
{"timestamp": "2026-05-11T05:15:11.887976+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 85.4, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (86ms)
{"timestamp": "2026-05-11T05:15:11.970881+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 82.5, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (83ms)
{"timestamp": "2026-05-11T05:15:12.052602+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 81.2, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 172624s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2087ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (730ms)
  [PASS] slack.webhook             http 200  (273ms)

0 FAIL / 14 checks
Ready for the scale test.

── [05:15:18Z] T-29m Verifying terraform state ──
  cluster_id=j-2M0QZS6DNOHK1
  master_dns=ec2-44-192-7-58.compute-1.amazonaws.com
  msk=boot-koh4qhsc.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [05:15:20Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [05:15:20Z] T-20m Sync project to EMR master via S3 ──
Completed 1.0 MiB/5.2 MiB (3.8 MiB/s) with 1 file(s) remainingCompleted 2.0 MiB/5.2 MiB (3.1 MiB/s) with 1 file(s) remainingCompleted 3.0 MiB/5.2 MiB (4.2 MiB/s) with 1 file(s) remainingCompleted 4.0 MiB/5.2 MiB (5.2 MiB/s) with 1 file(s) remainingCompleted 5.0 MiB/5.2 MiB (6.3 MiB/s) with 1 file(s) remainingCompleted 5.2 MiB/5.2 MiB (4.6 MiB/s) with 1 file(s) remainingupload: ../../../tmp/pulsetrack-scale-test.tar.gz to s3://pulsetrack-lakehouse-dev-03a28ee7/code/pulsetrack-scale-test.tar.gz
  Uploaded 6.1M tarball

── [05:15:22Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-05749019OV18WKPMMZD

── [05:15:24Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-05816101C8K8PCSVZ6QK

── [05:15:25Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-0444414WJI0DMVPDJ9D

── [05:15:26Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-048051520OXVFG6PDLPA

── [05:15:27Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
Warning: Permanently added 'ec2-44-192-7-58.compute-1.amazonaws.com' (ED25519) to the list of known hosts.
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 130: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 130: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 130: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 130: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 130: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T05:20:22Z
===============================================================================


── [05:20:22Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (245ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (495ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T05:20:26.760514+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 266.0, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (266ms)
{"timestamp": "2026-05-11T05:20:26.858242+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 97.3, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (98ms)
{"timestamp": "2026-05-11T05:20:26.936456+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 78.0, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (78ms)
{"timestamp": "2026-05-11T05:20:27.021906+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 85.3, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (85ms)
{"timestamp": "2026-05-11T05:20:27.112411+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 90.4, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 172939s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2172ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (664ms)
  [PASS] slack.webhook             http 200  (261ms)

0 FAIL / 14 checks
Ready for the scale test.

── [05:20:34Z] T-29m Verifying terraform state ──
  cluster_id=j-2M0QZS6DNOHK1
  master_dns=ec2-44-192-7-58.compute-1.amazonaws.com
  msk=boot-koh4qhsc.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [05:20:36Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [05:20:36Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 236K
  uploaded producer tarball: 6.1M

── [05:20:55Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-00840171R98VJFZKH8T8

── [05:20:56Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-04650271X8AQSSL0STOO

── [05:20:57Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-02562031QVOZSVWG5E4E

── [05:20:58Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-076321328HLCOI9JEVHS

── [05:21:00Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")
