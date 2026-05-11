
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

===============================================================================
PulseTrack scale test — 2026-05-11T14:31:46Z
===============================================================================


── [14:31:46Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (276ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (486ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T14:31:49.649853+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 317.7, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (318ms)
{"timestamp": "2026-05-11T14:31:49.739467+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 89.4, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (90ms)
{"timestamp": "2026-05-11T14:31:49.851318+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 111.5, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (112ms)
{"timestamp": "2026-05-11T14:31:49.940930+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 89.3, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (89ms)
{"timestamp": "2026-05-11T14:31:50.042655+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 101.6, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 206022s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (3286ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (955ms)
  [PASS] slack.webhook             http 200  (326ms)

0 FAIL / 14 checks
Ready for the scale test.

── [14:31:58Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [14:32:03Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [14:32:04Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 236K
  uploaded producer tarball: 6.1M

── [14:32:18Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-07111303JC1TMDTNRXN6

── [14:32:19Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-0080504161EYJZC1WP4K

── [14:32:21Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-0552204DPY2OCFB1CTP

── [14:32:22Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-0874471GOPD5DXWFE4O

── [14:32:24Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
Warning: Permanently added 'ec2-32-195-58-110.compute-1.amazonaws.com' (ED25519) to the list of known hosts.
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

===============================================================================
PulseTrack scale test — 2026-05-11T14:36:34Z
===============================================================================


── [14:36:34Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (348ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (453ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T14:36:39.444244+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 256.0, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (256ms)
{"timestamp": "2026-05-11T14:36:39.536413+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 92.0, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (92ms)
{"timestamp": "2026-05-11T14:36:39.626500+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 89.9, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (90ms)
{"timestamp": "2026-05-11T14:36:39.707332+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 80.5, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (81ms)
{"timestamp": "2026-05-11T14:36:39.784330+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 76.3, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 206312s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2600ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (961ms)
  [PASS] slack.webhook             http 200  (315ms)

0 FAIL / 14 checks
Ready for the scale test.

── [14:36:48Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [14:36:51Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [14:36:51Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 236K
  uploaded producer tarball: 6.1M

── [14:36:59Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-003640721UNT27N5E2EW

── [14:37:01Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-07087621DKBI05URGNZL

── [14:37:02Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-085851932XABYTDGH6R

── [14:37:03Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-00552993MWJZWDPEOCDL

── [14:37:04Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
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
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T14:40:39Z
===============================================================================


── [14:40:39Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (294ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (428ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T14:40:47.156413+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 253.9, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (254ms)
{"timestamp": "2026-05-11T14:40:47.269001+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 112.3, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (113ms)
{"timestamp": "2026-05-11T14:40:47.358950+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 89.7, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (90ms)
{"timestamp": "2026-05-11T14:40:47.464257+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 105.2, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (105ms)
{"timestamp": "2026-05-11T14:40:47.565455+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 101.1, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 206560s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1845ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (805ms)
  [PASS] slack.webhook             http 200  (304ms)

0 FAIL / 14 checks
Ready for the scale test.

── [14:40:55Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [14:40:58Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [14:40:58Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 236K
  uploaded producer tarball: 6.1M

── [14:41:04Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-0508760VDIANHB43QQZ

── [14:41:05Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-061104417XMBM9R6R5HZ

── [14:41:06Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-07307052LFN0QRRT2CV9

── [14:41:07Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-08549372IQTDO705YM7V

── [14:41:08Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
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
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T14:45:51Z
===============================================================================


── [14:45:51Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (267ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (405ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T14:45:57.107823+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 267.3, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (267ms)
{"timestamp": "2026-05-11T14:45:57.195294+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 86.9, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (88ms)
{"timestamp": "2026-05-11T14:45:57.285913+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 90.4, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (90ms)
{"timestamp": "2026-05-11T14:45:57.370477+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 84.4, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (85ms)
{"timestamp": "2026-05-11T14:45:57.459801+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 89.0, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 206869s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2549ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (954ms)
  [PASS] slack.webhook             http 200  (286ms)

0 FAIL / 14 checks
Ready for the scale test.

── [14:46:08Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [14:46:21Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [14:46:23Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 236K
  uploaded producer tarball: 6.1M

── [14:46:31Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-10176272ZJ0DLNS342II

── [14:46:32Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-006877335039CU4VOMUG

── [14:46:34Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-06004644GY6G78D3QB4

── [14:46:35Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-0600477AFJ4HGINX7IG

── [14:46:36Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
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
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 205: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T14:50:44Z
===============================================================================


── [14:50:44Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (340ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (390ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T14:50:47.952414+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 252.3, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (253ms)
{"timestamp": "2026-05-11T14:50:48.035907+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 83.3, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (83ms)
{"timestamp": "2026-05-11T14:50:48.117967+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 81.9, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (82ms)
{"timestamp": "2026-05-11T14:50:48.219093+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 100.9, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (101ms)
{"timestamp": "2026-05-11T14:50:48.311775+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 92.5, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 207160s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1873ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (711ms)
  [PASS] slack.webhook             http 200  (319ms)

0 FAIL / 14 checks
Ready for the scale test.

── [14:50:55Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [14:50:57Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [14:50:57Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 236K
  uploaded producer tarball: 6.1M

── [14:51:07Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-07622831A62G176YIFPA

── [14:51:09Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-07238061668821YF2QOR

── [14:51:10Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-085213528HQV5BK1E52I

── [14:51:11Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-04485872R0CGC1928RZP

── [14:51:12Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 213: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 213: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 213: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 213: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 213: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T14:54:25Z
===============================================================================


── [14:54:25Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (294ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (397ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T14:54:28.607854+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 253.5, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (254ms)
{"timestamp": "2026-05-11T14:54:28.702790+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 94.7, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (95ms)
{"timestamp": "2026-05-11T14:54:28.813168+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 109.9, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (110ms)
{"timestamp": "2026-05-11T14:54:28.898791+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 85.2, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (85ms)
{"timestamp": "2026-05-11T14:54:28.991673+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 92.6, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 207381s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1990ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (870ms)
  [PASS] slack.webhook             http 200  (286ms)

0 FAIL / 14 checks
Ready for the scale test.

── [14:54:35Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [14:54:39Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [14:54:39Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [14:54:47Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-096775839VXTW6YDIDLY

── [14:54:48Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-05103302ULHSBQJVWG4

── [14:54:49Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-04453812W6MXTKQ7NM40

── [14:54:51Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-0146463BA15ACLPQY0K

── [14:54:52Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 214: [[: 0
0: syntax error in expression (error token is "0")
./scripts/run_scale_test.sh: line 216: [[: 0
0: syntax error in expression (error token is "0")

ABORT: Streams did not become active within 5 min (need ≥3 of 4)

===============================================================================
PulseTrack scale test — 2026-05-11T15:00:27Z
===============================================================================


── [15:00:27Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (262ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (415ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:00:32.009823+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 253.0, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (253ms)
{"timestamp": "2026-05-11T15:00:32.095167+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 85.1, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (85ms)
{"timestamp": "2026-05-11T15:00:32.182928+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 87.6, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (88ms)
{"timestamp": "2026-05-11T15:00:32.276703+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 93.5, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (94ms)
{"timestamp": "2026-05-11T15:00:32.369471+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 92.5, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 207744s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1742ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (760ms)
  [PASS] slack.webhook             http 200  (555ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:00:41Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:00:44Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:00:44Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:00:50Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-04497114KOJF5HMTU6F

── [15:00:52Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-07096931NMCZV52EGYO2

── [15:00:53Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-0663269LKHZRXI7MLUE

── [15:00:54Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-01866623LL2EUMIERSLX

── [15:00:55Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 221: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 221: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 221: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T15:02:47Z
===============================================================================


── [15:02:47Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (304ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (541ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:02:52.521286+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 444.2, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (444ms)
{"timestamp": "2026-05-11T15:02:52.610627+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 89.2, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (89ms)
{"timestamp": "2026-05-11T15:02:52.701095+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 90.3, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (91ms)
{"timestamp": "2026-05-11T15:02:52.855476+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 154.2, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (154ms)
{"timestamp": "2026-05-11T15:02:52.945201+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 89.6, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 207885s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2182ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (751ms)
  [PASS] slack.webhook             http 200  (396ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:03:01Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:03:04Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:03:04Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:03:10Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-08678903ER5EWHKVHDQM

── [15:03:11Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-04484153PFSYGZNZNPSR

── [15:03:12Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-03366581XQ1R2K4EQ95C

── [15:03:13Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-00812691UO95SY9Z82FY

── [15:03:15Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 223: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 223: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 223: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 223: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 223: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T15:06:03Z
===============================================================================


── [15:06:03Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (289ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (386ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:06:07.382099+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 275.2, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (275ms)
{"timestamp": "2026-05-11T15:06:07.467808+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 85.6, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (86ms)
{"timestamp": "2026-05-11T15:06:07.559846+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 91.9, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (92ms)
{"timestamp": "2026-05-11T15:06:07.643469+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 83.5, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (84ms)
{"timestamp": "2026-05-11T15:06:07.728793+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 85.2, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 208080s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2105ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (711ms)
  [PASS] slack.webhook             http 200  (317ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:06:15Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:06:18Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:06:18Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:06:24Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-04444371TIN0UWX6DCGL

── [15:06:26Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-01133922XYT1HQSGWON7

── [15:06:27Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-0663029S1GHA8VF1C8S

── [15:06:28Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-001023228257CPBOYOV

── [15:06:29Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 235: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 235: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 235: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 235: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 235: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T15:09:38Z
===============================================================================


── [15:09:38Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (248ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (484ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:09:41.985088+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 251.9, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (252ms)
{"timestamp": "2026-05-11T15:09:42.075692+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 90.4, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (91ms)
{"timestamp": "2026-05-11T15:09:42.159093+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 83.1, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (83ms)
{"timestamp": "2026-05-11T15:09:42.242906+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 83.6, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (84ms)
{"timestamp": "2026-05-11T15:09:42.324907+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 81.9, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 208294s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2215ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (797ms)
  [PASS] slack.webhook             http 200  (297ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:09:50Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:09:53Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:09:54Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:10:01Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
%3|1778512202.205|FAIL|Sarans-MacBook-Air.local#producer-1| [thrd:sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaw]: sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098/bootstrap: Failed to resolve 'boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098': nodename nor servname provided, or not known (after 77ms in state CONNECT)
%3|1778512204.112|FAIL|Sarans-MacBook-Air.local#producer-1| [thrd:sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaw]: sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098/bootstrap: Failed to resolve 'boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098': nodename nor servname provided, or not known (after 8ms in state CONNECT, 1 identical error(s) suppressed)
%3|1778512235.204|FAIL|Sarans-MacBook-Air.local#producer-1| [thrd:sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaw]: sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098/bootstrap: Failed to resolve 'boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098': nodename nor servname provided, or not known (after 1ms in state CONNECT, 16 identical error(s) suppressed)
%3|1778512266.322|FAIL|Sarans-MacBook-Air.local#producer-1| [thrd:sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaw]: sasl_ssl://boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098/bootstrap: Failed to resolve 'boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098': nodename nor servname provided, or not known (after 2ms in state CONNECT, 19 identical error(s) suppressed)
  topic sensor_readings ERROR: KafkaError{code=_TIMED_OUT,val=-185,str="Failed while waiting for controller: Local: Timed out"}
  topic pharmacy_events ERROR: KafkaError{code=_TIMED_OUT,val=-185,str="Failed while waiting for controller: Local: Timed out"}
  topic pulsetrack_dlq ERROR: KafkaError{code=_TIMED_OUT,val=-185,str="Failed while waiting for controller: Local: Timed out"}

── [15:11:06Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-0336066226Q3Y06Y1SQU

── [15:11:08Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-0184564IJ4KYMFNASAE

── [15:11:09Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-0864226RAZCUC8GGKRC

── [15:11:10Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-076847522BN0J5S0SRUW

── [15:11:11Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──

===============================================================================
PulseTrack scale test — 2026-05-11T15:12:34Z
===============================================================================


── [15:12:34Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (292ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (419ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:12:38.949418+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 273.1, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (274ms)
{"timestamp": "2026-05-11T15:12:39.029723+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 80.1, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (80ms)
{"timestamp": "2026-05-11T15:12:39.119744+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 89.8, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (90ms)
{"timestamp": "2026-05-11T15:12:39.196145+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 76.1, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (76ms)
{"timestamp": "2026-05-11T15:12:39.279676+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 83.3, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 208471s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1832ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (703ms)
  [PASS] slack.webhook             http 200  (396ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:12:46Z] T-29m Verifying terraform state ──
  cluster_id=j-27HWNKEKO0I29
  master_dns=ec2-32-195-58-110.compute-1.amazonaws.com
  msk=boot-uetkmida.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:12:49Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:12:50Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:12:59Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
  topic sensor_readings already exists (OK)
  topic pharmacy_events already exists (OK)
  topic pulsetrack_dlq already exists (OK)

── [15:13:06Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-01877831KG5ZF3HA7WM1

── [15:13:07Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-04707003NSBQO6JDDTAZ

── [15:13:08Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-03680492F1MWVH405V2R

── [15:13:09Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-06628151P9TIJYT8GGP2

── [15:13:10Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
./scripts/run_scale_test.sh: line 288: [[: 0
0: syntax error in expression (error token is "0")

ABORT: Streams did not become active within 5 min (need ≥3 of 4)

===============================================================================
PulseTrack scale test — 2026-05-11T15:51:41Z
===============================================================================


── [15:51:41Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (239ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (421ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:51:44.976806+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 234.5, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (235ms)
{"timestamp": "2026-05-11T15:51:45.070008+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 93.1, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (93ms)
{"timestamp": "2026-05-11T15:51:45.147928+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 77.7, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (78ms)
{"timestamp": "2026-05-11T15:51:45.232223+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 84.2, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (84ms)
{"timestamp": "2026-05-11T15:51:45.311192+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 78.9, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 210817s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1593ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (853ms)
  [PASS] slack.webhook             http 200  (326ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:51:51Z] T-29m Verifying terraform state ──
  cluster_id=j-T5OF7WBI2I4V
  master_dns=ec2-3-228-22-91.compute-1.amazonaws.com
  msk=boot-hgcm6ppg.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:51:55Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:51:56Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:52:05Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
Warning: Permanently added 'ec2-3-228-22-91.compute-1.amazonaws.com' (ED25519) to the list of known hosts.
  created topic: sensor_readings
  created topic: pharmacy_events
  created topic: pulsetrack_dlq

── [15:52:13Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-01390473AP53T1YYO2CT

── [15:52:14Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-037621712K17E57NRQ8J

── [15:52:16Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-01820313DUE5KJ90QSXO

── [15:52:17Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-09677492WS4CADZOGHPZ

── [15:52:19Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T15:54:35Z
===============================================================================


── [15:54:35Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (425ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (392ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:54:40.537723+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 260.6, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (262ms)
{"timestamp": "2026-05-11T15:54:40.628574+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 90.6, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (91ms)
{"timestamp": "2026-05-11T15:54:40.715764+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 87.0, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (87ms)
{"timestamp": "2026-05-11T15:54:40.806398+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 90.5, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (91ms)
{"timestamp": "2026-05-11T15:54:40.903103+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 96.4, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 210993s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2925ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (843ms)
  [PASS] slack.webhook             http 200  (300ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:54:56Z] T-29m Verifying terraform state ──
  cluster_id=j-T5OF7WBI2I4V
  master_dns=ec2-3-228-22-91.compute-1.amazonaws.com
  msk=boot-hgcm6ppg.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:55:05Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:55:05Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:55:16Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
  topic sensor_readings already exists (OK)
  topic pharmacy_events already exists (OK)
  topic pulsetrack_dlq already exists (OK)

── [15:55:23Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-0979116382BI993Z9GL4

── [15:55:26Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-06060441TW6X0A824ONV

── [15:55:27Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-09285213A0KT8KSBRKIB

── [15:55:29Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-00742282NJNH91BF3666

── [15:55:30Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 286: [[: 0
0: syntax error in expression (error token is "0")

===============================================================================
PulseTrack scale test — 2026-05-11T15:58:10Z
===============================================================================


── [15:58:10Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (250ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (399ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T15:58:14.203631+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 278.2, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (278ms)
{"timestamp": "2026-05-11T15:58:14.303642+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 99.8, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (100ms)
{"timestamp": "2026-05-11T15:58:14.395248+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 91.4, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (92ms)
{"timestamp": "2026-05-11T15:58:14.494458+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 98.8, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (99ms)
{"timestamp": "2026-05-11T15:58:14.576084+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 81.3, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 211207s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (1906ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (828ms)
  [PASS] slack.webhook             http 200  (271ms)

0 FAIL / 14 checks
Ready for the scale test.

── [15:58:22Z] T-29m Verifying terraform state ──
  cluster_id=j-T5OF7WBI2I4V
  master_dns=ec2-3-228-22-91.compute-1.amazonaws.com
  msk=boot-hgcm6ppg.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [15:58:25Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [15:58:25Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 392K
  uploaded producer tarball: 6.1M

── [15:58:30Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
  topic sensor_readings already exists (OK)
  topic pharmacy_events already exists (OK)
  topic pulsetrack_dlq already exists (OK)

── [15:58:37Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-10255051Z5PU888MZ7X9

── [15:58:38Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-06680172S59FC0MS6RMH

── [15:58:39Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-08942043AG5V066GF7TT

── [15:58:40Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-01040183SDQFR3Y29EFR

── [15:58:42Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
  apps_running=0
0 (target=4)
./scripts/run_scale_test.sh: line 297: [[: 0
0: syntax error in expression (error token is "0")
