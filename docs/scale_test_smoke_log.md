
===============================================================================
PulseTrack scale test — 2026-05-11T18:18:00Z — mode=smoke-1000
  event_count=1000 user_count=50
===============================================================================


── [18:18:00Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (288ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (385ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T18:18:04.356465+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 246.2, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (246ms)
{"timestamp": "2026-05-11T18:18:04.447114+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 89.3, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (91ms)
{"timestamp": "2026-05-11T18:18:04.526830+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 79.3, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (80ms)
{"timestamp": "2026-05-11T18:18:04.611559+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 84.4, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (85ms)
{"timestamp": "2026-05-11T18:18:04.697609+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 85.8, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 219597s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (2424ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (1036ms)
  [PASS] slack.webhook             http 200  (301ms)

0 FAIL / 14 checks
Ready for the scale test.

── [18:18:12Z] T-29m Verifying terraform state ──
  cluster_id=j-1RW3D543TC5GG
  master_dns=ec2-32-197-183-52.compute-1.amazonaws.com
  msk=boot-6ndadg2m.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [18:18:14Z] T-25m Apply pending Glacierbase migrations ──
ERROR: 'unset env var referenced in config: ${LAKEHOUSE_BUCKET}'
  WARN: migrations failed or already applied (continuing)

── [18:18:15Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 396K
  uploaded producer tarball: 6.1M

── [18:18:21Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
  topic sensor_readings already exists (OK)
  topic pharmacy_events already exists (OK)
  topic pulsetrack_dlq already exists (OK)

── [18:18:27Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-0279851XTUSQU09ZUZJ

── [18:18:29Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-04405621YQ4K5CX62GHA

── [18:18:30Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-082251521FENJ85ADZHF

── [18:18:31Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-0290138R5UFVRKLU93Z

── [18:18:33Z] T-12m Start streaming bronze (pharmacy) ──
  bronze_pharmacy_step_id=s-0639316FWNKGJ3G0EFN

── [18:18:34Z] T-11m Pre-build static dimensions (4 dims, parallel batch) ──
  dim_metric=s-0069241RB9DWGQPUGKP dim_date=s-09581962MX55X4HRF70D dim_device=s-01188435PRMY8HSJE9J dim_time=s-06623173CQCN0WDWC97F

── [18:18:39Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=0 (target=5)
  apps_running=3 (target=5)
  apps_running=5 (target=5)

── [18:20:22Z] T-10m Launch all 4 producers on EMR master (tmux sessions) ──
Last metadata expiration check: 0:14:08 ago on Mon May 11 18:06:16 2026.
Dependencies resolved.
================================================================================
 Package     Architecture  Version                     Repository          Size
================================================================================
Installing:
 tmux        x86_64        3.2a-3.amzn2023.0.2         amazonlinux        478 k

Transaction Summary
================================================================================
Install  1 Package

Total download size: 478 k
Installed size: 1.1 M
Downloading Packages:
tmux-3.2a-3.amzn2023.0.2.x86_64.rpm              10 MB/s | 478 kB     00:00    
--------------------------------------------------------------------------------
Total                                           5.2 MB/s | 478 kB     00:00     
Running transaction check
Transaction check succeeded.
Running transaction test
Transaction test succeeded.
Running transaction
  Preparing        :                                                        1/1 
  Installing       : tmux-3.2a-3.amzn2023.0.2.x86_64                        1/1 
  Running scriptlet: tmux-3.2a-3.amzn2023.0.2.x86_64                        1/1 
  Verifying        : tmux-3.2a-3.amzn2023.0.2.x86_64                        1/1 

Installed:
  tmux-3.2a-3.amzn2023.0.2.x86_64                                               

Complete!
Completed 256.0 KiB/5.2 MiB (1.2 MiB/s) with 1 file(s) remainingCompleted 512.0 KiB/5.2 MiB (2.4 MiB/s) with 1 file(s) remainingCompleted 768.0 KiB/5.2 MiB (3.6 MiB/s) with 1 file(s) remainingCompleted 1.0 MiB/5.2 MiB (4.8 MiB/s) with 1 file(s) remaining  Completed 1.2 MiB/5.2 MiB (5.9 MiB/s) with 1 file(s) remaining  Completed 1.5 MiB/5.2 MiB (7.1 MiB/s) with 1 file(s) remaining  Completed 1.8 MiB/5.2 MiB (8.3 MiB/s) with 1 file(s) remaining  Completed 2.0 MiB/5.2 MiB (9.4 MiB/s) with 1 file(s) remaining  Completed 2.2 MiB/5.2 MiB (10.6 MiB/s) with 1 file(s) remaining Completed 2.5 MiB/5.2 MiB (11.7 MiB/s) with 1 file(s) remaining Completed 2.8 MiB/5.2 MiB (12.8 MiB/s) with 1 file(s) remaining Completed 3.0 MiB/5.2 MiB (14.0 MiB/s) with 1 file(s) remaining Completed 3.2 MiB/5.2 MiB (15.1 MiB/s) with 1 file(s) remaining Completed 3.5 MiB/5.2 MiB (16.2 MiB/s) with 1 file(s) remaining Completed 3.8 MiB/5.2 MiB (17.3 MiB/s) with 1 file(s) remaining Completed 4.0 MiB/5.2 MiB (18.4 MiB/s) with 1 file(s) remaining Completed 4.2 MiB/5.2 MiB (19.5 MiB/s) with 1 file(s) remaining Completed 4.5 MiB/5.2 MiB (12.6 MiB/s) with 1 file(s) remaining Completed 4.8 MiB/5.2 MiB (13.3 MiB/s) with 1 file(s) remaining Completed 5.0 MiB/5.2 MiB (13.9 MiB/s) with 1 file(s) remaining Completed 5.2 MiB/5.2 MiB (14.4 MiB/s) with 1 file(s) remaining download: s3://pulsetrack-lakehouse-dev-03a28ee7/code/pulsetrack-scale-test.tar.gz to ../../tmp/pulsetrack-scale-test.tar.gz
tar: Ignoring unknown extended header keyword 'SCHILY.fflags'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.FinderInfo'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.metadata:kMDItemTextContentLanguage'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.quarantine'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.metadata:kMDItemWhereFroms'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.macl'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.quarantine'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.metadata:kMDItemWhereFroms'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.metadata:kMDItemDownloadedDate'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.macl'
tar: Ignoring unknown extended header keyword 'SCHILY.fflags'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.FinderInfo'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.lastuseddate#PS'
tar: Ignoring unknown extended header keyword 'LIBARCHIVE.xattr.com.apple.lastuseddate#PS'
  batch-scale PID=30316
  whoop PID=30383
  openfda PID=30419
  fhir PID=30462
  All 4 producers launched (nohup). Logs at /tmp/{batch-scale,whoop,openfda,fhir}.log on master.

── [18:20:52Z] T-8m Sync EHR batches to S3 (master FS → s3://pulsetrack-lakehouse-dev-03a28ee7/ehr-batches/) ──

── [18:20:53Z] T-8m Submit batch tier (silver_ehr + silver_pharmacy + identity_bridge + dim_patient) ──
  silver_ehr=s-037578631A2V8MRYJ8S1 silver_pharmacy=s-0154898MYQDEVQ8A4GQ
  identity_bridge=s-0188062277JVATO1HAM4
  dim_patient=s-05784263GKZAM4F8101V

── [18:23:01Z] T-5m Trigger Prefect deployments (ad-hoc) ──
13:23:05.778 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8361
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
Deployment 'dbt-weekly/dbt-weekly' not found!
13:23:13.434 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8361
  WARN: prefect dbt-weekly trigger failed (continuing — flow may be unconfigured)
13:23:21.361 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8610
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
Deployment 'streaming-monitor/streaming-monitor' not found!
13:23:27.150 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8610
  WARN: prefect streaming-monitor trigger failed

── [18:23:27Z] T+0m Active monitoring (15 min before chaos) ──
  Open: Grafana, CloudWatch, Prefect UI.
  Watching for: producer rate, MSK ingress, bronze write rate.
  SMOKE mode: monitoring 3 min instead of 15, chaos drills skipped.

── [18:26:27Z] T+15m Chaos drills SKIPPED in smoke mode ──

── [18:26:27Z] T+45m Stop producers + drain streams ──
