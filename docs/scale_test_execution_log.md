
===============================================================================
PulseTrack scale test — 2026-05-11T18:31:41Z — mode=full-10M
  event_count=10000000 user_count=50000
===============================================================================


── [18:31:41Z] T-30m Pre-flight credential validator ──
PulseTrack pre-flight (14 checks)

  [PASS] aws.sts                   account=960341592614 arn=arn:aws:iam::960341592614:user/pulsetrack-admin  (281ms)
  [PASS] aws.region                region=us-east-1
  [PASS] aws.s3                    bucket=pulsetrack-lakehouse-dev-03a28ee7 put+delete OK  (393ms)
  [PASS] aws.glue                  all 3 dbs present: ['pulsetrack_bronze_dev', 'pulsetrack_silver_dev', 'pulsetrack_gold_dev']
  [PASS] aws.msk                   name=pulsetrack-dev-msk state=ACTIVE
{"timestamp": "2026-05-11T18:31:48.388614+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop", "source": "aws", "latency_ms": 358.9, "fields_count": 5}
  [PASS] secret.whoop              5 fields resolved  (359ms)
{"timestamp": "2026-05-11T18:31:48.471247+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "anthropic", "source": "aws", "latency_ms": 82.0, "fields_count": 1}
  [PASS] secret.anthropic          1 fields resolved  (82ms)
{"timestamp": "2026-05-11T18:31:48.552708+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "snowflake", "source": "aws", "latency_ms": 81.3, "fields_count": 7}
  [PASS] secret.snowflake          7 fields resolved  (81ms)
{"timestamp": "2026-05-11T18:31:48.626361+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "slack", "source": "aws", "latency_ms": 73.5, "fields_count": 1}
  [PASS] secret.slack              1 fields resolved  (74ms)
{"timestamp": "2026-05-11T18:31:48.697804+00:00", "level": "INFO", "logger": "pt_secrets.manager", "message": "secret resolved", "module": "manager", "function": "get", "line": 269, "secret": "whoop-tokens", "source": "aws", "latency_ms": 71.3, "fields_count": 3}
  [WARN] whoop.tokens              access_token expired 220421s ago — refresh will fire on next API call
  [PASS] snowflake.connect         version=10.16.101  (3761ms)
  [WARN] snowflake.views           could not validate: ProgrammingError: 002003 (42S02): SQL compilation error:
Object 'PULSETRACK.INFORMATION_SCHEMA.ICEBERG_TABLES' does not exist or not authorized.
  [PASS] anthropic.ping              (1062ms)
  [PASS] slack.webhook             http 200  (341ms)

0 FAIL / 14 checks
Ready for the scale test.

── [18:32:01Z] T-29m Verifying terraform state ──
  cluster_id=j-1RW3D543TC5GG
  master_dns=ec2-32-197-183-52.compute-1.amazonaws.com
  msk=boot-6ndadg2m.c3.kafka-serverless.us-east-1.amazonaws.com:9098
  bucket=pulsetrack-lakehouse-dev-03a28ee7

── [18:32:09Z] T-25m Apply pending Glacierbase migrations ──
Traceback (most recent call last):
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/runpy.py", line 196, in _run_module_as_main
    return _run_code(code, main_globals, None,
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/runpy.py", line 86, in _run_code
    exec(code, run_globals)
  File "/Users/nerdboss-stm/pulsetrack-cm/migrations/cli.py", line 489, in <module>
    sys.exit(main())
  File "/Users/nerdboss-stm/pulsetrack-cm/migrations/cli.py", line 479, in main
    return args.func(args)
  File "/Users/nerdboss-stm/pulsetrack-cm/migrations/cli.py", line 274, in cmd_run
    with _LockGuard(cfg):
  File "/Users/nerdboss-stm/pulsetrack-cm/migrations/cli.py", line 166, in __enter__
    self._handle = lock_mod.acquire(self.cfg.catalog, self._lock_cfg)
  File "/Users/nerdboss-stm/pulsetrack-cm/migrations/lock.py", line 105, in acquire
    ddb.put_item(
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/site-packages/botocore/client.py", line 569, in _api_call
    return self._make_api_call(operation_name, kwargs)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/site-packages/botocore/client.py", line 1023, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceNotFoundException: An error occurred (ResourceNotFoundException) when calling the PutItem operation: Requested resource not found
  WARN: migrations failed or already applied (continuing)

── [18:32:11Z] T-20m Sync project to EMR master via S3 ──
  syncing source tree to s3://pulsetrack-lakehouse-dev-03a28ee7/code/
  uploaded deps zip: 416K
  uploaded producer tarball: 6.1M

── [18:32:29Z] T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq) ──
  topic sensor_readings already exists (OK)
  topic pharmacy_events already exists (OK)
  topic pulsetrack_dlq already exists (OK)

── [18:32:37Z] T-15m Start streaming bronze (sensor) ──
  bronze_step_id=s-07678571DH8T5IXCIH3Y

── [18:32:40Z] T-14m Start streaming silver (sensor) ──
  silver_step_id=s-09306771L8C5R7XSW48S

── [18:32:41Z] T-13m Start gold fact_vital_reading ──
  gold_fvr_step_id=s-0375606UA4879HBUK2N

── [18:32:43Z] T-12m Start gold fact_vital_daily_summary ──
  gold_fvd_step_id=s-00679932Y5UPXHWVP608

── [18:32:47Z] T-12m Start streaming bronze (pharmacy) ──
  bronze_pharmacy_step_id=s-06915103O7C07BD5EW6D

── [18:32:49Z] T-11m Pre-build static dimensions (4 dims, parallel batch) ──
  dim_metric=s-08922483440JRM4KHI6N dim_date=s-08669854OGF40BH37C4 dim_device=s-01805303VA3FUZ6I0CGY dim_time=s-03369793VKAJZT67VWX1

── [18:33:16Z] T-12m Wait for streams to be ACTIVE (max 5 min) ──
  apps_running=2 (target=5)
  apps_running=4 (target=5)

── [18:34:28Z] T-10m Launch all 4 producers on EMR master (tmux sessions) ──
Requirement already satisfied: httpx in /usr/local/lib/python3.11/site-packages (0.28.1)
Requirement already satisfied: anyio in /usr/local/lib/python3.11/site-packages (from httpx) (4.13.0)
Requirement already satisfied: certifi in /usr/local/lib/python3.11/site-packages (from httpx) (2026.4.22)
Requirement already satisfied: httpcore==1.* in /usr/local/lib/python3.11/site-packages (from httpx) (1.0.9)
Requirement already satisfied: idna in /usr/local/lib/python3.11/site-packages (from httpx) (3.14)
Requirement already satisfied: h11>=0.16 in /usr/local/lib/python3.11/site-packages (from httpcore==1.*->httpx) (0.16.0)
Requirement already satisfied: typing_extensions>=4.5 in /usr/local/lib/python3.11/site-packages (from anyio->httpx) (4.15.0)
Completed 256.0 KiB/5.2 MiB (1.3 MiB/s) with 1 file(s) remainingCompleted 512.0 KiB/5.2 MiB (2.6 MiB/s) with 1 file(s) remainingCompleted 768.0 KiB/5.2 MiB (3.8 MiB/s) with 1 file(s) remainingCompleted 1.0 MiB/5.2 MiB (5.1 MiB/s) with 1 file(s) remaining  Completed 1.2 MiB/5.2 MiB (6.3 MiB/s) with 1 file(s) remaining  Completed 1.5 MiB/5.2 MiB (7.5 MiB/s) with 1 file(s) remaining  Completed 1.8 MiB/5.2 MiB (8.8 MiB/s) with 1 file(s) remaining  Completed 2.0 MiB/5.2 MiB (10.0 MiB/s) with 1 file(s) remaining Completed 2.2 MiB/5.2 MiB (11.2 MiB/s) with 1 file(s) remaining Completed 2.5 MiB/5.2 MiB (12.3 MiB/s) with 1 file(s) remaining Completed 2.8 MiB/5.2 MiB (13.5 MiB/s) with 1 file(s) remaining Completed 3.0 MiB/5.2 MiB (14.7 MiB/s) with 1 file(s) remaining Completed 3.2 MiB/5.2 MiB (15.8 MiB/s) with 1 file(s) remaining Completed 3.5 MiB/5.2 MiB (17.0 MiB/s) with 1 file(s) remaining Completed 3.8 MiB/5.2 MiB (18.1 MiB/s) with 1 file(s) remaining Completed 4.0 MiB/5.2 MiB (19.2 MiB/s) with 1 file(s) remaining Completed 4.2 MiB/5.2 MiB (20.4 MiB/s) with 1 file(s) remaining Completed 4.5 MiB/5.2 MiB (21.5 MiB/s) with 1 file(s) remaining Completed 4.8 MiB/5.2 MiB (22.6 MiB/s) with 1 file(s) remaining Completed 5.0 MiB/5.2 MiB (23.7 MiB/s) with 1 file(s) remaining Completed 5.2 MiB/5.2 MiB (24.7 MiB/s) with 1 file(s) remaining download: s3://pulsetrack-lakehouse-dev-03a28ee7/code/pulsetrack-scale-test.tar.gz to ../../tmp/pulsetrack-scale-test.tar.gz
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
  batch-scale PID=43177
  whoop PID=47832
  openfda PID=47878
  ehr PID=47929
  All 4 producers launched (nohup). Logs at /tmp/{batch-scale,whoop,openfda,fhir}.log on master.

── [18:40:33Z] T-8m Sync EHR batches to S3 (master FS → s3://pulsetrack-lakehouse-dev-03a28ee7/ehr-batches/) ──

── [18:40:35Z] T-8m Submit batch tier (silver_ehr + silver_pharmacy + identity_bridge + dim_patient) ──
  silver_ehr=s-09667172HQCV59MJ91BB silver_pharmacy=s-0088434EQEIHXOZJ9MR
  identity_bridge=s-0087069XCIOU9ZNAK96
  dim_patient=s-07101202F5EWHKV4YG2I

── [18:42:47Z] T-5m Trigger Prefect deployments (ad-hoc) ──
13:42:51.233 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8072
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
Deployment 'dbt-weekly/dbt-weekly' not found!
13:42:54.592 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8072
  WARN: prefect dbt-weekly trigger failed (continuing — flow may be unconfigured)
13:42:58.658 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8670
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
Deployment 'streaming-monitor/streaming-monitor' not found!
13:43:02.082 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8670
  WARN: prefect streaming-monitor trigger failed

── [18:43:02Z] T+0m Active monitoring (15 min before chaos) ──
  Open: Grafana, CloudWatch, Prefect UI.
  Watching for: producer rate, MSK ingress, bronze write rate.

── [18:58:02Z] T+15m Chaos drill 1 — kill ONE silver executor ──
[chaos-1] Target: app_name~='silver_sensor_streaming' host=ec2-32-197-183-52.compute-1.amazonaws.com
Traceback (most recent call last):
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_task.py", line 286, in <module>
    sys.exit(main())
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_task.py", line 230, in main
    app_id = find_app_id(args.app_name, args.ssh_key, host)
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_task.py", line 89, in find_app_id
    stdout = ssh_master(
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_task.py", line 69, in ssh_master
    result = subprocess.run(full, capture_output=True, text=True, timeout=60)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 505, in run
    stdout, stderr = process.communicate(input, timeout=timeout)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 1154, in communicate
    stdout, stderr = self._communicate(input, endtime, timeout)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 2022, in _communicate
    self._check_timeout(endtime, orig_timeout, stdout, stderr)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 1198, in _check_timeout
    raise TimeoutExpired(
subprocess.TimeoutExpired: Command '['ssh', '-i', '/Users/nerdboss-stm/.ssh/pulsetrack-emr.pem', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes', 'hadoop@ec2-32-197-183-52.compute-1.amazonaws.com', 'yarn application -list -appStates RUNNING']' timed out after 60 seconds
  WARN: drill 1 failed — postmortem will capture details

── [19:09:03Z] T+30m Chaos drill 2 — kill ENTIRE silver streaming app ──
[chaos-2] Target: app~='silver_sensor_streaming' host=ec2-32-197-183-52.compute-1.amazonaws.com cluster=j-1RW3D543TC5GG
Traceback (most recent call last):
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_app.py", line 269, in <module>
    sys.exit(main())
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_app.py", line 198, in main
    app_id = find_app_id(args.app_name, args.ssh_key, host)
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_task.py", line 89, in find_app_id
    stdout = ssh_master(
  File "/Users/nerdboss-stm/pulsetrack-cm/scripts/chaos/kill_spark_task.py", line 69, in ssh_master
    result = subprocess.run(full, capture_output=True, text=True, timeout=60)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 505, in run
    stdout, stderr = process.communicate(input, timeout=timeout)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 1154, in communicate
    stdout, stderr = self._communicate(input, endtime, timeout)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 2022, in _communicate
    self._check_timeout(endtime, orig_timeout, stdout, stderr)
  File "/Users/nerdboss-stm/.pyenv/versions/3.10.12/lib/python3.10/subprocess.py", line 1198, in _check_timeout
    raise TimeoutExpired(
subprocess.TimeoutExpired: Command '['ssh', '-i', '/Users/nerdboss-stm/.ssh/pulsetrack-emr.pem', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes', 'hadoop@ec2-32-197-183-52.compute-1.amazonaws.com', 'yarn application -list -appStates RUNNING']' timed out after 60 seconds
  WARN: drill 2 failed — postmortem will capture details

── [19:20:04Z] T+45m Stop producers + drain streams ──
ABORT at line 529: rc=255 cmd="$SSH "pkill -TERM -f batch_scale_producer || true;       pkill -TERM -f 'whoop_api.producer' || true;       pkill -TERM -f openfda_producer || true;       pkill -TERM -f fhir_producer || true;       echo 'producer stop signals sent'""
