#!/bin/bash
# PulseTrack EMR bootstrap — installs Python deps and stages project code on every node.
# Arg 1: bootstrap S3 bucket (also used as the staging location for pulsetrack.tar.gz)
set -ex

BOOTSTRAP_BUCKET="${1:-}"
if [[ -z "$BOOTSTRAP_BUCKET" ]]; then
  echo "ERROR: bootstrap bucket arg is required" >&2
  exit 1
fi

# ─── Python dependencies ────────────────────────────────────────────────────
# Notes:
#   * Do NOT `pip install --upgrade pip` on EMR/Amazon Linux — pip is
#     installed via RPM and pip can't uninstall its own RPM-installed copy.
#   * `--ignore-installed` lets us install over distro packages where needed.
#   * `delta-spark` and `pyspark` are installed with `--no-deps`. The default
#     install pulls PyPI's pyspark, which puts a `/usr/local/bin/spark-submit`
#     ahead of EMR's `/usr/lib/spark/bin/spark-submit` on PATH, leaving
#     SPARK_HOME unset and stripping EMR's jar discovery. We use the EMR-bundled
#     pyspark on the cluster nodes; the Python `delta` package is just the
#     thin wrapper around the JVM jars (which we install separately below).
# Pinned to latest stable as of EMR 7.13.0 / Spark 3.5.6 (May 2026).
# Versions chosen for compatibility with the EMR-bundled Spark/Hadoop/Iceberg
# build — bumping any one line should be paired with a re-run of the unit
# tests in ``tests/`` and a smoke test against the cluster.
# EMR 7.13 ships Python 3.11 alongside 3.9; ``spark-submit`` defaults to
# 3.11 (PYSPARK_PYTHON in /usr/lib/spark/conf/spark-env.sh). Install all
# Python deps under 3.11 explicitly — using ``pip3`` resolves to 3.9 and
# Spark drivers fail with ModuleNotFoundError at runtime.
PYTHON_BIN=/usr/bin/python3.11

sudo $PYTHON_BIN -m pip install --ignore-installed \
    pydantic==2.10.6 \
    pydantic-settings==2.8.1 \
    great-expectations==1.3.13 \
    prometheus-client==0.21.1 \
    confluent-kafka==2.8.0 \
    fastavro==1.10.0 \
    requests==2.32.3 \
    fhir.resources==8.0.0 \
    aws-msk-iam-sasl-signer-python==1.0.2 \
    PyYAML==6.0.2 \
    boto3

# delta-spark Python wrapper — installed --no-deps because its declared
# pyspark dep otherwise pulls PyPI's pyspark and clobbers
# /usr/lib/spark/bin/spark-submit. The Delta JVM jars are symlinked from
# /usr/share/aws/delta/lib/ below — that's the actual dependency.
# 3.3.x line for Spark 3.5.x; if the EMR Spark version changes, revisit.
sudo $PYTHON_BIN -m pip install --no-deps delta-spark==3.3.0

# ─── Delta Lake JVM jars on Spark classpath ─────────────────────────────────
# EMR 7.x bundles Delta jars at /usr/share/aws/delta/lib/ but does NOT
# symlink them into /usr/lib/spark/jars/ the way it does for Iceberg.
# Without these symlinks, spark-defaults.conf's
# `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
# fails with ClassNotFoundException at session init. Mirror EMR's pattern:
# resolve the AMZN-built jars by glob (the version + amzn-N suffix changes
# with each EMR release — emr-7.2.0 had 3.1.0-amzn-0, emr-7.13.0 has a
# different version, etc.) so this bootstrap survives release-label bumps
# without code changes.
DELTA_LIB="/usr/share/aws/delta/lib"
SPARK_LIB="/usr/lib/spark/jars"
if [[ -d "$DELTA_LIB" ]]; then
  for jar in "$DELTA_LIB"/delta-spark_2.12-*-amzn-*.jar \
             "$DELTA_LIB"/delta-storage-*-amzn-*.jar; do
    [[ -f "$jar" ]] && sudo ln -sf "$jar" "$SPARK_LIB/$(basename "$jar")"
  done
  echo "Delta jars symlinked into $SPARK_LIB:"
  ls -la "$SPARK_LIB"/delta-*.jar 2>&1 | head
fi

# ─── PulseTrack project code ────────────────────────────────────────────────
# Operator uploads s3://${BOOTSTRAP_BUCKET}/bootstrap/pulsetrack.tar.gz before launch.
# If the tarball is missing we skip — the cluster still boots and the operator
# can `aws s3 cp` + extract afterwards.
sudo mkdir -p /opt/pulsetrack
if aws s3 ls "s3://${BOOTSTRAP_BUCKET}/bootstrap/pulsetrack.tar.gz" >/dev/null 2>&1; then
  aws s3 cp "s3://${BOOTSTRAP_BUCKET}/bootstrap/pulsetrack.tar.gz" /tmp/pulsetrack.tar.gz
  sudo tar xzf /tmp/pulsetrack.tar.gz -C /opt/pulsetrack
  rm -f /tmp/pulsetrack.tar.gz
fi
