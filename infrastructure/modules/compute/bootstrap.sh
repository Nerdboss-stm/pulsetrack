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
sudo pip3 install --ignore-installed \
    pydantic==2.9.0 \
    pydantic-settings==2.6.0 \
    great-expectations==1.2.0 \
    prometheus-client==0.21.0 \
    confluent-kafka==2.6.0 \
    fastavro==1.9.5 \
    requests==2.32.0 \
    fhir.resources==7.1.0 \
    aws-msk-iam-sasl-signer-python==1.0.2
sudo pip3 install --no-deps delta-spark==3.0.0

# ─── Delta Lake JVM jars on Spark classpath ─────────────────────────────────
# EMR 7.x bundles Delta jars at /usr/share/aws/delta/lib/ but does NOT
# symlink them into /usr/lib/spark/jars/ the way it does for Iceberg.
# Without these symlinks, spark-defaults.conf's
# `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
# fails with ClassNotFoundException at session init. Mirror EMR's pattern:
# symlink the AMZN-built jars (3.1.0-amzn-0, matched to Spark 3.5.x / Scala 2.12).
DELTA_LIB="/usr/share/aws/delta/lib"
SPARK_LIB="/usr/lib/spark/jars"
if [[ -d "$DELTA_LIB" ]]; then
  sudo ln -sf "$DELTA_LIB/delta-spark_2.12-3.1.0-amzn-0.jar" \
              "$SPARK_LIB/delta-spark_2.12-3.1.0-amzn-0.jar"
  sudo ln -sf "$DELTA_LIB/delta-storage-3.1.0-amzn-0.jar" \
              "$SPARK_LIB/delta-storage-3.1.0-amzn-0.jar"
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
