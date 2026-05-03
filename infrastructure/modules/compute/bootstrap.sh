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
sudo pip3 install --upgrade pip
sudo pip3 install \
    pydantic==2.9.0 \
    pydantic-settings==2.6.0 \
    great-expectations==1.2.0 \
    prometheus-client==0.21.0 \
    confluent-kafka==2.6.0 \
    fastavro==1.9.5 \
    requests==2.32.0 \
    fhir.resources==7.1.0 \
    delta-spark==3.0.0

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
