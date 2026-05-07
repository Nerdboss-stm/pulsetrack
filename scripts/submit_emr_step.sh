#!/bin/bash
# Submit a PySpark job to the running EMR cluster.
# Usage: bash scripts/submit_emr_step.sh <relative-path-to-py-file>
# Example: bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
set -euo pipefail

JOB="${1:-}"
if [[ -z "$JOB" ]]; then
  echo "Usage: $0 <relative-path-to-py-file>" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT/infrastructure"

CLUSTER_ID="$(terraform output -raw emr_cluster_id)"
BUCKET="$(terraform output -raw lakehouse_bucket_name)"
cd "$REPO_ROOT"

# Package source tree (excluding heavy/irrelevant dirs) and stage on S3.
TARBALL=/tmp/pulsetrack.tar.gz
tar czf "$TARBALL" \
    --exclude='.git' \
    --exclude='venv' \
    --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='infrastructure/.terraform' \
    --exclude='spark-warehouse' \
    --exclude='_delta_log' \
    .
aws s3 cp "$TARBALL" "s3://${BUCKET}/code/pulsetrack.tar.gz"
aws s3 cp "$JOB" "s3://${BUCKET}/code/${JOB}"

aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --steps "[{
        \"Name\": \"${JOB}\",
        \"ActionOnFailure\": \"CONTINUE\",
        \"HadoopJarStep\": {
            \"Jar\": \"command-runner.jar\",
            \"Args\": [
                \"spark-submit\",
                \"--deploy-mode\", \"cluster\",
                \"--conf\", \"spark.pyspark.python=/usr/bin/python3\",
                \"s3://${BUCKET}/code/${JOB}\"
            ]
        }
    }]"

echo "Submitted ${JOB} to EMR cluster ${CLUSTER_ID}"
