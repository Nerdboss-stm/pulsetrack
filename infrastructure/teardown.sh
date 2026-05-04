#!/bin/bash
# PulseTrack — destroy all AWS resources. Empties the S3 bucket first since
# `terraform destroy` cannot remove a non-empty bucket.
set -euo pipefail

cd "$(dirname "$0")"

ENV_FILE="${1:-environments/dev.tfvars}"

echo "⚠️  This will destroy ALL PulseTrack AWS resources defined by this Terraform state."
echo "    var-file: ${ENV_FILE}"
echo "    Press Ctrl+C within 5 seconds to cancel..."
sleep 5

# Pull bucket name from outputs (only if we have state to read).
if terraform output -raw lakehouse_bucket_name >/dev/null 2>&1; then
  BUCKET="$(terraform output -raw lakehouse_bucket_name)"
  echo "Emptying s3://${BUCKET}/ (lifecycle expiration is 30d but destroy needs it empty NOW)..."
  aws s3 rm "s3://${BUCKET}" --recursive
else
  echo "No lakehouse_bucket_name output — skipping S3 empty step."
fi

echo "Running terraform destroy..."
terraform destroy -var-file="${ENV_FILE}" -auto-approve

echo "✅ All resources destroyed. Verify in the AWS console: https://console.aws.amazon.com/"
