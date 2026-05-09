#!/bin/bash
# PulseTrack — destroy only the expensive bits (EMR + MSK).
# Keeps everything that costs $0 ongoing: VPC, S3 (with data), Glue, IAM, CloudWatch, SNS.
#
# Why this scope:
#   - Stop the ~$1.30/hr burn the moment EMR + MSK are gone.
#   - Networking + monitoring are free anyway — destroying them just makes the
#     next `terraform apply` slower (~10 extra min for VPC) and forces an email
#     re-confirmation for the SNS budget alert subscription.
#   - S3 + Glue + IAM hold all the data and metadata and stay untouched.
#
# For a full nuke (including data + bucket), use teardown.sh instead.

set -euo pipefail

cd "$(dirname "$0")"

ENV_FILE="${1:-environments/dev.tfvars}"

echo "⚠️  This will destroy EMR cluster + MSK Serverless cluster (the expensive bits)."
echo "    KEEPING: VPC, S3 bucket (and all data), Glue databases, IAM roles,"
echo "             CloudWatch dashboard, SNS topic + email subscription, Budget alarm."
echo "    var-file: ${ENV_FILE}"
echo "    Press Ctrl+C within 5 seconds to cancel..."
sleep 5

terraform destroy \
    -target=module.compute \
    -target=module.kafka \
    -var-file="${ENV_FILE}" \
    -auto-approve

echo "✅ EMR + MSK destroyed. Hourly burn stopped (~\$1.30/hr → \$0)."
echo "   VPC, S3 (with data), Glue, IAM, monitoring all persist for ~\$0/month."
echo "   Next \`terraform apply\` re-creates EMR + MSK against existing data (~5 min)."
echo "   To fully destroy everything (including data), run \`bash teardown.sh\`."
