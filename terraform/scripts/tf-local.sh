#!/usr/bin/env bash
# Bootstraps Floci and runs Terraform against the local environment.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/../.."

echo "[tf-local] Step 1: Ensure Floci is running..."
if ! curl -sf http://localhost:4566/_floci/health > /dev/null; then
  echo "  Floci not running. Start with: make up"
  exit 1
fi

echo "[tf-local] Step 2: Pre-create terraform-state bucket..."
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    s3api create-bucket --bucket terraform-state 2>/dev/null || true

echo "[tf-local] Step 3: terraform init..."
cd "$SCRIPT_DIR/../environments/local"
terraform init \
  -backend-config="endpoint=http://localhost:4566" \
  -reconfigure

echo "[tf-local] Step 4: terraform apply..."
terraform apply \
  -var="floci_endpoint=http://localhost:4566" \
  -auto-approve

echo "[tf-local] Done. Run 'terraform output' to see resource identifiers."
