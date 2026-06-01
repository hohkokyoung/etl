#!/usr/bin/env bash
# Creates all S3 buckets, SQS queues, SNS topics, and Glue DB via Floci.
set -e

ENDPOINT=http://localhost:4566
AWS="aws --endpoint-url $ENDPOINT --region us-east-1 --no-cli-pager"

echo "[floci] Creating S3 buckets..."
for bucket in raw-bronze processed-silver curated-gold terraform-state etl-scripts; do
  $AWS s3api create-bucket --bucket "$bucket" 2>/dev/null || true
  echo "  ✓ s3://$bucket"
done

echo "[floci] Setting lifecycle policies..."
$AWS s3api put-bucket-lifecycle-configuration \
  --bucket raw-bronze \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "expire-raw-180d",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "Expiration": {"Days": 180}
    }]
  }' 2>/dev/null || true

echo "[floci] Creating SQS queues..."
$AWS sqs create-queue --queue-name pipeline-events 2>/dev/null || true
$AWS sqs create-queue --queue-name dlq-failed-ingestion 2>/dev/null || true
echo "  ✓ SQS: pipeline-events, dlq-failed-ingestion"

echo "[floci] Creating SNS topics..."
$AWS sns create-topic --name etl-alerts 2>/dev/null || true
$AWS sns create-topic --name pipeline-completed 2>/dev/null || true
echo "  ✓ SNS: etl-alerts, pipeline-completed"

echo "[floci] Creating Glue database..."
$AWS glue create-database --database-input '{"Name": "etl_lake", "Description": "ETL data lake catalog"}' 2>/dev/null || true
echo "  ✓ Glue DB: etl_lake"

echo "[floci] Creating Athena workgroup..."
$AWS athena create-work-group \
  --name etl-workgroup \
  --configuration 'ResultConfiguration={OutputLocation=s3://curated-gold/athena-results/}' \
  2>/dev/null || true
echo "  ✓ Athena workgroup: etl-workgroup"

echo "[floci] Initialization complete."
