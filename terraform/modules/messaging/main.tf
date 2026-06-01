variable "environment" { type = string }

resource "aws_sqs_queue" "pipeline_events" {
  name                       = "pipeline-events"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 60
  tags                       = { Environment = var.environment }
}

resource "aws_sqs_queue" "dlq" {
  name                      = "dlq-failed-ingestion"
  message_retention_seconds = 1209600  # 14 days
  tags                      = { Environment = var.environment }
}

resource "aws_sqs_queue_redrive_policy" "pipeline_events_rdp" {
  queue_url = aws_sqs_queue.pipeline_events.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sns_topic" "etl_alerts" {
  name = "etl-alerts"
  tags = { Environment = var.environment }
}

resource "aws_sns_topic" "pipeline_completed" {
  name = "pipeline-completed"
  tags = { Environment = var.environment }
}

resource "aws_kinesis_stream" "events" {
  name             = "etl-events-stream"
  shard_count      = 1
  retention_period = 24
  tags             = { Environment = var.environment }
}

output "pipeline_events_url" { value = aws_sqs_queue.pipeline_events.url }
output "etl_alerts_arn"      { value = aws_sns_topic.etl_alerts.arn }
