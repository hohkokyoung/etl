variable "environment" { type = string }

locals {
  buckets = ["raw-bronze", "processed-silver", "curated-gold", "terraform-state", "etl-scripts"]
}

resource "aws_s3_bucket" "lake" {
  for_each      = toset(local.buckets)
  bucket        = each.key
  force_destroy = true

  tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze_expiry" {
  bucket = aws_s3_bucket.lake["raw-bronze"].id

  rule {
    id     = "expire-raw-180d"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 180 }
  }
}

output "bronze_bucket_name" { value = aws_s3_bucket.lake["raw-bronze"].bucket }
output "silver_bucket_name" { value = aws_s3_bucket.lake["processed-silver"].bucket }
output "gold_bucket_name"   { value = aws_s3_bucket.lake["curated-gold"].bucket }
