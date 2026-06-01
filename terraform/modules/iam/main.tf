variable "environment"   { type = string }
variable "bronze_bucket" { type = string }
variable "silver_bucket" { type = string }
variable "gold_bucket"   { type = string }

resource "aws_iam_role" "etl_spark" {
  name = "etl-spark-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "s3_lake_access" {
  name = "etl-s3-lake-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.bronze_bucket}",
          "arn:aws:s3:::${var.bronze_bucket}/*",
          "arn:aws:s3:::${var.silver_bucket}",
          "arn:aws:s3:::${var.silver_bucket}/*",
          "arn:aws:s3:::${var.gold_bucket}",
          "arn:aws:s3:::${var.gold_bucket}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_s3" {
  role       = aws_iam_role.etl_spark.name
  policy_arn = aws_iam_policy.s3_lake_access.arn
}

resource "aws_iam_role" "etl_glue" {
  name = "etl-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

output "spark_role_arn" { value = aws_iam_role.etl_spark.arn }
output "glue_role_arn"  { value = aws_iam_role.etl_glue.arn }
