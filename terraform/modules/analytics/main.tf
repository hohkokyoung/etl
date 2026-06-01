variable "environment"  { type = string }
variable "gold_bucket"  { type = string }

resource "aws_glue_catalog_database" "lake" {
  name        = "etl_lake"
  description = "ETL data lake catalog — bronze/silver/gold layers"
}

resource "aws_glue_crawler" "bronze_crawler" {
  name          = "etl-bronze-crawler"
  role          = "arn:aws:iam::000000000000:role/etl-glue-role"
  database_name = aws_glue_catalog_database.lake.name

  s3_target {
    path = "s3://raw-bronze/"
  }

  schedule = "cron(0 * * * ? *)"  # hourly

  tags = { Environment = var.environment }
}

resource "aws_glue_crawler" "gold_crawler" {
  name          = "etl-gold-crawler"
  role          = "arn:aws:iam::000000000000:role/etl-glue-role"
  database_name = aws_glue_catalog_database.lake.name

  s3_target {
    path = "s3://${var.gold_bucket}/"
  }

  schedule = "cron(30 * * * ? *)"

  tags = { Environment = var.environment }
}

resource "aws_athena_workgroup" "etl" {
  name = "etl-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${var.gold_bucket}/athena-results/"
    }
  }

  tags = { Environment = var.environment }
}

output "glue_database_name"   { value = aws_glue_catalog_database.lake.name }
output "athena_workgroup_name" { value = aws_athena_workgroup.etl.name }
