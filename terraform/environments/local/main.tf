terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Store state in Floci S3 (bootstrapped by init.sh before terraform init)
  backend "s3" {
    bucket                      = "terraform-state"
    key                         = "etl/local/terraform.tfstate"
    region                      = "us-east-1"
    endpoint                    = "http://localhost:4566"
    access_key                  = "test"
    secret_key                  = "test"
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    force_path_style            = true
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3         = var.floci_endpoint
    sqs        = var.floci_endpoint
    sns        = var.floci_endpoint
    iam        = var.floci_endpoint
    glue       = var.floci_endpoint
    athena     = var.floci_endpoint
    kinesis    = var.floci_endpoint
    rds        = var.floci_endpoint
    eks        = var.floci_endpoint
    cloudwatch = var.floci_endpoint
  }
}

# ── Modules ───────────────────────────────────────────────────────

module "storage" {
  source          = "../../modules/storage"
  environment     = "local"
}

module "messaging" {
  source          = "../../modules/messaging"
  environment     = "local"
}

module "analytics" {
  source          = "../../modules/analytics"
  environment     = "local"
  gold_bucket     = module.storage.gold_bucket_name
}

module "iam" {
  source          = "../../modules/iam"
  environment     = "local"
  bronze_bucket   = module.storage.bronze_bucket_name
  silver_bucket   = module.storage.silver_bucket_name
  gold_bucket     = module.storage.gold_bucket_name
}

module "compute" {
  source          = "../../modules/compute"
  environment     = "local"
}

# ── Outputs ───────────────────────────────────────────────────────

output "s3_buckets" {
  value = {
    bronze = module.storage.bronze_bucket_name
    silver = module.storage.silver_bucket_name
    gold   = module.storage.gold_bucket_name
  }
}

output "sqs_pipeline_events_url" {
  value = module.messaging.pipeline_events_url
}

output "eks_cluster_name" {
  value = module.compute.eks_cluster_name
}
