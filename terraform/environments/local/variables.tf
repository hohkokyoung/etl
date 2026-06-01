variable "floci_endpoint" {
  description = "Floci (mock AWS) endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "local"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}
