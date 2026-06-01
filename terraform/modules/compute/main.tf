variable "environment" { type = string }

# EKS cluster — mocked via Floci. In prod, this creates a real EKS cluster.
# The actual k8s workloads run in k3d locally (same configs, different endpoint).

resource "aws_eks_cluster" "etl" {
  name     = "etl-cluster-${var.environment}"
  role_arn = "arn:aws:iam::000000000000:role/eks-role"
  version  = "1.30"

  vpc_config {
    subnet_ids = ["subnet-00000001", "subnet-00000002"]
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_eks_node_group" "workers" {
  cluster_name    = aws_eks_cluster.etl.name
  node_group_name = "etl-workers"
  node_role_arn   = "arn:aws:iam::000000000000:role/eks-node-role"
  subnet_ids      = ["subnet-00000001", "subnet-00000002"]

  scaling_config {
    desired_size = 2
    max_size     = 5
    min_size     = 1
  }

  instance_types = ["t3.xlarge"]

  tags = { Environment = var.environment }
}

output "eks_cluster_name"     { value = aws_eks_cluster.etl.name }
output "eks_cluster_endpoint" { value = aws_eks_cluster.etl.endpoint }
