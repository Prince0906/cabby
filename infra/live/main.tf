# S0: wire the root module.
#
# Given modules in ../modules:
#   host    one EC2 host per role in contracts/runtime.yaml (platform, app, llm)
#   rds     Postgres for Dagster, MLflow and pgvector
#   bucket  your artifact bucket (never aws_s3_bucket: see AGENTS.md)
#   secret  the lab/wiki secret container
#
# variables.tf has the inputs; outputs.tf must expose what runtime.yaml lists under
# terraform_outputs. Ask your agent to propose the wiring, then check it against those files.

# Look up the network AWS already gave us. "data" reads what exists; "resource" creates something.
data "aws_vpc" "default" {
  default = true
}

# The one subnet all three hosts use (us-east-1a, from variables.tf).
data "aws_subnet" "host" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = var.availability_zone
  default_for_az    = true
}

# Subnets RDS may use. RDS needs 2+ AZs; I keep it to 1a and 1b so it can't land somewhere odd like 1e.
data "aws_subnets" "db" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = ["us-east-1a", "us-east-1b"]
  }
}

locals {
  host_types = {
    platform = var.platform_instance_type
    app      = var.app_instance_type
    llm      = var.llm_instance_type
  }
}

module "host" {
  for_each           = local.host_types
  source             = "../modules/host"
  role               = each.key
  instance_type      = each.value
  subnet_id          = data.aws_subnet.host.id
  security_group_ids = [aws_security_group.this[each.key].id]
}

# Postgres for Dagster, MLflow and pgvector (S1+). Private: only the app and platform groups reach it.
module "rds" {
  source             = "../modules/rds"
  instance_class     = var.rds_instance_class
  subnet_ids         = data.aws_subnets.db.ids
  security_group_ids = [aws_security_group.this["rds"].id]
}

# Your own bucket: MLflow artifacts and training sets. Name becomes lab-artifacts-<account id>.
module "artifacts" {
  source = "../modules/bucket"
  name   = "lab-artifacts"
}

# An empty container for the wiki token (S6). The value is added later, outside Terraform.
module "wiki_secret" {
  source      = "../modules/secret"
  name        = "lab/wiki"
  description = "Ops wiki URL and per-student token (S6)"
}