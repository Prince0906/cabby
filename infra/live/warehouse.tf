# S1: the Redshift warehouse (infra/modules/redshift), wired into the root module.
#
# One ra3.large node with LabRole attached as the default role, so COPY can say `IAM_ROLE default`.
# No ingress and no password: Dagster reaches it through the Redshift Data API as the database
# user `labadmin`. It sits in the same AZ as the hosts (var.availability_zone), and the subnet
# group reuses the 1a/1b subnets that RDS uses (data.aws_subnets.db in main.tf).
#
# Pause it at night: `aws redshift pause-cluster --cluster-identifier lab` (a running node costs
# about $13 overnight; a paused one bills storage only).

module "redshift" {
  source            = "../modules/redshift"
  subnet_ids        = data.aws_subnets.db.ids
  availability_zone = var.availability_zone
}

# Named in contracts/platform.yaml (terraform_outputs); scripts and the grader read them by name.
output "redshift_cluster_id" {
  value = module.redshift.cluster_id
}

output "redshift_database" {
  value = module.redshift.database
}
