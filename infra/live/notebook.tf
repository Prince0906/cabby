# S2: the SageMaker notebook (infra/modules/notebook), wired into this root module, allowed to reach
# MLflow on the platform host (port 5000, contracts/runtime.yaml), and exposed as notebook_name.
# Set github_repo in terraform.tfvars (owner/name): the notebook clones your public repo.
#
# The notebook is for exploring only (decision record 007): it reads Redshift through the Data API and
# logs to its own MLflow experiment. It never registers a model and never sets an alias.
#
# Network: the notebook gets its own security group (no inbound). MLflow listens on the platform host's
# PRIVATE address, so the notebook uses that, and the platform group admits the notebook group on 5000
# (like it already admits the app group in security.tf).
# Cost: about $0.05 an hour while running. Stop it at night:
#   aws sagemaker stop-notebook-instance --notebook-instance-name lab

module "notebook" {
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.host.id
  repo_url  = "https://github.com/${var.github_repo}"

  # Written to ~/SageMaker/.lab.env on every start; the notebook's first cell reads it.
  env = {
    MLFLOW_TRACKING_URI = "http://${module.host["platform"].private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.redshift.cluster_id
    REDSHIFT_DATABASE   = module.redshift.database
    REDSHIFT_DB_USER    = "labadmin"
  }
}

# The platform group accepts MLflow traffic from the notebook group, and from nothing else new.
resource "aws_vpc_security_group_ingress_rule" "mlflow_from_notebook" {
  security_group_id            = aws_security_group.this["platform"].id
  referenced_security_group_id = module.notebook.security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
}

output "notebook_name" {
  value = module.notebook.name
}
