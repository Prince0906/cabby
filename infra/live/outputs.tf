# S0: expose the outputs listed in contracts/runtime.yaml under terraform_outputs.
# scripts/register-runner.sh, scripts/catch-up.sh and the grader read them by those names.

output "app_public_ip" {
  value = module.host["app"].public_ip
}

output "app_instance_id" {
  value = module.host["app"].instance_id
}

output "platform_instance_id" {
  value = module.host["platform"].instance_id
}

output "llm_instance_id" {
  value = module.host["llm"].instance_id
}

output "rds_endpoint" {
  value = module.rds.endpoint
}

output "artifact_bucket" {
  value = module.artifacts.name
}

# S3: where serving (on the app host) reaches MLflow. Private address: it only works inside the VPC, and the
# platform security group admits port 5000 from the app host's group only (plus the notebook's, S2).
output "platform_private_ip" {
  value = module.host["platform"].private_ip
}

# S6: where the app and platform hosts reach the LLM host (chat 8080, embeddings 8081). Private address: the
# LLM security group admits those ports from the app and platform groups only. Used for the LLM_HOST variable.
output "llm_private_ip" {
  value = module.host["llm"].private_ip
}
