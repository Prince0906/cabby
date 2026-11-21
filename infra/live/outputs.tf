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
