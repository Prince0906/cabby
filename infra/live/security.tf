# S0: security groups and rules.
#
# Decision point: who may reach the app (ingress scope)? Record it in decisions/.
# Traffic between hosts (ports in contracts/runtime.yaml):
#   app -> platform  MLflow
#   app, platform -> llm  chat and embeddings
#   app, platform -> rds  Postgres
# There is no SSH in this lab: use SSM Session Manager for a shell and port forwarding for UIs.

locals {
  groups = {
    app      = "App host: Traefik on port 80"
    platform = "Platform host: Dagster and MLflow"
    llm      = "LLM host: llama.cpp chat and embeddings"
    rds      = "RDS Postgres"
  }
}

# Four empty firewalls. Rules are separate resources so groups can refer to each other.
resource "aws_security_group" "this" {
  for_each    = local.groups
  name_prefix = "lab-${each.key}-"
  description = each.value
  vpc_id      = data.aws_vpc.default.id
}

# Egress: every group may talk out (image pulls, GitHub, SSM, S3).
resource "aws_vpc_security_group_egress_rule" "all" {
  for_each          = aws_security_group.this
  security_group_id = each.value.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# Ingress: the only public door. Port 80 on the app host, from ingress_cidrs.
resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each          = toset(var.ingress_cidrs)
  security_group_id = aws_security_group.this["app"].id
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  cidr_ipv4         = each.value
}

locals {
  # "to" accepts traffic on "port" from "from". Ports come from contracts/runtime.yaml.
  internal = {
    mlflow_from_app     = { to = "platform", from = "app", port = 5000 }
    chat_from_app       = { to = "llm", from = "app", port = 8080 }
    chat_from_platform  = { to = "llm", from = "platform", port = 8080 }
    embed_from_app      = { to = "llm", from = "app", port = 8081 }
    embed_from_platform = { to = "llm", from = "platform", port = 8081 }
    pg_from_app         = { to = "rds", from = "app", port = 5432 }
    pg_from_platform    = { to = "rds", from = "platform", port = 5432 }
  }
}

resource "aws_vpc_security_group_ingress_rule" "internal" {
  for_each                     = local.internal
  security_group_id            = aws_security_group.this[each.value.to].id
  referenced_security_group_id = aws_security_group.this[each.value.from].id
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
}
