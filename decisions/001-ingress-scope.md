# 001: Ingress scope: only my own address may reach port 80

Stage: S0 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request)

## Choice

The app host's security group allows inbound TCP 80 only from `ingress_cidrs`, which holds my current
public address as a single-address range (`/32`). The grader's address is not known yet: until it is
added to the list, the grader cannot reach the app. This is the known gap in this choice. Nothing else is open from outside: no port 22, and no Dagster port 3000
(UIs go through SSM port forwarding). Hosts reach each other by security-group reference, not by address.

## Rejected alternative

`0.0.0.0/0`: let the whole internet reach port 80.

## Why

- Least exposure: the only people who need to open the app are me (testing) and the grader. The CD job
  runs on the app host itself, so GitHub never connects in.
- The rule lasts the whole two days, not just S0. Later stages add `/assist` (LLM compute per request)
  and the MCP server, and in S0 Traefik sends every path to the app, so `/metrics` answers 200
  through port 80 (I saw this locally). Open to everyone, strangers could read it and, later, spend my
  compute.
- It is plain HTTP with no login, so the network rule is the only access control.
- Cost of this choice: my address can change. The fix is one line in `terraform.tfvars` and an `apply`
  that touches only a firewall rule.

## What would show this was wrong

- I keep losing access because my address changes (new Wi-Fi, hotspot): `/version` times out although
  the stack is healthy. Then I should look at a fixed address or SSM port forwarding instead of editing the list again.
- The grader or a teammate can't reach the app and I would otherwise be widening the range.
- Traefik's access log shows requests from addresses I did not allow (the rule is not doing what I think).
