# 014: Drop container filesystem counters, node_exporter's own metrics and Docker's virtual network devices

Stage: S5 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; rewrite the reasoning in your own words, you will be asked to defend it)

## Choice

`observability/alloy/budget.alloy` drops three things and keeps everything else:

| Block | Dropped | Why |
|---|---|---|
| `containers` | `container_fs_.*` (about 16 families, 37-44 series each) | per-container filesystem counters; nothing in the dashboard or an SLO reads them |
| `host` | `node_scrape_collector_.*` (46 per host) | node_exporter reporting on itself |
| `host` | `node_network_.*` with `device` matching `veth.*`, `br-.*`, `docker0` | Docker gives each container a veth with a random name, so every deploy mints new series |
| `services` | nothing | the app, serving and Traefik are about 330 series, and they are what the dashboard, SLOs and S8 canary read |

Kept on purpose: every `lab_*` and `traefik_*` series, `node_cpu_seconds_total`, and
`container_memory_working_set_bytes` (the dashboard's).

## Rejected alternative

**An allowlist: keep only the nine metrics the dashboard uses, plus `lab_*`.** It would bring the count to
roughly 600-900. But when something unexpected happens (a disk filling, a container restarting in a loop) I would
have almost nothing to look at, and I cannot know today what an incident will need. I also considered dropping
only the veth devices (about 3,000 series left): it stops the deploy churn but keeps hundreds of series nobody
reads.

## Why

- Before any rule, the two hosts sent 3,337 series (Explore: `count by (host, job)`). 90% came from cAdvisor and
  node_exporter, and about 10% from the lab's own services.
- The biggest single block was `container_fs_*`: at least 569 of the app host's 894 cAdvisor series came from
  15 of its families alone.
- The S6 LLM host will add roughly 1,500 more (my estimate), and S8's canary adds series per model version. The
  free tier is 10,000 for all hosts.
- The label that changes on every deploy is the veth device name (and container ids in cAdvisor); dropping the
  veth series removes the main source of churn.
- Cost of this choice: I lose per-container disk I/O. A full disk would show on the host's filesystem metrics
  (kept), not per container.

## What would show this was wrong

- After the three deploys that step 3 asks for, the count is still growing with each deploy: another label
  changes per deploy, and it needs a `labeldrop`.
- An incident where the answer was in a dropped family (a container's disk writes, a veth's errors).
- The count after the rules is far from my estimate of 1,700-1,900, which would mean I misjudged what is in the
  families I never looked at (the other ~400 series of the app host's node_exporter).
