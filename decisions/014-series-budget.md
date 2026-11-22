# 014: Send only a keep-list of host and container metrics; keep the services whole

Stage: S5 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; rewrite the reasoning in your own words, you will be asked to defend it)

## Choice

`observability/alloy/budget.alloy` sends only the series a dashboard, an SLO or an incident would read:

| Block | Kept | Also |
|---|---|---|
| `host` | `up`, `node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`, `node_memory_MemTotal_bytes`, `node_filesystem_avail_bytes`, `node_filesystem_size_bytes`, `node_load1`, `node_network_receive_bytes_total`, `node_network_transmit_bytes_total` | drops the `veth*`, `br-*` and `docker0` network devices first |
| `containers` | `up`, `container_memory_working_set_bytes`, `container_cpu_usage_seconds_total`, `container_start_time_seconds` | drops the labels `id` and `image` |
| `services` | everything (the app, serving and Traefik, about 330 series) | nothing dropped |

A `keep` rule drops every series whose name does not match, so a new metric family is not sent until it is
added to the list.

## Rejected alternative

**Drop only the noise I could see** (`container_fs_*`, `node_scrape_collector_*` and the veth devices: my first
version of this record). It gave about 1,790 series, 18% of the budget, and it worked. But it leaves the other
hundreds of metric names nobody reads, and any new family a library adds would be sent without anyone choosing
it. I also considered keeping only the nine metrics the dashboard uses: too little to debug with.

## Why

- Before any rule the two hosts sent 3,337 series; 90% came from cAdvisor and node_exporter, and about 10% from
  the lab's own services. After the first version it was about 1,790, and Grafana listed 467 metric names, most
  of which I would never open.
- The keep-list is what a person needs to answer "is it up, is it busy, is it out of memory or disk, did a
  container restart": the metrics the dashboard reads, plus CPU, load and network totals.
- `id` and `image` change on every deploy (the cgroup path and the image tag), so dropping them stops each
  deploy from minting new container series. `name` stays the same and is the only label the dashboard reads.
- The veth devices are dropped before the keep-list because the network totals are kept for every device left.
- The S6 LLM host adds roughly 1,500 series without this rule (my estimate), and S8's canary adds series per
  model version; the free tier is 10,000 for all hosts together.
- Cost of this choice: a metric I did not list is gone, for example a container's disk I/O or TCP statistics.
  Adding it back is one edit and a redeploy.

## What would show this was wrong

- An incident where the answer was in a dropped family, and I could not see it.
- After the three deploys that S5 step 3 asks for, the count still grows with each deploy: another label changes
  per deploy and needs a `labeldrop`.
- A dashboard panel or an SLO that goes empty after this change: a metric or label it needs is not on the list.
