# 013: Three SLOs on /predict: 99.5% not an error, 95% under 0.25 s, 99.5% answered by the model

Stage: S5 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choices are the author's; rewrite the reasoning in your own words, you will be asked to defend it)

## Choice

| SLI (from `contracts/metrics.yaml`) | Objective over 30 days | Error budget |
|---|---|---|
| Share of `/predict` requests that are not 5xx | 99.5% | 0.5% |
| Share of `/predict` requests faster than 0.25 s | 95% | 5% |
| Share of quotes with `source="model"`, not `fallback` | 99.5% | 0.5% |

Each has two alerts, in `observability/rules/slo.rules.yaml`. A fast burn pages: burn rate 14.4 on both the 1h
and the 5m window, for 2 minutes. A slow burn opens a ticket: burn rate 6 on both the 6h and the 30m window,
for 15 minutes. A 4xx (like a 422 for an invalid zone) is the caller's mistake and does not count as an error.

## Rejected alternative

**Only availability and latency, with no model-answered SLO.** It is less to maintain. But when serving is
down the app still answers HTTP 200 from its static median, so both of those stay green while every rider gets
the same 12-minute quote. I also rejected **99.9%**: it allows about 43 minutes of failure a month, and a deploy
gap or a few minutes of serving trouble would use most of it.

## Why

- Each SLI catches a failure the others cannot: errors, slowness, and a silent fallback.
- 99.5% allows about 3.6 hours of failure a month: strict enough to mean something, loose enough to leave room
  for deploys (the blue-green switch has no gap) and for my own break test.
- 0.25 s sits just under the 300 ms the app waits for serving before it falls back, so a request over 0.25 s is
  one where the model is already close to being skipped. The objective is 95%, not 99.5%, because latency is
  noisier than an error count and a cold request is allowed to be slow.
- Two windows on a fast burn: the 1h window says the problem is real, the 5m window says it is still happening.
  A two-minute spike moves the 5m window only (the unit test shows it does not page).
- Cost of this choice: with no traffic the ratios have no value, so a total outage with zero requests does not
  page. `health.sh` and the status workflow have to catch that.

## Evidence from the break test (2026-11-22)

I stopped `serving-champion` at 12:21:39 UTC while sending one `/predict` a second. The app answered from the
fallback within 3 seconds. The Availability and Latency panels stayed at 100.00% the whole time (every answer
was a fast 200). The Model-answered panel fell to 27.29% (1 h window) and `PredictFallbackFastBurn` was firing
by about 12:29 UTC, so about 7 to 8 minutes after the break (scrape 15 s, rule evaluation about 1 min, a 5 m
rate window, `for: 2m`). `PredictFallbackSlowBurn` was pending. Starting the container again at 12:34:02 brought
the champion back in 7 seconds. This is the case the rejected alternative would have missed.

## What would show this was wrong

- The page alert fires and, on looking, no rider was affected (the threshold is too sensitive), or riders
  complained and nothing fired (too loose).
- The latency alert needs 72% of requests slow for an hour to page. If real slowness shows up as 20% slow
  requests, the latency SLO will not see it, and the objective or the threshold should change.
- The fallback SLO fires during every deploy or promotion, which would mean the reload is not as smooth as
  decision 010 assumes.
