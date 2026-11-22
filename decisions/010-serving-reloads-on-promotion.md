# 010: Serving picks up a new champion by `POST /reload` during promotion, not by restarting or polling

Stage: S3 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; edit the reasoning into your own words)

## Choice

`promote.yaml` moves the `champion` alias, then calls `deploy/web.sh reload champion` (`POST /reload` on
`serving-champion`). The container keeps answering with the old model until the new one has loaded, then swaps.
The workflow then checks three things: the reload returned the promoted version, `/model` shows it, and the app's
`/predict` answers with `"alias": "champion"` and that version. The job runs in the `production` environment, so a
reviewer approves it first.

Related choices made with it:

- Only the current `challenger`, or a version carrying a `previous_champion` tag (it was champion before), can be
  promoted. `eval_report` writes the metric tags on every trained version, gate or not, so tags alone prove nothing.
- The outgoing champion is recorded as the tag `previous_champion` on the new version (`none` for the first promotion).
  Naming that version in `promote.yaml` is how the model gets rolled back (S8).

## Rejected alternative

**Restart the serving container so it loads the new alias at start-up.** It needs no reload code. But the container
is out of Traefik's rotation while it loads (its `/model` health check fails), so the app quotes the static median
for those seconds, on every promotion. I also rejected **polling the alias** from inside serving: anyone moving the
alias would change live quotes with no check and no record, and the containers could switch at different moments.

## Why

- A promotion should change what riders get at one moment, after a reviewer has said yes, with nothing lost in between.
  Reload gives that; restart does not; polling gives up the "moment".
- The reload is a step inside the promotion run, so its result is checked there and shown in the run.
- Cost of this choice: two steps that can half-finish. If the alias moves and the reload or a check fails, MLflow
  says one version and serving runs another. The run fails, says so, and prints the outgoing champion to promote back.

## What would show this was wrong

- A promotion run ends with the alias and serving disagreeing, more than once.
- `/reload` takes long enough that the swap shows up in the app's latency or error rate.
- More than one serving container per alias, so one `/reload` no longer reaches them all.
