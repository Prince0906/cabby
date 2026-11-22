# 011: A reviewer approves every blue-green switch, after the idle colour passes its smoke test

Stage: S3 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; edit the reasoning into your own words)

## Choice

CD deploys a commit to the idle colour, then checks it while it takes no rider traffic: `/version` reports the commit
CI built, `/predict` answers, and an invalid zone gets a 422. The switch job waits in the `production` environment
for the required reviewer. After the approval it checks the live colour hasn't changed and smoke-tests the idle
colour again, switches, and checks `/version`, `/predict` and the 422 through the live path. The old colour keeps
running. Rolling back is a separate manual workflow that switches colours without deploying, and has no approval step.

## Rejected alternative

**Switch automatically when the smoke test passes.** Every merge would reach riders in about a minute with no one
waiting. But the smoke test only covers a version, one prediction and one validation error. A build that passes it
and is still wrong would reach every rider until someone noticed and rolled back. I also considered automatic
revert after the switch and left it out, because it can only catch what the check can see.

## Why

- A switch changes what every rider gets, and nothing is lost by waiting: the tested build sits on the idle colour
  and the old one keeps serving. GitHub records who approved and when.
- Rollback needs no approval because it only returns to a colour that was live before, and a dispatch is already a
  human decision.
- Cost of this choice: every merge waits for me. A run waiting for approval keeps the `cd` concurrency slot, so a
  newer push queues behind it and I have to approve or reject the old run first. The approval can also be given
  long after the smoke test, which is why the switch job re-checks before it flips.

## What would show this was wrong

- I approve within seconds, without looking at anything. Then the gate is a ritual, and the smoke test should
  decide (with automatic revert).
- Merges wait hours for an approval, or a stale run gets approved by mistake.
- A bad build gets approved because the smoke test showed nothing wrong. Then the checks before the pause are too
  thin, and the approval is not the fix.
