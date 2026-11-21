# 003: Runner credentials: the host's instance profile, nothing stored in GitHub

Stage: S0 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request)

## Choice

The self-hosted runner on the app host (label `lab-app`) uses the host's own instance profile,
`LabInstanceProfile`. Its jobs get short-lived AWS credentials from the instance metadata, renewed
by AWS. No AWS keys or session tokens are stored as GitHub secrets for CI or CD. The runner is
registered by `scripts/register-runner.sh`, after fork-PR approval was set to "all external
contributors".

## Rejected alternative

Static Learner Lab session credentials copied into GitHub Actions secrets (`scripts/gh-secrets.sh`).

## Why

- Session credentials expire when the lab session ends (about 4 hours), so CD would break until I
  copied new ones, and they would sit in a public repository's secret store.
- Today's pipeline needs no AWS calls from GitHub: CI uses `GITHUB_TOKEN` (push to GHCR), and CD runs on
  the host. The static secrets are only needed later for the destroy workflow, which runs on a GitHub-hosted runner.
- The risk I accept: the runner holds the host's permissions, so a malicious workflow on it could use
  them. I limit that with three things: fork-PR approval for all external contributors, no workflow
  with a pull-request trigger may use a self-hosted runner (`tools/check_workflows.py` enforces it), and
  `cd.yaml` only starts after a push or dispatch on `main` of this repository. The lab does not let me
  create a narrower role, so `LabRole` is the smallest permission set available to me. I am the only
  collaborator and `main` is not branch-protected; that is acceptable now, and I would revisit it if
  anyone else gets write access.

## What would show this was wrong

- A job on the runner does something I did not write (Actions run history, or AWS activity I cannot explain).
- A pipeline step needs AWS access that the instance profile does not allow.
- The runner stops picking up jobs after a lab restart, so deploys silently stop.
- The registration token or runner setup turns out to be exposed somewhere I did not intend (the SSM command history holds it for an hour).
