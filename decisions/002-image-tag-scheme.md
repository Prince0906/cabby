# 002: Image tag scheme: one immutable tag per commit, `sha-<first 7 of the commit>`

Stage: S0 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request)

## Choice

CI tags each image `ghcr.io/<owner>/<repo>/app:sha-<first 7 characters of the commit SHA>` (lowercase
path, as GHCR requires) and bakes the full SHA in as `GIT_SHA`. CD deploys exactly the tag of the CI run
it follows. A rollback would point at the previous commit's tag: CD prints the image that was running
before each deploy ("What is running now"), so the target is known before it is replaced. I have not
exercised a rollback in S0, so that part is a design intent, not something I have tested.

## Rejected alternative

`latest` (a moving tag, or one that is simply re-pushed on every build).

## Why

- A SHA tag names exactly one commit and never changes, so "what is running" has one answer.
  `/version` returns the full `git_sha` and the tag, and the first live deploy showed the commit I had
  just pushed, matching `git rev-parse HEAD`.
- `latest` moves: two hosts could both say `latest` and run different code, Compose may not re-pull it,
  and "go back to the previous latest" does not exist because the previous one is overwritten.
- CD checks out the same commit CI built, so the Compose file and the image always belong together.
- Cost of this choice: tags are not readable (no `v1.2`), so I read the commit log to know what a tag
  contains.

## What would show this was wrong

- Two different builds ever share a tag (7 characters collided), or `/version` and the tag disagree.
- A rollback fails because the old tag was removed from GHCR before I needed it.
- I need to deploy something that did not come through CI on `main` (a hotfix branch, a PR preview), which this scheme does not cover.
