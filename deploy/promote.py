"""Promote a model version to `champion` (S3). The only code that moves that alias.

Run by .github/workflows/promote.yaml inside the serving-champion container, because that image already has
the MLflow client and MLFLOW_TRACKING_URI / MODEL_NAME set:

    docker exec -i -e VERSION=<version or empty> lab-serving-serving-champion-1 python - < deploy/promote.py

VERSION empty means the current challenger. Prints one JSON line on success; a refusal goes to stderr, exit 1.

Which versions may be promoted (decision made in S3):
  - the current `challenger`: the pipeline only moves that alias behind `beats_champion` (record 008), or
  - a version that carries `previous_champion`: it was champion before, so it passed the gate and an approval once.
  Never a version that merely has the tags: eval_report writes them for every trained version, gate or not.
Every version must also carry the contract's version_tags (contracts/mlflow.yaml).
"""

import json
import os
import sys

from mlflow import MlflowClient

REQUIRED_TAGS = ("training_window", "holdout_month", "features_version", "git_sha",
                 "mae_min", "p95_latency_ms", "champion_mae_min")


def refuse(why: str) -> None:
    print(f"promotion refused: {why}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    name = os.environ["MODEL_NAME"]
    client = MlflowClient()
    aliases = client.get_registered_model(name).aliases
    champion, challenger = aliases.get("champion"), aliases.get("challenger")

    version = os.environ.get("VERSION", "").strip() or challenger
    if not version:
        refuse("no version named and there is no challenger")
    if version == champion:
        refuse(f"v{version} is already champion")

    tags = client.get_model_version(name, version).tags  # raises if the version doesn't exist
    missing = [t for t in REQUIRED_TAGS if t not in tags]
    if missing:
        refuse(f"v{version} is missing tags the pipeline writes: {', '.join(missing)}")
    if version != challenger and "previous_champion" not in tags:
        refuse(f"v{version} is neither the challenger (v{challenger}) nor a version that was champion before")

    # Record the outgoing champion first: if the alias move below fails, the note is harmless;
    # if it were written after and failed, a promotion would leave no way back.
    client.set_model_version_tag(name, version, "previous_champion", champion or "none")
    client.set_registered_model_alias(name, "champion", version)
    print(json.dumps({"promoted": version, "previous_champion": champion or "none", "was_challenger": version == challenger}))


main()
