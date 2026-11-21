# 007: The notebook is for exploring; everything that repeats runs in the pipeline, and only the pipeline moves `challenger`

Stage: S2 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request, from the author's draft)

## Choice

The SageMaker notebook is for looking at data and trying ideas. Anything that has to happen on every new
month goes into the pipeline (`training.py`, `feature_check.py`), and only the pipeline registers model
versions, tags them, and sets the `challenger` alias. The notebook never registers a model and never
sets an alias.

| Work | Where it runs |
|---|---|
| Looking at distributions, charts, quick queries | Notebook |
| Trying an idea on a 5% sample | Notebook |
| Building `trip_features`, and the `feature_contract` check | Pipeline |
| Training on the full training window (record 009) | Pipeline |
| Evaluating on the holdout month (record 009) | Pipeline |
| Registering the version and its seven tags in MLflow | Pipeline |
| The `beats_champion` gate (record 008) | Pipeline |
| Moving the `challenger` alias | Pipeline only |

## Rejected alternative

Training and registering models from the notebook. It is faster for one idea, but it is the path where
a model can reach an alias without passing a guard.

## Why

- **Reproducibility.** A notebook result can depend on the order the cells were run and on values left
  in memory. The pipeline runs the same steps in the same order each time, from a commit.
- **The guards always run.** In the pipeline `feature_contract` and `beats_champion` cannot be skipped.
  In a notebook nothing forces them to run.
- **Every pipeline version is traceable.** It carries the training window, holdout month, git commit and
  metrics as tags. The notebook does log to MLflow, but to its own experiment (`trip-duration-notebook`),
  and those runs have no commit, no guard result and no tags the grader reads.
- **The score that decides must come from the full data.** The notebook trains on a 5% sample, so its
  numbers differ from the full-data model. The comparison that promotes a version uses the pipeline's.
- **Work that lives only in a notebook is not reviewed.** It is not in git unless it is committed, and
  it disappears when the instance is deleted at teardown. (Stopping the instance at night does not erase
  its disk; that is not the reason.)
- Cost of this choice: turning a notebook idea into production means moving it into `training.py`,
  committing, pushing and letting the pipeline run. That slower path is the point.

## What I accept, and what is not locked

The rule "only the pipeline sets `challenger`" is enforced by how the code and the repo are written, not
by a technical lock. The notebook has to reach MLflow on port 5000 to log its own experiments (the
firewall rule in `infra/live/notebook.tf`), and this MLflow server has no authentication. So anyone with
the notebook, or any machine inside the network, could set an alias by hand. What protects against that
is the review and the audit trail: a version registered by hand would not carry the seven tags, and an
alias that moved without a matching Dagster run is visible.

## What would show this was wrong

- The `challenger` alias points at a version that has no matching `train_model` run in Dagster, or that
  is missing the tags in `contracts/mlflow.yaml`. That means the rule was bypassed.
- A model reaches serving whose score came from a notebook run.
- The pipeline path is so slow that I stop trying ideas in the notebook, and the explore step stops
  being useful.
- Someone needs a one-off experiment on the full data, which the notebook's sample cannot give.
