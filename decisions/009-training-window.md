# 009: Train on the three months before the latest month, test on the latest month

Stage: S2 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request; the window is the author's to confirm)

## Choice

The holdout is the latest month in `staging.trip_features`, and the training window is the three months
just before it (`TRAIN_MONTHS = 3` in `training.py`). All of them must be present, with no gap, or the
run fails. Today that means training on **2024-01..2024-03** (9,452,562 trips) and testing on
**2024-04** (3,476,146 trips). The zone-pair distance lookup comes from the window only.

The holdout moves: when 2024-05 lands, the next run trains on 2024-02..04 and tests on 2024-05. A run's
config can name a different holdout month (`training_set: holdout_month`), for example to backtest.

## Rejected alternative

**A random split across all four months** (say 80/20). Serving always predicts trips in the future, so
a random split tests something serving never does. Trips from the same days, weather and events would
sit on both sides of the split, and the score would come out better than real life. The month after
the window is the honest test, because it has the same gap that serving has.

I also rejected **every month available** (a growing window). Durations change through the year: the
median went from 11.6 minutes in January to 12.8 in April, about +3% a month. A growing window keeps
the oldest pattern forever, and it slowly fills the platform host's 8 GB as months add up. With four
months loaded, it would also be the same as three months today.

## Why

- **Three months is enough data for the zone pairs.** The window has 36,398 zone pairs and covers all
  260 pickup zones seen in April. Only 3,797 April trips (0.11%) have a pair the window never saw, and
  they fall back to the pickup zone's distance. One month would see fewer pairs.
- **It is recent enough.** Backtesting v1 with the same rule gave 3.901 MAE on March and 3.956 on April.
  The score holds from one fold to the next, so three months is not stale.
- **It fits.** 9.5M rows of six columns train in one Dagster run on the t4g.large, alongside the rest
  of the platform.
- It matches `drift.py`: before there is a champion, drift compares each month with the three months
  before it (`TRAIN_MONTHS = 3`).
- Cost: one holdout month decides the gate. A strange month (a holiday, a storm) can let a lucky model
  through or block a good one.

## What would show this was wrong

- A backtest with a 6-month window (once enough months are loaded) beats the 3-month one on the same
  holdouts, slices included.
- `drift_report` shows PSI over 0.2 within the window itself. Then the months inside it are too
  different to mix, and the window should be shorter.
- The gate's verdict flips when I re-run with the holdout one month earlier. Then one month is too
  small a test, and the gate should look at two holdout months.
- A training run runs out of memory on the platform host as the data grows.
