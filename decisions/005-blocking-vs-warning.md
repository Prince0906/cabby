# 005: Block when the data is unusable, warn when it is only odd; every threshold comes from the four real months

Stage: S1 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request)

## Choice

The month's run is one Dagster run from `trip_files` to `zone_hour_stats`, so a blocking check stops
the later steps. Rule: a check blocks when the month cannot be used, and warns when it is only odd.

| Check | Blocks? | Limit | Observed in 2024-01..04 |
|---|---|---|---|
| `schema_contract` (on `trip_files`) | yes | any difference: missing, extra (even trailing), reordered, or retyped column | files match |
| `rows_match_footer` | yes | any difference from the file's row count | equal in all four months |
| `times_present` (null pickup or dropoff) | yes | more than 0.01% | 0% |
| `pickup_in_file_month` | yes | more than 0.01% | at most 0.0006% (9 to 23 rows) |
| `mart_filter_drop_share` | warns above 1%, blocks above 3% | see left | 0.46% to 0.50% |
| `rows_vs_history` (vs the median of the other loaded months) | warns above 30%, blocks above 50% | see left | -2% to +19% |
| `null_passengers` | warns above 25% only | | 4.7% to 11.9% |
| `duplicate_rows` | warns above 0.1% only | | 0 (1 row in February) |
| `median_duration_shift` | warns above 15% only | | about +3% a month (+6.7% for April) |

## Rejected alternative

Treating any schema difference as a warning, or warning only on a new trailing column. Parquet COPY maps
columns by position and fails on a column-count mismatch, so a trailing extra column would pass a
warning and then fail late, after the run started; a reordered column would load without an error and
put values in the wrong columns. I also rejected "any difference blocks" for the other checks: a normal
month varies (March has 19% more trips than February), and checks that stop good months teach people to
switch them off.

## Why

- The limits come from loading the four months into Redshift and measuring each one, then checking the
  same numbers again in the live run (for example the drop share came out at 0.46% to 0.50%). Each limit
  sits well above what normal months do, so a block means something is wrong.
- The mart filter drops bad rows silently (duration <= 0 or > 180 minutes, pickup zone outside 1..263),
  so the drop share is what makes them visible. It adds three groups, so it is a slight over-count.
- The row-count check compares with every OTHER loaded month, not only earlier ones, because the sensor
  starts all months together and the load order is not guaranteed. The first month to load has nothing to
  compare with and passes; the footer, null, date and drop-share checks do not need history, so they
  still guard it. Block at 50% leaves 2.5 times the largest swing seen (+19%) and catches a month with
  half its trips; at 60% a month 55% short would only warn.
- Two deliberate simplifications. Duplicates only warn, because a doubled load is already caught by
  `rows_match_footer`, and the duplicate share was measured on all 19 columns, not the smaller set
  `stg_trips` keeps. The unknown-zone check is folded into the drop share (about 0.4% every month, TLC
  uses ids 264 and 265 for "unknown"); the cost is that I lose the zone signal on its own.
- The sensor design supports the blocking. It uses the file's ETag as the run key, so a corrected
  re-upload gets a run and a repeated request does not; it is deployed stopped, because it marks a month
  as seen when it requests the run and a bug in the load SQL would have failed all four months with no
  retry. I proved January by hand first, then turned the sensor on.
- Evidence it blocks: a test runs the real job with three bad files (extra column, 2023 types, missing
  column); in each, `trip_files` runs, `raw_trips` does not, and no COPY is issued.

## What would show this was wrong

- A good month is blocked (a false alarm), or I catch myself widening a limit because of one month.
- A corrupt month reaches `zone_hour_stats` (the instructor's gameday month is the test). I have not yet
  seen a real corrupt month; so far only the unit tests exercised the blocking.
- The limits drift as months are added: the baseline is only four months and one year of data.
- A schema change I want to accept (like the 2025 trailing column) blocks every month; that is by design,
  and changing `contracts/trips.schema.yaml` is a reviewed decision of its own.
