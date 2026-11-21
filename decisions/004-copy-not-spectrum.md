# 004: Load each month with COPY into Redshift, not Spectrum; delete-then-insert so a rerun is harmless

Stage: S1 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request)

## Choice

`raw_trips` loads each month's Parquet file into the table `ingest.raw_trips` with `COPY` (using the
cluster's default IAM role). One transaction does three things in order: delete that file's rows
(`source_file = '<file>'`), COPY the 19 file columns into a temp table, then insert them with the file
name added as `source_file`. The same transaction pattern is used by the given `stg_trips`. Code:
`sql/raw_trips.sql`, `defs/raw_trips.py`.

## Rejected alternative

Spectrum: leave the files in S3 and query them in place through an external table. I did not try it. I
rejected it on how it fits this pipeline, not on a benchmark.

## Why

- `zone_hour_stats` is rebuilt from every month on every run, and `stg_trips` reads a real
  `ingest.raw_trips` table. Local tables make each rebuild a scan of Redshift storage; Spectrum would
  scan S3 again each time and make me rewrite the given staging SQL.
- Evidence from a real load of 2024-01..04: each month (about 3 to 3.6 million rows, 50 to 60 MB of
  Parquet) loaded in 15 to 18 seconds. The four months are 13,069,067 rows, 1.2 GB in `raw_trips`.
- Safe to run twice, shown on the real stack: January was loaded twice (a manual run, then the sensor)
  and stayed at 2,964,624 rows. If the COPY or the insert fails, the delete is rolled back too, so a
  failed run leaves the previous load of that month in place.
- The Parquet file has no `source_file` column, so COPY goes through a temp table. COPY maps columns by
  position, so the column order is guaranteed by the blocking schema check (record 005), not by COPY.
- Cost of this choice: the data is stored twice (S3 and Redshift) and a month that changes has to be
  reloaded. The extra storage is small here.

## What would show this was wrong

- A month's load stops taking seconds and starts taking minutes, or the cluster runs short of disk, as
  more months arrive.
- I need to query files I have not loaded (for example an exploratory month), where Spectrum would save
  a load.
- A contract change (a new column, as in the 2025 files) forces me to reload every month, which would
  cost more than reading in place.
- Row counts after a run differ from the file's footer: the quality check `rows_match_footer` catches
  that, and it would mean the idempotent load is not doing what I think.
