# 006: Sort on pickup time, let Redshift choose the distribution (AUTO)

Stage: S1 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request)

## Choice

`ingest.raw_trips` is declared with `diststyle auto` and `sortkey (tpep_pickup_datetime)`. The given
`staging.stg_trips` already sorts on `pickup_at`, and I kept it. Redshift chose the layout of the rest.
What the cluster reports after loading all four months:

| Table | Rows | Size | Distribution | Sort key |
|---|---|---|---|---|
| `ingest.raw_trips` | 13,069,067 | 1,244 MB | AUTO(EVEN) | `tpep_pickup_datetime` |
| `staging.stg_trips` | 13,069,067 | 646 MB | AUTO(EVEN) | `pickup_at` |
| `marts.zone_hour_stats` | 5,801 | 26 MB | AUTO(EVEN) | AUTO |
| `ingest.taxi_zones` | 265 | 7 MB | AUTO(ALL) | AUTO |

## Rejected alternative

A KEY distribution on the pickup zone, so that trips of one zone sit on the same slice for the
per-zone mart. I did not run it; I rejected it by reasoning. A few zones carry a large share of trips
(JFK is zone 132, with tens of thousands of trips in single hours), so most rows could pile onto a few
slices and leave the others idle. That is the skew a distribution key can cause.

## Why

- One `ra3.large` node and about 13 million rows: the cluster has no other nodes to move data between,
  so a bad distribution costs speed at most, and AUTO is the default Redshift recommends.
- Time is the natural filter for trips (monthly loads, per-month deletes and checks), so the sort key is
  the pickup time. For the mart the work is a group by zone and hour, which no single key helps much.
- Evidence: the layout was used for four monthly loads and nine checks per month; a full run of one month
  takes about a minute, and the mart rebuild is quick. I did not benchmark other layouts, so I cannot
  claim this one is the best.
- Honest limit: both large tables report 100% "unsorted". New rows are only sorted when Redshift's
  background vacuum runs, so right now the sort key is not helping yet. It does no harm at this size.

## What would show this was wrong

- Queries or monthly runs get noticeably slower as more months arrive.
- The table report shows a high `skew_rows` (it is empty at the moment) or the unsorted share does not
  fall after the background vacuum.
- A later stage needs a join on the pickup zone that is slow, which would be a reason to look at a key
  again with a measurement.
