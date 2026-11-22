# 018: The MCP server reads one table, as a user that cannot log in with a password

Stage: S7 · Date: 2026-11-22 · Author: Prince0906 · Drafted by agent: yes (the choice is the author's; rewrite the reasoning in your own words, you will be asked to defend it)

## Choice

The MCP server's `query_zone_stats` runs as the Redshift user `lab_ro`, created by the `mcp_reader` asset
(`pipelines/src/labpipes/defs/mcp_reader.py`):

| On | `lab_ro` may |
|---|---|
| `marts.zone_hour_stats` | `SELECT` |
| schema `marts` | `USAGE` (no `CREATE`) |
| schemas `ingest` and `staging` and their tables | nothing |

It is created with `PASSWORD DISABLE`, so it cannot log in with a password at all: the Data API's `DbUser` path
gets short-lived credentials from IAM. A blocking asset check (`mcp_reader_is_read_only`) and the status workflow
both read the privileges from Redshift and fail if the user can write the mart or use `ingest` or `staging`.

## Rejected alternatives

- **Run as `labadmin`.** One less user, and the tool would work the first time, but a flaw in the tool would
  then have the rights to rewrite or drop anything in the warehouse.
- **`GRANT SELECT ON ALL TABLES IN SCHEMA marts`.** Simpler, but every future mart table would be readable by the
  assistant without anyone deciding so. I grant the one table the tool needs.

## Why

- Least privilege: the tool needs one table, so that is all it has. If its SQL or the model's arguments were ever
  wrong, the worst the user can do is read that table.
- The raw trips (`ingest`) and the cleaned trips (`staging`) hold the actual trip rows; the mart has only counts
  and percentiles per zone and hour. The assistant never needs the rows.
- The mart refresh is `delete from` and `insert into` the same table, with `create table if not exists`, so the
  table and its grant survive a refresh. The grant job is idempotent and can be re-run to restate the grants.
- The proof is in the warehouse, not in my words. On 2026-11-22 the status workflow reported `read_marts: true`,
  `write_marts: false`, `use_ingest: false`, `create_in_marts: false`, and the grant job's blocking check passed.
- The tool's SQL is two fixed statements: the zone id and the hour go in only as named parameters, and the
  metric picks a key in Python, so nothing from a tool call becomes SQL text.
- Cost of this choice: a new tool that needs another table fails until someone grants it on purpose.

## What would show this was wrong

- The status workflow or the check reports any privilege other than `SELECT` on the mart.
- A mart refresh that drops the table (a change to the DDL) and the grant is gone: `query_zone_stats` then fails
  with a permission error until the grant job is run again.
- The assistant needing a column or table the user cannot read, and the temptation to widen it.
