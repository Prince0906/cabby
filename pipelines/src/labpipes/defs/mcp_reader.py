"""S7: the MCP server's database user. query_zone_stats must not run as labadmin: create a user that can
read the marts schema and nothing else (decision: database role scope), with the RedshiftData resource.
The MCP server connects as it through the Data API's --db-user path (MCP_DB_USER, default lab_ro), so it
needs no password. Remember the mart is refreshed: grants must survive that.
"""

import dagster as dg

from ..resources import RedshiftData

READER = "lab_ro"  # the MCP server's MCP_DB_USER; a constant here, so no SQL below is built from outside input


@dg.asset(deps=["zone_hour_stats"], group_name="marts", kinds={"redshift"}, pool="redshift")
def mcp_reader(redshift: RedshiftData) -> dg.MaterializeResult:
    """The read-only user the MCP server's query_zone_stats runs as: it may SELECT marts.zone_hour_stats and
    nothing else. Re-run it whenever you want the grants re-stated; every statement is idempotent.

    The mart refresh does `delete from` and `insert into` on the same table and `create table if not exists`
    (contracts/marts.sql), so the table, and the grant on it, survive a refresh. Granting on one table, not on
    `all tables in schema marts`, keeps a future mart private until someone grants it on purpose."""
    exists = int(redshift.scalar(f"select count(*) from pg_user where usename = '{READER}'"))
    redshift.execute(
        # PASSWORD DISABLE: the user cannot log in with a password at all; the Data API's db-user path gets
        # temporary credentials from IAM instead.
        *([] if exists else [f"create user {READER} password disable"]),
        # Start from nothing on the schemas it must not see, then give back exactly one thing.
        f"revoke all on schema ingest from {READER}",
        f"revoke all on schema staging from {READER}",
        f"revoke all on all tables in schema ingest from {READER}",
        f"revoke all on all tables in schema staging from {READER}",
        f"revoke all on all tables in schema marts from {READER}",
        f"grant usage on schema marts to {READER}",
        f"grant select on marts.zone_hour_stats to {READER}",
    )
    return dg.MaterializeResult(metadata={"user": READER, "created": dg.MetadataValue.bool(not exists)})


PRIVILEGES_SQL = f"""
select has_table_privilege('{READER}', 'marts.zone_hour_stats', 'select') as read_mart,
       has_table_privilege('{READER}', 'marts.zone_hour_stats', 'insert') as insert_mart,
       has_table_privilege('{READER}', 'marts.zone_hour_stats', 'delete') as delete_mart,
       has_schema_privilege('{READER}', 'marts', 'create') as create_in_marts,
       has_schema_privilege('{READER}', 'ingest', 'usage') as use_ingest,
       has_schema_privilege('{READER}', 'staging', 'usage') as use_staging
"""


def is_read_only(p: dict) -> bool:
    """True when the privileges row says: SELECT on the mart, and nothing else it was checked for."""
    yes = lambda v: v in (True, "true", "t")  # noqa: E731 - the Data API returns booleans as true/false
    return yes(p["read_mart"]) and not any(yes(p[k]) for k in ("insert_mart", "delete_mart", "create_in_marts", "use_ingest", "use_staging"))


@dg.asset_check(asset=mcp_reader, blocking=True, description="lab_ro can read the mart, and cannot write it or read ingest or staging.")
def mcp_reader_is_read_only(redshift: RedshiftData) -> dg.AssetCheckResult:
    p = redshift.query(PRIVILEGES_SQL)[0]
    return dg.AssetCheckResult(passed=is_read_only(p), severity=dg.AssetCheckSeverity.ERROR, description=str(p),
                               metadata={k: dg.MetadataValue.text(str(v)) for k, v in p.items()})


grant_mcp_reader = dg.define_asset_job("grant_mcp_reader", selection=dg.AssetSelection.assets(mcp_reader),  # and its check
                                       description="Create or re-state the MCP server's read-only database user (S7).")
