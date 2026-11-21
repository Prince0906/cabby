-- ingest.raw_trips for one month: the file's rows exactly as they landed, plus the file they came from.
-- ONE transaction (RedshiftData.execute runs several statements as one): if the COPY or the insert
-- fails, the delete is undone too, so a failed run leaves the previous load of this month in place and
-- a repeated run gives the same rows once.
-- Parquet COPY maps columns by POSITION, so it needs exactly the contract's columns in the contract's
-- order; the blocking schema_contract check on trip_files makes sure the file has them.
-- Distribution is AUTO (one small node; see decisions/). Sorted on pickup time like staging.stg_trips.
-- {month}, {file}, {uri} and {columns} are substituted after validation (batch statements take no parameters).
create schema if not exists ingest;
create table if not exists ingest.raw_trips (
  source_file varchar(64) not null,
  {columns}
) diststyle auto sortkey (tpep_pickup_datetime);
delete from ingest.raw_trips where source_file = '{file}';
create temp table raw_trips_load ({columns});
copy raw_trips_load from '{uri}' iam_role default format as parquet;
insert into ingest.raw_trips select '{file}', * from raw_trips_load;
