-- @def // this is the table definition query for creating the table 

@def = select 'name' as Name, 20 as Value -- to define the table


-- @query() // this is the real query to run for the job

-- the following syntax will set up temp query

-- @temp(ab)

-- @day(ab)

-- @week(ab)

-- @fortnight(ab)

-- @month(ab)
@month(ab) -- will be translated as 
create table ctasyearcache.ab as 
WITH (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = 's3://my-athena-ctas-month-cache/20191030/'
    )

-- @quarter(ab)
@quarter(ab) -- will be translated as 
create table ctasyearcache.ab as 
WITH (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = 's3://my-athena-ctas-quarter-cache/20191030/'
    )

-- @year(ab)
@year(ab) -- will be translated as 
create table ctasyearcache.ab as 
WITH (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = 's3://my-athena-ctas-year-cache/20191030/'
    )

@year(ab) select * from somewhere;

-- run the export and delete it from temp database

-- @run -- use the @rum mark to split multiple queries the whole line with -- @run will be used as splitter
with
a as (select * from x)
@export('s3://bucketname/path/to/file/date(-2)/')
select * from a


-- @year(ab)
@year(ab) -- will be translated to the something similar to the following
create table ctasyearcache.ab as 
WITH (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = 's3://my-athena-ctas-year-cache/20191030/'
    )
-- @temp(ab)
@temp(ab) -- will be translated to the something similar to the following
create table ctastempcache.ab as 
WITH (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = 's3://my-athena-ctas-temp-cache/20191030/'
    )


-- a block to run sql
-- {
with
data1 as (select * from db.table1)
@year(ab) = -- this is cache table expires in a year
select * from data1
-- }
-- {
with
data1 as (select * from db.table1)
@temp(ab) = -- this is temporary table which will be deleted after this query
select * from @year(ab) -- this will just refer to the cache table name
-- }
-- {

@export('s3://bucketname/path/to/file/date(-2)/') =
select * from @temp(ab)

-- }

-- if( // the query should return one row and column with bool value

-- )
-- {

-- } 
-- elseif( // a query return true or false

-- ) {

-- }
-- else{

-- }

-- while( // a query return true or false

-- ) {

-- }

-- switch( // a query return string or int 

--) 
-- case(value1){ // run when case value1

--}
-- case(value2){ // run when case value1

--}
-- default{

-- }