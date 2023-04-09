//create integration  with accountadmin  role bcz it's good practise

use role accountadmin;

//creating integration

create or replace storage integration aws_sf_data
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::*********' => put your own storage aws_role_arn from role properties in aws .
STORAGE_ALLOWED_LOCATIONS=('s3://snowflake-datastorage'); => This will locate your bucket location

//grant all types of privileges to sysadmin role 

GRANT USAGE ON INTEGRATION AWS_SF_DATA TO ROLE SYSADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

grant create stage on schema "ECOMMERCE_DB"."ECOMMERCE_DEV" to role sysadmin;


USE ROLE SYSADMIN;

//creating Database and schema and using them

USE SCHEMA ECOMMERCE_DB.ECOMMERCE_DEV;

DESCRIBE INTEGRATION AWS_SF_DATA;

//creating file format
CREATE OR REPLACE FILE FORMAT JSON_LOAD_FORMAT TYPE='JSON';

//creating External stage(aws s3 bucket)
CREATE OR REPLACE STAGE STG_LINEITEM_JSON_DEV
STORAGE_INTEGRATION=aws_sf_data
URL='s3://snowflake-datastorage/stream_dev/' This is folder location in s3 bucket
file_format=json_load_format;

list @STG_LINEITEM_JSON_DEV;

//CREATING FINAL TABLE

create or replace table lineitem as select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM limit 1;

select * from lineitem;

//CREATING RAW TABLE

create or replace table lineitem_raw_json (src variant);

//CREATING STREAM ON RAW TABLE

create or replace stream lineitem_stream on table lineitem_raw_json;

//CREATING TASK 

create or replace task lineitem_load_task
warehouse=compute_wh
schedule='1 minute'
when system$stream_has_data('lineitem_stream')
as
merge into lineitem as li
using
(
select 
        SRC:L_ORDERKEY as L_ORDERKEY,
        SRC:L_PARTKEY as L_PARTKEY,
        SRC:L_SUPPKEY as L_SUPPKEY,
        SRC:L_LINENUMBER as L_LINENUMBER,
        SRC:L_QUANTITY as L_QUANTITY,
        SRC:L_EXTENDEDPRICE as L_EXTENDEDPRICE,
        SRC:L_DISCOUNT as L_DISCOUNT,
        SRC:L_TAX as L_TAX,
        SRC:L_RETURNFLAG as L_RETURNFLAG,
        SRC:L_LINESTATUS as L_LINESTATUS,
        SRC:L_SHIPDATE as L_SHIPDATE,
        SRC:L_COMMITDATE as L_COMMITDATE,
        SRC:L_RECEIPTDATE as L_RECEIPTDATE,
        SRC:L_SHIPINSTRUCT as L_SHIPINSTRUCT,
        SRC:L_SHIPMODE as L_SHIPMODE,
        SRC:L_COMMENT as L_COMMENT
  
  from lineitem_stream
  where metadata$action='INSERT'

) as li_stg
on li.l_orderkey=li_stg.l_orderkey and li.L_PARTKEY = li_stg.L_PARTKEY and li.L_SUPPKEY = li_stg.L_SUPPKEY 

when matched then update
set 
 li.L_PARTKEY = li_stg.L_PARTKEY,
    li.L_SUPPKEY = li_stg.L_SUPPKEY,
    li.L_LINENUMBER = li_stg.L_LINENUMBER,
    li.L_QUANTITY = li_stg.L_QUANTITY,
    li.L_EXTENDEDPRICE = li_stg.L_EXTENDEDPRICE,
    li.L_DISCOUNT = li_stg.L_DISCOUNT,
    li.L_TAX = li_stg.L_TAX,
    li.L_RETURNFLAG = li_stg.L_RETURNFLAG,
    li.L_LINESTATUS = li_stg.L_LINESTATUS,
    li.L_SHIPDATE = li_stg.L_SHIPDATE,
    li.L_COMMITDATE = li_stg.L_COMMITDATE,
    li.L_RECEIPTDATE = li_stg.L_RECEIPTDATE,
    li.L_SHIPINSTRUCT = li_stg.L_SHIPINSTRUCT,
    li.L_SHIPMODE = li_stg.L_SHIPMODE,
    li.L_COMMENT = li_stg.L_COMMENT
    
    when not matched then insert
    (
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT
) 
values 
(
    li_stg.L_ORDERKEY,
    li_stg.L_PARTKEY,
    li_stg.L_SUPPKEY,
    li_stg.L_LINENUMBER,
    li_stg.L_QUANTITY,
    li_stg.L_EXTENDEDPRICE,
    li_stg.L_DISCOUNT,
    li_stg.L_TAX,
    li_stg.L_RETURNFLAG,
    li_stg.L_LINESTATUS,
    li_stg.L_SHIPDATE,
    li_stg.L_COMMITDATE,
    li_stg.L_RECEIPTDATE,
    li_stg.L_SHIPINSTRUCT,
    li_stg.L_SHIPMODE,
    li_stg.L_COMMENT
);

show tasks;

//RESUMING TASK BCZ BY DEFAULT TASK REMAIN IN SUSPENDED STATE
alter task lineitem_load_task resume;

//copy into lineitem_raw_json from @STG_LINEITEM_JSON_DEV on_error=ABORT_STATEMENT;

//CHECK ALL TABLE TO ENSURE BEFORE RUNNING THIS TASK ALL TABLE IS EMPTY
SELECT * FROM LINEITEM_RAW_JSON LIMIT 10;
SELECT count(*) FROM LINEITEM_STREAM LIMIT 10;
SELECT count(*) FROM LINEITEM LIMIT 10;
TRUNCATE LINEITEM;
truncate lineitem_raw_json;

//CREATING PIPE THAT WILL TRIGGER NOTICATION ONCE DATA COME IN BUCKET
create or replace pipe lineitem_pipe auto_ingest=True as
copy into lineitem_raw_json from @STG_LINEITEM_JSON_DEV;

show pipes;
