/*########################
# setting up the context 
########################*/

use role SYSADMIN;

create database virtuslab_db;
create schema virtuslab_db.sch_util;

use database virtuslab_db;
use schema sch_util;

/*########################
# setting up the environment
# creating file formats, stage
########################*/


CREATE OR REPLACE FILE FORMAT CSV
	NULL_IF = ('NULL', 'null', '\\N')
    SKIP_BLANK_LINES = TRUE
    SKIP_HEADER = 1
COMMENT='parse comma-delimited data';


CREATE OR REPLACE FILE FORMAT JSON
	TYPE = JSON
	NULL_IF = ()
COMMENT='parse JSON data';


  create or replace stage stg_virtuslab
  url = 's3://virtuslab' 
  credentials=(aws_key_id='☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' aws_secret_key='☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺');
                  
  
  list @stg_virtuslab;
  list @stg_virtuslab pattern='.*.csv*'  ;
  list @stg_virtuslab pattern='.*.json*'  ;;  
  
   
 select $1, $2, $3  from  @stg_virtuslab (pattern => '.*customers.csv*');
  select $1, $2, $3, $4  from  @stg_virtuslab (pattern => '.*products.csv*');
 select $1 from  @stg_virtuslab (file_format => 'JSON' ,pattern => '.*.json*');
 
/*########################
# setting up the environment
# creating target tables, streams
########################*/

 
create schema virtuslab_db.sch_dim;
create schema virtuslab_db.sch_trs;
 
create table sch_dim.customers_raw (customer_id varchar, loyalty_score integer, load_tmpstmp timestamp);
create table sch_dim.customers (customer_id varchar, loyalty_score integer, from_tmpstmp timestamp, to_tmpstmp timestamp, active boolean);

create table sch_dim.products_raw (product_id varchar, product_description varchar, product_category varchar,  load_tmpstmp timestamp);
create table sch_dim.products (product_id varchar, product_description varchar, product_category varchar, from_tmpstmp timestamp, to_tmpstmp timestamp, active boolean);

create table sch_trs.transactions_raw (json variant, load_tmpstmp timestamp);

create transient table sch_trs.transactions_etl (customer_id varchar, product_id varchar, price number, date_of_purchase timestamp, load_tmpstmp timestamp);

create table sch_trs.transactions like sch_trs.transactions_etl;

create stream sch_dim.s_customers on table sch_dim.customers_raw;
create stream sch_dim.s_products on table sch_dim.products_raw;
create stream sch_trs.s_transactions on table sch_trs.transactions_raw;

select SYSTEM$STREAM_HAS_DATA('sch_dim.s_customers');

/*########################
# initial load
########################*/

         
create or replace procedure sch_util.sp_copy_from_s3 (source_stage varchar, target varchar, file_format varchar, pattern varchar, mode varchar)
  RETURNS VARCHAR(16777216)
  LANGUAGE JAVASCRIPT
  COMMENT = 'use qualified object names'
  EXECUTE AS OWNER
as $$

    var sql_cmd_trunc = "truncate table " + TARGET;
    var sql_cmd_copy_customers = "copy into " + TARGET + " from (select $1, $2, current_timestamp() from @" + SOURCE_STAGE + ") file_format = (format_name = " + FILE_FORMAT + ") pattern = '" + PATTERN + "' on_error = 'CONTINUE'";
    var sql_cmd_copy_products =  "copy into " + TARGET + " from (select $1, $2, $3, current_timestamp() from @" + SOURCE_STAGE + ") file_format = (format_name = " + FILE_FORMAT + ") pattern = '" + PATTERN + "' on_error = 'CONTINUE'";
    var sql_cmd_copy_transactions = "copy into " + TARGET + " from (select $1, current_timestamp() from @" + SOURCE_STAGE + ") file_format = (format_name = " + FILE_FORMAT + ") pattern = '" + PATTERN + "' on_error = 'CONTINUE'";

    if (MODE == "RELOAD") {
        try {
          snowflake.execute (
              {sqlText: sql_cmd_trunc}
              );
        }
        catch (err)  {
            return 'Error during truncate'
        }
    }



    if (TARGET == "SCH_DIM.CUSTOMERS_RAW") {
        try {
          snowflake.execute ({sqlText: sql_cmd_copy_customers});
        }
        catch (err)  {return 'Error during loading customers files'}
    }

    if (TARGET == "SCH_DIM.PRODUCTS_RAW") {
        try {
          snowflake.execute ({sqlText: sql_cmd_copy_products});
        }
        catch (err)  {return 'Error during loading producsts files'}
    }
    
    
    if (TARGET == "SCH_TRS.TRANSACTIONS_RAW") {
        try {
            snowflake.execute ({sqlText: sql_cmd_copy_transactions});
        }            
        catch (err)  {return 'Error during loading trnasaction files'}
    }    
    
   
    return 'Finished OK'


$$;


-- more generic one:
create or replace procedure sch_util.sp_copy_from_s3 (source_stage varchar, target varchar, file_format varchar)  
  RETURNS VARCHAR(16777216)
  LANGUAGE JAVASCRIPT
  COMMENT = 'use qualified object names'  
  EXECUTE AS OWNER

as $$

    var sql_cmd_delete = "delete from " + TARGET_TABLE
    var sql_cmd_trunc = "truncate table " + TARGET;
    var sql_cmd_copy = "copy into " + TARGET + " from @" + SOURCE_STAGE + " file_format = (format_name = " + FILE_FORMAT + ") on_error = 'CONTINUE'";

    if (MODE == "RELOAD") {
        try {
          snowflake.execute ({sqlText: sql_cmd_trunc});
        }
        catch (err)  {return 'Error during truncate'}
    }


    if (MODE == "DELTA") {
        try {
          snowflake.execute ({sqlText: sql_cmd_delete});
        }
        catch (err)  {return 'Error during delete'}
    }

    try {
      snowflake.execute ({sqlText: sql_cmd_copy});
    }
    catch (err)  {return 'Error during copy'}   
    
   
    return 'Finished OK'

$$;


/* -- test load
call sch_util.sp_copy_from_s3 ('SCH_UTIL.STG_VIRTUSLAB', 'SCH_DIM.CUSTOMERS_RAW', 'SCH_UTIL.CSV', '.*customers.csv*', 'INITIAL');
call sch_util.sp_copy_from_s3 ('SCH_UTIL.STG_VIRTUSLAB', 'SCH_DIM.PRODUCTS_RAW', 'SCH_UTIL.CSV', '.*products.csv*', 'INITIAL');
call sch_util.sp_copy_from_s3 ('SCH_UTIL.STG_VIRTUSLAB', 'SCH_TRS.TRANSACTIONS_RAW', 'SCH_UTIL.JSON', '.*.json*', 'INITIAL');

select * from "VIRTUSLAB_DB"."SCH_DIM"."CUSTOMERS_RAW";
select * from "VIRTUSLAB_DB"."SCH_DIM"."PRODUCTS_RAW";
select * from "VIRTUSLAB_DB"."SCH_TRS"."TRANSACTIONS_RAW";

truncate  "VIRTUSLAB_DB"."SCH_DIM"."CUSTOMERS_RAW";
truncate  "VIRTUSLAB_DB"."SCH_DIM"."PRODUCTS_RAW";
truncate  "VIRTUSLAB_DB"."SCH_TRS"."TRANSACTIONS_RAW";
*/



/*########################
# setting up a weekly delta load
########################*/


CREATE WAREHOUSE WH_TASKER 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
         WAREHOUSE_TYPE = 'STANDARD' 
         AUTO_SUSPEND = 30 -- suspend fast as it's only used for tasks
         AUTO_RESUME = TRUE 
         MIN_CLUSTER_COUNT = 1 
         MAX_CLUSTER_COUNT = 2 
         SCALING_POLICY = 'STANDARD';
         


create or replace procedure sch_util.sp_virtuslab_copy ()
RETURNS varchar
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
DECLARE 
    rezultat varchar default '';
    
BEGIN
  
    call sch_util.sp_copy_from_s3 ('SCH_UTIL.STG_VIRTUSLAB', 'SCH_DIM.CUSTOMERS_RAW', 'SCH_UTIL.CSV', '.*customers.csv*', 'INITIAL');
    call sch_util.sp_copy_from_s3 ('SCH_UTIL.STG_VIRTUSLAB', 'SCH_DIM.PRODUCTS_RAW', 'SCH_UTIL.CSV', '.*products.csv*', 'INITIAL');
    call sch_util.sp_copy_from_s3 ('SCH_UTIL.STG_VIRTUSLAB', 'SCH_TRS.TRANSACTIONS_RAW', 'SCH_UTIL.JSON', '.*.json*', 'INITIAL');
    rezultat := 'all files loaded succesfully to raw tables';
  
    return rezultat;   
  
  
EXCEPTION
   when statement_error then 
    return object_construct('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);


  when other then
    return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
  
END;  
  
$$;


create or replace task sch_util.t_virtuslab_weekly
warehouse = WH_TASKER
schedule='USING CRON 0 8 * * MON Europe/Warsaw'
as call sch_util.sp_virtuslab_copy ();



create or replace procedure sch_util.sp_slowly_changing_dims ()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
as $$
DECLARE
    rezultat varchar default '';
    kursor cursor for (select max(LOAD_TMPSTMP) from SCH_DIM.S_CUSTOMERS);
    kursor2 cursor for (select max(LOAD_TMPSTMP) from SCH_DIM.S_PRODUCTS);
    p1 timestamp;
    p2 timestamp;
    

BEGIN
    BEGIN TRANSACTION;
    
    OPEN kursor;
    FETCH kursor into p1;
    CLOSE kursor;
    
    MERGE INTO SCH_DIM.CUSTOMERS TGT using SCH_DIM.S_CUSTOMERS SRC
        ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
      WHEN MATCHED AND TGT.LOYALTY_SCORE != SRC.LOYALTY_SCORE then update
        SET TGT.TO_TMPSTMP = SRC.LOAD_TMPSTMP, TGT.ACTIVE = 0
      WHEN NOT MATCHED THEN insert (CUSTOMER_ID, LOYALTY_SCORE, FROM_TMPSTMP, TO_TMPSTMP, ACTIVE)
        values (src.CUSTOMER_ID,src.LOYALTY_SCORE,NULL,NULL,1);
    
    INSERT INTO SCH_DIM.CUSTOMERS (CUSTOMER_ID, LOYALTY_SCORE, FROM_TMPSTMP, TO_TMPSTMP, ACTIVE)
        SELECT src.CUSTOMER_ID,src.LOYALTY_SCORE,NULL,NULL,1 FROM SCH_DIM.S_CUSTOMERS SRC
        WHERE exists (select 1 from SCH_DIM.CUSTOMERS TGT where TGT.CUSTOMER_ID = SRC.CUSTOMER_ID AND TGT.LOYALTY_SCORE != SRC.LOYALTY_SCORE);
    
    UPDATE SCH_DIM.CUSTOMERS TGT 
        SET TO_TMPSTMP = :p1, ACTIVE = 0
        WHERE not exists (select 1 from SCH_DIM.S_CUSTOMERS SRC WHERE TGT.CUSTOMER_ID = SRC.CUSTOMER_ID);
        
    COMMIT;
    rezultat := 'Customers updated';
    
    BEGIN TRANSACTION;
    
    OPEN kursor2;
    FETCH kursor2 into p2;
    CLOSE kursor2;
    
    MERGE INTO SCH_DIM.PRODUCTS TGT using SCH_DIM.S_PRODUCTS SRC
        ON tgt.PRODUCT_ID = src.PRODUCT_ID
      WHEN MATCHED AND (TGT.PRODUCT_DESCRIPTION != SRC.PRODUCT_DESCRIPTION OR TGT.PRODUCT_CATEGORY != SRC.PRODUCT_CATEGORY) then update
        SET TGT.TO_TMPSTMP = SRC.LOAD_TMPSTMP, TGT.ACTIVE = 0
      WHEN NOT MATCHED THEN insert (PRODUCT_ID, PRODUCT_DESCRIPTION, PRODUCT_CATEGORY, FROM_TMPSTMP, TO_TMPSTMP, ACTIVE)
        values (src.PRODUCT_ID,src.PRODUCT_DESCRIPTION, src.PRODUCT_CATEGORY,NULL,NULL,1);
        
    INSERT INTO SCH_DIM.PRODUCTS (PRODUCT_ID, PRODUCT_DESCRIPTION, PRODUCT_CATEGORY, FROM_TMPSTMP, TO_TMPSTMP, ACTIVE)
        SELECT src.PRODUCT_ID,src.PRODUCT_DESCRIPTION, src.PRODUCT_CATEGORY,NULL,NULL,1 FROM SCH_DIM.S_PRODUCTS SRC
        WHERE exists (select 1 from SCH_DIM.PRODUCTS TGT where TGT.PRODUCT_ID = SRC.PRODUCT_ID AND (TGT.PRODUCT_DESCRIPTION != SRC.PRODUCT_DESCRIPTION OR TGT.PRODUCT_CATEGORY != SRC.PRODUCT_CATEGORY));
        
    UPDATE SCH_DIM.PRODUCTS TGT 
        SET TO_TMPSTMP = :p2, ACTIVE = 0
        WHERE not exists (select 1 from SCH_DIM.S_PRODUCTS SRC WHERE TGT.PRODUCT_ID = SRC.PRODUCT_ID);
        
    COMMIT;
    
    rezultat := rezultat || ' and Products updated';
    return rezultat;

EXCEPTION
   when statement_error then
    ROLLBACK;
    return object_construct('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);


  when other then
    return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
  
END;  

$$
;


create task sch_util.t_custmers_products_refresh
	warehouse = WH_TASKER
	after sch_util.t_virtuslab_weekly
    when (system$stream_has_data('SCH_DIM.S_PRODUCTS') or system$stream_has_data('SCH_DIM.S_CUSTOMERS'))
    as CALL sch_util.sp_slowly_changing_dims ();



create view sch_util.v_transactions_flatten
as

select 
JSON:customer_id::string customer_id,
x.value:product_id::string product_id,
x.value:price::number price,
JSON:date_of_purchase::timestamp date_of_purchase,
LOAD_TMPSTMP

from sch_trs.s_transactions,
lateral FLATTEN(INPUT => JSON:basket) x;


create or replace procedure sch_util.sp_transactions_etl ()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
as $$
DECLARE
    rows_inserted varchar default '';
    rezultat varchar default '';
    kursor cursor for select * from table(result_scan(last_query_id()));
    

BEGIN
    BEGIN TRANSACTION;
    TRUNCATE SCH_TRS.TRANSACTIONS_ETL;
    INSERT INTO SCH_TRS.TRANSACTIONS_ETL SELECT * FROM SCH_UTIL.V_TRANSACTIONS_FLATTEN;
    MERGE INTO SCH_TRS.TRANSACTIONS TGT USING SCH_TRS.TRANSACTIONS_ETL SRC
        ON TGT.CUSTOMER_ID = SRC.CUSTOMER_ID AND TGT.PRODUCT_ID = SRC.PRODUCT_ID AND TGT.DATE_OF_PURCHASE = SRC.DATE_OF_PURCHASE AND TGT.PRICE = SRC.PRICE
     WHEN NOT MATCHED THEN INSERT (CUSTOMER_ID,PRODUCT_ID,DATE_OF_PURCHASE,PRICE,LOAD_TMPSTMP)
        values (SRC.CUSTOMER_ID,SRC.PRODUCT_ID,SRC.DATE_OF_PURCHASE,SRC.PRICE,SRC.LOAD_TMPSTMP);
   
   
    OPEN kursor;
    FETCH kursor into rows_inserted;

    COMMIT; 

    rezultat := 'Number of rows inserted into SCH_TRS.TRANSACTIONS: ' || rows_inserted ;
    return rezultat;

EXCEPTION
   when statement_error then 
    ROLLBACK;
    return object_construct('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);


  when other then
    return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
  
END;  

$$
;


create task sch_util.t_transactions_etl
	warehouse = WH_TASKER
	after sch_util.t_virtuslab_weekly
    when system$stream_has_data('SCH_DIM.S_TRANSACTIONS')
    as CALL sch_util.sp_transactions_etl ();


USE ROLE ACCOUNTADMIN;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

alter task sch_util.t_custmers_products_refresh resume;
alter task sch_util.t_transactions_etl resume;
alter task sch_util.t_virtuslab_weekly resume;



/*########################
# reporting
########################*/

create schema virtuslab_db.sch_rpt;


create view sch_rpt.v_customer_score
as
select 
tr.customer_id,
cs.loyalty_score,
tr.product_id,
pr.product_category,
count(distinct date_of_purchase) purchase_count

from SCH_TRS.TRANSACTIONS tr
inner JOIN SCH_DIM.CUSTOMERS cs ON tr.customer_id = cs.customer_id and tr.date_of_purchase between ifnull(cs.from_tmpstmp,tr.date_of_purchase) and ifnull(cs.to_tmpstmp,tr.date_of_purchase)
inner JOIN SCH_DIM.PRODUCTS pr ON tr.product_id = pr.product_id and tr.date_of_purchase between ifnull(pr.from_tmpstmp,tr.date_of_purchase) and ifnull(pr.to_tmpstmp,tr.date_of_purchase)

group by tr.customer_id,cs.loyalty_score,tr.product_id,pr.product_category;



create function sch_rpt.udtf_customer_score (start_date varchar, end_date varchar)
returns table (customer_id varchar, loyalty_score integer, product_id varchar, product_category varchar, purchase_count integer)
as
$$
select 
tr.customer_id,
cs.loyalty_score,
tr.product_id,
pr.product_category,
count(distinct date_of_purchase) purchase_count

from SCH_TRS.TRANSACTIONS tr
inner JOIN SCH_DIM.CUSTOMERS cs ON tr.customer_id = cs.customer_id and tr.date_of_purchase between ifnull(cs.from_tmpstmp,tr.date_of_purchase) and ifnull(cs.to_tmpstmp,tr.date_of_purchase)
inner JOIN SCH_DIM.PRODUCTS pr ON tr.product_id = pr.product_id and tr.date_of_purchase between ifnull(pr.from_tmpstmp,tr.date_of_purchase) and ifnull(pr.to_tmpstmp,tr.date_of_purchase)

where to_date(tr.date_of_purchase) >= to_date(start_date)
and to_date(tr.date_of_purchase) <= to_date(end_date)

group by tr.customer_id,cs.loyalty_score,tr.product_id,pr.product_category
$$;




/*########################
# test run
########################*/


CALL sch_util.sp_virtuslab_copy ();
CALL sch_util.sp_slowly_changing_dims ();
CALL sch_util.sp_transactions_etl ();

SELECT * FROM sch_rpt.v_customer_score;

SELECT * FROM table(sch_rpt.udtf_customer_score('2022-05-25','2022-05-25'));