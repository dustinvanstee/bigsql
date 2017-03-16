----------------------------------------------------
-- PNC Use Case 5
----------------------------------------------------
-- FD = Financial demo
-- AS PNCUSER
CREATE SCHEMA IF NOT EXISTS FD;
USE FD;

----------------------------------------------------
-- 1. Create a hadoop table based on a file that resides in HDFS
----------------------------------------------------
-- hdfs dfs -mkdir /user/pncuser/tickdata_small/  
-- hdfs dfs -put aapl_ticker.csv /user/pncuser/tickdata_small/  
-- hdfs dfs -put ibm_ticker.csv /user/pncuser/tickdata_small/  
-- hdfs dfs -rm /user/pncuser/tickdata/*
-- hdfs dfs -ls /user/pncuser/tickdata/
-- hdfs dfs -put WIKI_20170114.csv /user/pncuser/tickdata/  (1 min)
DROP TABLE IF EXISTS tickdata_ext;
CREATE EXTERNAL HADOOP TABLE tickdata_ext (
    symbol varchar(16),
    dt   date,
    open float,
    high float,
    low float,
    close float,
    volume float,
    ex_dividend float,
    split_ratio float,
    adj_open float,
    adj_high float,
    adj_low float,
    adj_close float,
    adj_volume float)
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  LOCATION '/user/pncuser/tickdata/';

SELECT count(*) FROM fd.tickdata_ext
-- 14.5M records
----------------------------------------------------
-- 2. Create the same table as parquet format and load (completed)
----------------------------------------------------
--https://developer.ibm.com/hadoop/2016/07/25/big-sql-load-loading-data-into-a-partitioned-table/
-- drop table IF EXISTS fd.tickdata_pq; (dont run)
CREATE HADOOP TABLE IF NOT EXISTS tickdata_pq (
    dt   date,
    open float,
    high float,
    low float,
    close float,
    volume float,
    ex_dividend float,
    split_ratio float,
    adj_open float,
    adj_high float,
    adj_low float,
    adj_close float,
    adj_volume float)
    STORED AS PARQUETFILE
    PARTITIONED BY 
      ( symbol varchar(16)
        )
    ;
    
----------------------------------------------------
-- 3. Load the data into parquet format
----------------------------------------------------

-- This kicks off a mapreduce job behind the scenes....
-- experiment with partitioning !
-- ~ 3mins to load
LOAD HADOOP USING FILE
  URL '/user/pncuser/tickdata/'
  WITH SOURCE PROPERTIES (
   'field.delimiter'=',',
   'ignore.extra.fields'='true',
   'field.indexes'='2,3,4,5,6,7,8,9,10,11,12,13,14,1'
   )
INTO TABLE fd.tickdata_pq
   APPEND
   WITH LOAD PROPERTIES (
    'max.rejected.records'=5000, 
    'num.map.tasks' = 20,
    'num.reduce.tasks' = 20
    )
;

SELECT count(*) FROM fd.tickdata_ext
SELECT count(*) FROM fd.tickdata_pq

--[pncuser@biginsights-sn ~]$ hdfs dfs -ls /apps/hive/warehouse/fd.db/tickdata_pq
--Found 3 items
--drwxrwxrwx   - pncuser hadoop          0 2017-02-18 20:55 /apps/hive/warehouse/fd.db/tickdata_pq/._biginsights_stats
--drwxrwx---   - pncuser hadoop          0 2017-02-18 20:55 /apps/hive/warehouse/fd.db/tickdata_pq/symbol=AAPL
--drwxrwx---   - pncuser hadoop          0 2017-02-18 20:55 /apps/hive/warehouse/fd.db/tickdata_pq/symbol=IBM

----------------------------------------------------
-- 3. BigSQL over HBASE
----------------------------------------------------

-- Declare a table that will be managed by HBASE 
-- Default encoding uses binary representation, using STRING for key as example for demo
--https://www.ibm.com/support/knowledgecenter/en/SSPT3X_4.2.0/com.ibm.swg.im.infosphere.biginsights.db2biga.doc/doc/biga_crhbasetbl.html
DROP TABLE IF EXISTS fd.tickdata_hbase;  --Table pre-built, dont drop for demo due to 3min load time

CREATE HBASE TABLE FD.tickdata_hbase  
( 
    symbol varchar(16),
    dt   date,
    open float,
    high float,
    low float,
    close float,
    volume float,
    ex_dividend float,
    split_ratio float,
    adj_open float,
    adj_high float,
    adj_low float,
    adj_close float,
    adj_volume float
)
  COLUMN MAPPING
(
    key                      mapped by (dt,symbol  ) ENCODING STRING, 
    cf_data:cq_open          mapped by (open       ) ENCODING STRING, 
    cf_data:cq_high          mapped by (high       ), 
    cf_data:cq_low           mapped by (low        ), 
    cf_data:cq_close         mapped by (close      ), 
    cf_data:cq_volume        mapped by (volume     ), 
    cf_data:cq_ex_dividend   mapped by (ex_dividend), 
    cf_data:cq_split_ratio   mapped by (split_ratio), 
    cf_data:cq_adj_open      mapped by (adj_open   ), 
    cf_data:cq_adj_high      mapped by (adj_high   ), 
    cf_data:cq_adj_low       mapped by (adj_low    ), 
    cf_data:cq_adj_close     mapped by (adj_close  ), 
    cf_data:cq_adj_volume    mapped by (adj_volume )
);

-- Load Data into HBASE via map reduce
-- Load Documentations
-- https://www.ibm.com/support/knowledgecenter/en/SSPT3X_4.2.0/com.ibm.swg.im.infosphere.biginsights.db2biga.doc/doc/biga_load_from.html
-- 3min run 
LOAD HADOOP using file url '/user/pncuser/tickdata_small/'
   WITH SOURCE PROPERTIES ('field.delimiter'=',') INTO TABLE FD.tickdata_hbase
   WITH LOAD PROPERTIES (
    'max.rejected.records'=5000, 
    'num.map.tasks' = 20,
    'num.reduce.tasks' = 20
    );

select count(*) from FD.tickdata_hbase;
SELECT symbol,close,dt FROM FD.tickdata_hbase WHERE symbol='AAPL' FETCH FIRST 10 ROWS ONLY;

--hbase shell
--list
-- scan 'fd.tickdata_hbase', {COLUMNS => ['cf_data:cq_open'], LIMIT => 10}  -- (string rep)
-- scan 'fd.tickdata_hbase', {COLUMNS => ['cf_data:cq_close'], LIMIT => 10}  -- (binary rep)


----------------------------------------------------
-- 4. BigSQL over HBASE (existing database in HBASE)
----------------------------------------------------
-- table already pre-created in hbase
-- create 'fd.tickdata_hbase_manual', 'cf_data'
-- put 'fd.tickdata_hbase_manual', '01-01-2017,AAPL', 'cf_data:open', 500
-- put 'fd.tickdata_hbase_manual', '01-01-2017,AAPL', 'cf_data:close', 505
-- put 'fd.tickdata_hbase_manual', '01-01-2017,IBM', 'cf_data:open', 100
-- put 'fd.tickdata_hbase_manual', '01-01-2017,IBM', 'cf_data:close', 105
-- scan 'fd.tickdata_hbase_manual'

-- Create a table that points to existing HBASE table
DROP TABLE IF EXISTS fd.tickdata_hbase_manual; 
CREATE EXTERNAL HBASE TABLE FD.tickdata_hbase_manual
( 
    dt_symbol varchar(30),
    open float,
    close float
)
COLUMN MAPPING
(
  key                   mapped by (dt_symbol)   ENCODING STRING, 
  cf_data:open          mapped by (open       ) ENCODING STRING, 
  cf_data:close         mapped by (close      ) ENCODING STRING
);

-- Query results using BigSQL
select * from FD.tickdata_hbase_manual;

