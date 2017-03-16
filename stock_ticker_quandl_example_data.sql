-- FD = Financial demo

-- MAC hints ...
-- Hit F5 TO refresh SCHEMA!
-- CNTL + ENTER runs a line
-- Option  + x  runs a batch of lines

CREATE SCHEMA IF NOT EXISTS FD;
drop table IF EXISTS fd.tickdata_pq;

-- Symbol,	Date,		Open      	High       	Low     	Close       Volume  		Ex-Dividend	Split Ratio  	Adj. Open  	
-- Adj. High  		Adj. Low  			Adj. Close              Adj. Volume
USE FD;

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
  LOCATION '/user/ibm/tickdata/';

GRANT ALL ON FD.tickdata_ext TO USER useribm;

SELECT count(*) FROM fd.tickdata_ext

drop table fd.tickdata_pq;
CREATE HADOOP TABLE IF NOT EXISTS tickdata_pq (
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
    STORED AS PARQUETFILE;

-- This kicks off a mapreduce job behind the scenes....
-- experiment with partitioning !
LOAD HADOOP USING FILE
  URL '/user/ibm/tickdata/'
  WITH SOURCE PROPERTIES (
   'field.delimiter'=',',
   'ignore.extra.fields'='true',
   'field.indexes'='1,2,3,4,5,6,7,8,9,10,11,12,13,14'
   )
INTO TABLE tickdata_pq
   APPEND
;

drop TABLE IF EXISTS fd.tickdata_pq2;

CREATE HADOOP TABLE IF NOT EXISTS fd.tickdata_pq2 (
    dt date,
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
      (symbol varchar(16)
        )
    ;
    
  -- INT(dt)/10000 AS year 
    
    partitioned BY ( 
      INT(cast(trans_ts AS date))/10000 AS year_part,
      INT(cast(trans_ts AS date))/100 AS month_part,
    )
    
    -- PARTITIONED BY (P VARCHAR(20));
--NOT WORKING.  Hive null partition only ... for some reason load doesnt perform the casting..
-- This kicks off a mapreduce job behind the scenes....
LOAD HADOOP USING FILE
  URL '/user/ibm/tds/'
  WITH SOURCE PROPERTIES (
   'field.delimiter'=',',
   'ignore.extra.fields'='true',
   'field.indexes'='2,3,4,5,6,7,8,9,10,11,12,13,14,1'
   )
INTO TABLE fd.tickdata_pq2
   APPEND
;

-- WORKING (SMALL)
INSERT INTO fd.tickdata_pq2 ( symbol, dt , open ,high ,low ,close ,volume ,ex_dividend ,split_ratio ,adj_open ,adj_high ,adj_low ,adj_close ,adj_volume ) 
  SELECT *
  FROM fd.tickdata_ext  WHERE symbol LIKE '%IB%' ;
-- WORKING (ALL)
INSERT INTO fd.tickdata_pq2 ( symbol, dt , open ,high ,low ,close ,volume ,ex_dividend ,split_ratio ,adj_open ,adj_high ,adj_low ,adj_close ,adj_volume ) 
  SELECT *
  FROM fd.tickdata_ext WHERE symbol LIKE 'W%' OR symbol LIKE 'X%'  OR symbol LIKE 'Y%'  OR symbol LIKE 'Z%';
  -- SAMPLE DATA
A,1999-11-19,42.94,43.0,39.81,40.38,10897100.0,0.0,1.0,40.807951739942,40.864972631987,37.833361871614,38.375060346038,10897100.0



SELECT count(*) FROM fd.tickdata_ext WHERE symbol = 'IBM';

SELECT avg(open) FROM fd.tickdata_pq2 WHERE symbol = 'AAPL';

-- Add a hash function ... not working quite yet
-- https://bytes.com/topic/db2/answers/185553-have-any-function-db2-database-can-generate-unique-id-each-string
USE "DEFAULT";

DROP FUNCTION HASH;

CREATE FUNCTION HASH
(HASH VARCHAR(32000) FOR BIT DATA)
RETURNS INTEGER
SPECIFIC HASH EXTERNAL NAME 'hash!hash'
NOT FENCED RETURNS NULL ON NULL INPUT
DETERMINISTIC NO SQL NO EXTERNAL ACTION
LANGUAGE C PARAMETER STYLE SQL ALLOW PARALLEL;

values hash('ab');
