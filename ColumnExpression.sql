-- MAC hints ...
-- Hit F5 TO refresh SCHEMA!
-- CNTL + ENTER runs a line
-- Highlight code, then Option  + x  runs a batch of lines


use amplab;

drop table amplab.USERVISITS_COL_EXP;

CREATE HADOOP TABLE USERVISITS_COL_EXP (
    sourceIP varchar(16),
    destURL varchar(100),
    visitDate date,
    adRevenue float,
    userAgent varchar(256),
    countryCode char(3),
    languageCode char(6),
    searchWord varchar(32),
    duration integer)
    STORED AS TEXTFILE
 )
    ;
  --    YEAR(visitDate) AS YEAR_PART,
  --    MONTH(visitDate) AS MONTH_PART
 SUBSTRING(sourceIP,1,3) AS tmp

    -- This kicks off a mapreduce job behind the scenes....
LOAD HADOOP USING FILE
  URL '/user/bigsql/uservisits/'
  WITH SOURCE PROPERTIES (
   'field.delimiter'=',',
   'ignore.extra.fields'='true',
   'field.indexes'='1,2,3,4,5,6,7,8,9'
   )
INTO TABLE USERVISITS_COL_EXP
PARTITION ()  APPEND
;

-- look in /apps/hive/warehouse/amplab.db/USERVISITS_COL_EXP/......
INSERT INTO uservisits_col_exp SELECT * FROM uservisits_pq;


SELECT * FROM uservisits_col_exp LIMIT 10;
SELECT INT(visitDate) / 100 FROM uservisits_col_exp LIMIT 10;
SELECT SUBSTRING(sourceIP,1,3) AS tmp FROM uservisits_col_exp LIMIT 10;




CREATE HADOOP TABLE TRANSACTION (
  trans_id int, 			-- transaction id, unique
  product varchar(50), 
  trans_ts varchar(20)	-- transaction timestamp
) 
PARTITIONED BY ( 
  INT(trans_id/10000) AS trans_part,
  YEAR(trans_ts) AS year_part 
)

INSERT INTO AMPLAB.TRANSACTION (trans_id,product,trans_ts) VALUES (1, 'bleck','2005-12-01');

SELECT * FROM amplab.TRANSACTION;


