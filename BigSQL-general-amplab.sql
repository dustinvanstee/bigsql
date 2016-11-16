select tabschema,tabname from syscat.tables where tabschema = 'BIGSQL';
select tabschema,tabname from syscat.tables;
select tabschema,tabname,colname from syscat.columns WHERE tabschema = 'BIGSQL' ORDER BY tabname;


--## Process User Visits ##
CREATE schema amplab;
use amplab;
DROP TABLE IF EXISTS uservisits_ext;
DROP TABLE IF EXISTS uservisits_pq;

DROP TABLE IF EXISTS rankings_ext;
DROP TABLE IF EXISTS rankings_pq;

DROP TABLE IF EXISTS documents_ext;
DROP TABLE IF EXISTS documents_pq;

DROP TABLE IF EXISTS URL_COUNTS_PARTIAL_EXT;
DROP TABLE IF EXISTS URL_COUNTS_PARTIAL_pq;
DROP TABLE IF EXISTS URL_COUNTS_TOTAL_pq;

CREATE EXTERNAL HADOOP TABLE uservisits_ext (
    sourceIP varchar(116),
    destURL varchar(100),
    visitDate date,
    adRevenue float,
    userAgent varchar(256),
    countryCode char(3),
    languageCode char(6),
    searchWord varchar(32),
    duration integer)
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  LOCATION '/user/bigsql/uservisits/';
 
SELECT count(*) FROM  uservisits_ext;
 SELECT * FROM  uservisits_ext LIMIT 10;
 
drop table amplab.USERVISITS_PQ;
CREATE HADOOP TABLE uservisits_pq (
    sourceIP varchar(16),
    destURL varchar(100),
    visitDate date,
    adRevenue float,
    userAgent varchar(256),
    countryCode char(3),
    languageCode char(6),
    searchWord varchar(32),
    duration integer)
    STORED AS PARQUETFILE;

    -- This kicks off a mapreduce job behind the scenes....
LOAD HADOOP USING FILE
  URL '/user/bigsql/uservisits/'
  WITH SOURCE PROPERTIES (
   'field.delimiter'=',',
   'ignore.extra.fields'='true',
   'field.indexes'='1,2,3,4,5,6,7,8,9'
   )
INTO TABLE uservisits_pq
   APPEND
;

SELECT count(*) FROM  uservisits_pq;
SELECT * FROM  uservisits_ext  LIMIT 10;

-- cartesian join
SELECT count(*) FROM uservisits_ext, uservisits_pq WHERE uservisits_ext.searchWord = uservisits_pq.searchWord;
SELECT count(*) FROM uservisits_ext, uservisits_pq;


