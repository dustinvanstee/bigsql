----------------------------------------------------
-- FEDERATION commands as BIGSQL
----------------------------------------------------
-- # In OS
--   su - bigsql
--   db2 update dbm cfg using federated yes
-- # Node name here must correspond to what you have in tnsnames.ora
--   db2 catalog tcpip node PNCDEMO remote 10.177.93.154 server 1521
--   db2 catalog database pncdemo as pncdemo at node PNCDEMO
--   db2 list database directory
-- see /data/work/osa_2017_02_pnc
--   db2 list node directory
----------------------------------------------------
-- FEDERATION commands as BIGSQL
----------------------------------------------------

DROP WRAPPER ORA;
--DROP SERVER ORASERV;

CREATE WRAPPER ORA LIBRARY 'libdb2net8.so';

CREATE SERVER ORASERV 
  TYPE ORACLE 
  VERSION 12 
  WRAPPER ORA 
  AUTHORIZATION ”PNCUSER” 
  PASSWORD ”IBMDem0snow” 
  OPTIONS (NODE 'PNCDEMO', PUSHDOWN 'Y', COLLATING_SEQUENCE 'N');
  
CREATE USER MAPPING FOR bigsql SERVER ORASERV OPTIONS ( REMOTE_AUTHID 'PNCUSER', REMOTE_PASSWORD 'IBMDem0snow');

-- Switch to PNCUSER profile
-- Extract some data from Lending Club

CREATE SCHEMA  IF NOT EXISTS PNC;
USE PNC;
CREATE NICKNAME PNC.LOAN_TBL_2000 FOR ORASERV.PNCUSER.LOAN_TBL_2000;
CREATE NICKNAME PNC.LOAN_TBL      FOR ORASERV.PNCUSER.LOAN_TBL;


SELECT count(*) FROM pnc.loan_tbl_2000
SELECT * FROM pnc.loan_tbl_2000 FETCH FIRST 5 ROWS ONLY
SELECT count(*) FROM pnc.loan_tbl

-- Demonstrate a simple Join between a table in BigSQL and Oracle
DROP TABLE IF EXISTS PNC.CREDIT_RISK_DESCRIPTION
CREATE HADOOP TABLE IF NOT EXISTS PNC.CREDIT_RISK_DESCRIPTION (
    grade varchar(2),
    description varchar(100)
    )
    STORED AS PARQUETFILE; 

INSERT INTO PNC.CREDIT_RISK_DESCRIPTION VALUES ( 'A','Low Risk');
INSERT INTO PNC.CREDIT_RISK_DESCRIPTION VALUES ( 'B','Low Risk');
INSERT INTO PNC.CREDIT_RISK_DESCRIPTION VALUES ( 'C','Med Risk');
INSERT INTO PNC.CREDIT_RISK_DESCRIPTION VALUES ( 'D','Med Risk');
INSERT INTO PNC.CREDIT_RISK_DESCRIPTION VALUES ( 'E','High Risk');
INSERT INTO PNC.CREDIT_RISK_DESCRIPTION VALUES ( 'F','High Risk');
SELECT * FROM PNC.CREDIT_RISK_DESCRIPTION

-- Join Data from Oracle or any RDBMS and BigSQL
SELECT A.ID,A.LOAN_AMNT,A.GRADE,B.DESCRIPTION FROM PNC.LOAN_TBL_2000 AS A 
  LEFT OUTER JOIN PNC.CREDIT_RISK_DESCRIPTION AS B ON A.GRADE =  B.GRADE




-- Example of a very small table created

-- CREATE SCHEMA IF NOT EXISTS PNC;
-- USE pnc;
-- CREATE nickname pnc.creditcard FOR ORASERV.system.creditcard;
-- SELECT * FROM pnc.creditcard
-- DROP TABLE PNC.CREDIT_RISK_DESCRIPTION

-- CLeanup Section ...  
DROP TABLE PNC.CREDIT_RISK
DROP TABLE PNC.CREDIT_RISK_DESCRIPTION
DROP TABLE PNC.lendingclubintrateavg
DROP TABLE PNC.TEST1

DROP SCHEMA  IF  EXISTS PNC;


