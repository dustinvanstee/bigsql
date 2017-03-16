----------------------------------------------------
-- Use case 6 commands
----------------------------------------------------
-- sync hive /bigsql metastore
-- https://www.ibm.com/support/knowledgecenter/en/SSPT3X_4.2.0/com.ibm.swg.im.infosphere.biginsights.db2biga.doc/doc/biga_hadsyncobj.html#reference_qbt_113_nm
CALL SYSHADOOP.HCAT_SYNC_OBJECTS('PNC', '.*', 'a', 'REPLACE', 'CONTINUE', 'TRANSFER OWNERSHIP TO pncuser');

USE PNC;
SELECT count(*) FROM lendingClubIntRateAvg;
SELECT * FROM lendingClubIntRateAvg FETCH FIRST 10 ROWS ONLY;

-- Perform a SIMPLE JOIN OF this DATA WITH our oracle TABLE 

SELECT A.ID,A.LOAN_AMNT,A.GRADE,A.INT_RATE,B.avg_int_rate,(A.INT_RATE-B.avg_int_rate) AS int_rate_diff FROM PNC.LOAN_TBL_2000 AS A 
  LEFT OUTER JOIN PNC.lendingClubIntRateAvg AS B ON A.GRADE =  B.GRADE ORDER BY int_rate_diff ASC
