--########################################
--# As pncuser or system, connected to oracle 
--##########################################

SELECT count(*) FROM PNCUSER.LOAN_TBL_2000
SELECT count(*) FROM PNCUSER.LOAN_TBL
SELECT count(*) FROM PNCUSER.LOAN_VIEW_2000
SELECT count(*) FROM PNCUSER.LOAN_VIEW

--SELECT * FROM PNCUSER.LOAN_TBL_2000 WHERE rownum <= 3
-- TRUNCATE TABLE PNCUSER.LOAN_TBL_2000

--########################################
--# Ascreate a view for Zeppelin demo.  Needed for 
--# type conversions 
--##########################################

DROP VIEW PNCUSER.LOAN_VIEW;
CREATE VIEW PNCUSER.LOAN_VIEW AS
   SELECT ID, 
   CAST(LOAN_AMNT AS INT) AS LOAN_AMNT,
   MEMBER_ID,
   CAST(FUNDED_AMNT AS DEC) AS FUNDED_AMNT,
   TERM,
   CAST(INT_RATE AS DEC) AS INT_RATE,
   GRADE,
   SUB_GRADE,
   HOME_OWNERSHIP,
   CAST(ANNUAL_INC AS DEC) AS ANNUAL_INC,
   VERIFICATION_STATUS,
   ISSUE_D,
   LOAN_STATUS,
   PURPOSE,
   "DESC"
   ZIP_CODE,
   ADDR_STATE,
   CAST(DTI AS DEC) AS DTI,
   CAST(DELINQ_2YRS AS DEC) AS DELINQ_2YRS,
   CAST(OPEN_ACC AS INT) AS OPEN_ACC,
   CAST(REVOL_BAL AS INT) AS REVOL_BAL,
   CAST(REVOL_UTIL AS INT) AS REVOL_UTIL,
   CAST(TOTAL_PYMNT AS INT) AS TOTAL_PYMNT
   FROM pncuser.LOAN_TBL
   WHERE rownum <= 2000
   WITH CHECK OPTION;
   

   
