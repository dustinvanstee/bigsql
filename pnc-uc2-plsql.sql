----------------------------------------------------
-- PL/SQL commands as BIGSQL
----------------------------------------------------
set sql_compat = 'ORA'
set SQL_COMPAT = 'ORA';

create or replace function LOAN_RATING_FUNC return sys_refcursor
 as
 LOAN_RATING_CUR sys_refcursor;
   begin
   open LOAN_RATING_CUR for
   SELECT
           GRADE,
           ID,
           EMP_TITLE,
           LOAN_AMNT,
           ANNUAL_INC
    FROM   LOAN_TBL
    WHERE  GRADE  = 'A';
 return LOAN_RATING_CUR;
   end;
/
select LOAN_RATING_FUNC() from dual;
