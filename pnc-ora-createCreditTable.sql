----------------------------------------------------
-- SQL DDL to Create and seed some data into AGG table
----------------------------------------------------
-- 
----------------------------------------------------
-- 
----------------------------------------------------

-- MAC hints ...
-- Hit F5 TO refresh SCHEMA!
-- CNTL + ENTER runs a line
-- Highlight code, then Option  + x  runs a batch of lines


CREATE SCHEMA  IF NOT EXISTS MUFG;

-- Create a credit card table for the demo
CREATE TABLE CREDITCARD (
  CCARD_ID INT,
  SSN VARCHAR(11),
  CARDTYPE VARCHAR(15),
  CARDNUMBER VARCHAR(16),
  EXPMONTH INT,
  EXPYEAR INT
);

INSERT INTO CREDITCARD VALUES ( 1,'121-11-1111','MSCD','5555292233334444',12,2018);
INSERT INTO CREDITCARD VALUES ( 2,'222-45-2324','VISA','4944111192223333',12,2018);
INSERT INTO CREDITCARD VALUES ( 3,'224-34-3456','AMEX','3393111122226363',12,2018);
INSERT INTO CREDITCARD VALUES ( 4,'927-65-4321','DISC','7777911162293333',12,2018);
INSERT INTO CREDITCARD VALUES ( 5,'121-19-1191','MSCD','5555226233334444',12,2018);
INSERT INTO CREDITCARD VALUES ( 6,'232-45-2924','VISA','4444111122293333',12,2018);
INSERT INTO CREDITCARD VALUES ( 7,'224-34-3496','AMEX','3333191122229333',12,2018);

SELECT * FROM CREDITCARD ;

SELECT t.* FROM system.creditcard t 
