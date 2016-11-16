--db2 update dbm cfg using federated yes
--db2 catalog tcpip node mynode remote awh-yp-small02.services.dal.bluemix.net server 50000
--db2 list node directory show detail
--db2 catalog database sample as sample at node mynode
--db2 terminate
--db2 list database directory

Host name:
awh-yp-small02.services.dal.bluemix.net
Port number:
50000
Database name:
BLUDB
User ID:
dash111207
Password:
6d6d27bd659e


CREATE WRAPPER drda;
CREATE SERVER dashremote 
  TYPE DB2/UDB
  VERSION 11.1
  WRAPPER drda
  AUTHORIZATION "dash111207"
  PASSWORD "6d6d27bd659e"
  OPTIONS ( host 'awh-yp-small02.services.dal.bluemix.net', port '50000', dbname 'BLUDB', password 'Y' )
  
  
CREATE USER mapping FOR bigsql server dashremote OPTIONS(
  REMOTE_AUTHID 'dash111207', REMOTE_PASSWORD '6d6d27bd659e'
)

CREATE nickname dashdb_gosalesdw.emp_expense_fact FOR dashremote.gosalesdw.emp_expense_fact;

SELECT * FROM dashdb_gosalesdw.emp_expense_fact LIMIT 100;

