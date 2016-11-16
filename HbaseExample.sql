CREATE HBASE TABLE IF NOT EXISTS BIGSQLLAB.REVIEWS ( 
REVIEWID 		varchar(10) primary key not null,
PRODUCT		varchar(30), 
RATING		int, 
REVIEWERNAME	varchar(30), 
REVIEWERLOC		varchar(30), 
COMMENT		varchar(100), 
TIP			varchar(100)
) 
COLUMN MAPPING 
( 
key		mapped by (REVIEWID), 
summary:product	mapped by (PRODUCT),
summary:rating  mapped by (RATING),
reviewer:name	mapped by (REVIEWERNAME),
reviewer:location  mapped by (REVIEWERLOC),
details:comment    mapped by (COMMENT),
details:tip	   mapped by (TIP) 
);


-- hdfs dfs -ls /apps/hbase/data/data/default
insert into bigsqllab.reviews  values ('198','scarf','2','Bruno',null,'Feels cheap',null);
insert into bigsqllab.reviews (reviewid, product, rating, reviewername) values ('298','gloves','3','Beppe');

select count(*) AS count from bigsqllab.reviews; 

select reviewid, product, reviewername, reviewerloc 
from bigsqllab.reviews
where rating >= 3; 


-- Creating Views in HBASE
create view bigsqllab.testview as 
select reviewid, product, reviewername, reviewerloc 
from bigsqllab.reviews
where rating >= 3;

-- Query the view
select reviewid, product, reviewername  
from bigsqllab.testview;


