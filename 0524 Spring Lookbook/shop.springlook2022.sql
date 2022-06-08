-- create external table, shop.springlook2022, for capping price 

-- ssh util.data
-- sudo -iu hadoop
-- ssh hdp-alpha-metastore-0cff3f4206054b2e5
-- hive

-- check data before migration
select *
from wandajuan.springlook2000


-- create a shop table in presto
drop table shop.springlook2022
create table shop.springlook2022 (
house_id bigint,
vendor_listing_id bigint,
category_id bigint,
all_status_valid boolean,
final_display_price double,
fcth double,
fm string,
tgm string,
categoryname string,
categorypath string,
categorylevel bigint,
status string,
categorymarginid double,
categorymarginfloor double,
categorymarginrecommended double,
brands string,
tradestatus string,
product_id bigint,
price_cap double,
new_tg double,
current_tg double,
current_fl double,
new_fl double
)
LOCATION 's3a://houzz-data-data-us-west-2/impala_export/shop.db/springlook2022';


-- insert data in Presto
insert into shop.springlook2022
select * from wandajuan.springlook2022_v3


-- check data before migrating to hdp hive
select * from shop.springlook2022


-- after checking all good, migrate to hdp

--ssh util.data
--sudo -iuhadoop
--ssh hdp-main-assistant-0676fac20cb2225fc
--hive


-- create a shop table in hive hdp
drop table shop.springlook2022
create table shop.springlook2022 (
house_id bigint,
vendor_listing_id bigint,
category_id bigint,
all_status_valid boolean,
final_display_price double,
fcth double,
fm string,
tgm string,
categoryname string,
categorypath string,
categorylevel bigint,
status string,
categorymarginid double,
categorymarginfloor double,
categorymarginrecommended double,
brands string,
tradestatus string,
product_id bigint,
price_cap double,
new_tg double,
current_tg double,
current_fl double,
new_fl double
)
LOCATION 's3a://houzz-data-data-us-west-2/impala_export/shop.db/springlook2022';


alter table shop.springlook2022
set LOCATION 's3a://houzz-data-data-us-west-2/impala_export/shop.db/springlook2022';



