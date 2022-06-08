--/Users/wj/Google Drive/2022/2022Q3/0505 dedup upc/explore.sql
-- ways to dedup upc


create table wandajuan.pmt_tmp_v2 as (
select count(*) over (partition by pa.upc) num_hz_per_upc, pa.upc, pmt.*
from shop.product_master_table_v2 pmt
left join c2.product_attributes pa
on pmt.house_id = pa.house_id
)

select distinct upc, num_hz_per_upc 
from wandajuan.pmt_tmp_v2
where 
upc not in ('', '000000000000', '0000000000000', '000000000000', '0000000000000') and 
num_hz_per_upc > 1
and upc is not null
order by 2 desc





---
with t as (
-- 2376840
-- 839068
select upc, count(distinct house_id) n_prod from wandajuan.pmt_tmp_v2 pmt
where 
upc not in ('', '000000000000', '0000000000000', '000000000000', '0000000000000') and 
num_hz_per_upc > 1
and trim(pmt.upc) != ''
and cast(pmt.upc as bigint) != 0
group by 1
--order by 2
)
select avg(n_prod)
from t

select pa.parent_product_id, pa.upc, pmt.* 
from shop.product_master_table_v2 pmt
left join c2.product_attributes pa
on pmt.house_id = pa.house_id


select pa.parent_product_id, pa.upc, pmt.* 
from wandajuan.pmt_tmp_v2 pmt
left join c2.product_attributes pa
on pmt.house_id = pa.house_id
where 
pmt.upc not in ('', '000000000000', '0000000000000', '000000000000', '0000000000000') and 
num_hz_per_upc > 1
and pmt.upc is not null
and trim(pmt.upc) != ''
and cast(pmt.upc as bigint) != 0
order by 2 