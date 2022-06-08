dt = '2022-06-04'
create table wandajuan.pmt_dedup_upc as (
select 
		sum(item_gmv_90d) over (partition by pmt.vendor_name) as vendor_gmv,
		count(*) over (partition by pa.upc) num_hz_per_upc, 
		-- flag to identified known invalid upc
		if(trim(upc)!= '' and try_cast(upc as bigint) != 0 and upc is not null, 1, 0) legit_upc,
		pa.upc, 
		pa.parent_product_id, 
		pmt.* 
from shop.product_master_table_v2 pmt
left join c2.product_attributes pa
on pmt.house_id = pa.house_id
)

-- 2.3M hz w dup upc
select * from wandajuan.pmt_dedup_upc
where num_hz_per_upc > 1 and legit_upc = 1
order by vendor_gmv desc


-- EDA

select vendor_name, vendor_gmv, count(distinct upc) nunique_upc, count(distinct house_id) n_hz,
		1.00*count(distinct house_id)/count(distinct upc) avg_hz_per_upc
from wandajuan.pmt_dedup_upc
where num_hz_per_upc > 1 and legit_upc = 1
and vendor_gmv > 0
and seller_type_desc = 'Direct'
and item_gmv_90d > 0
group by 1, 2
order by 5 desc

select * from 
wandajuan.pmt_dedup_upc
where num_hz_per_upc > 1 and legit_upc = 1
and vendor_name = 'GDFStudio'