-- dt = '2022-05-16'
create table wandajuan.pmt_dedup_upc as (
select count(*) over (partition by pa.upc) num_hz_per_upc, 
		-- flag to identified known invalid upc
		if(trim(upc)!= '' and try_cast(upc as bigint) != 0 and upc is not null, 1, 0) legit_upc,
		pa.upc, 
		pa.parent_product_id, 
		pmt.* 
from shop.product_master_table_v2 pmt
left join c2.product_attributes pa
on pmt.house_id = pa.house_id
)


-- summary counts
select count(*) n_products,
		count(distinct upc) nunique_upcs,
		count(if(num_hz_per_upc > 1, 1, null)) n_hz_w_dup_upc,
		count(if(num_hz_per_upc > 1 and legit_upc = 1, 1, null)) n_hz_w_valid_dup_upc,
		count(distinct if(num_hz_per_upc > 1 and legit_upc = 1, upc, null)) nunique_upc_to_dedup
from wandajuan.pmt_dedup_upc


-- top dup upcs by overall
with t as (
select distinct upc, num_hz_per_upc, legit_upc from wandajuan.pmt_dedup_upc
where num_hz_per_upc > 1 and legit_upc = 1
)
select *,  sum(num_hz_per_upc) over (order by num_hz_per_upc desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_total_hz_cnt, 
			sum(num_hz_per_upc) over () total_hz_w_dup_upc,
			1.0000*(sum(num_hz_per_upc) over (order by num_hz_per_upc desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))/(sum(num_hz_per_upc) over ()) cum_perc
from t
order by 2 desc


-- t1/t2 + vendor_name
-- t1: 259
-- t2: 2534
select 
		count(*) over (partition by upc) num_t1t2_per_upc,
		dense_rank() over (partition by upc order by vendor_name) vendor_rnk,
--		*
		num_hz_per_upc,
		upc,
		parent_product_id,
		house_id,
		vendor_id,
		vendor_name,
		seller_type_desc,
		manufacturer,
		product_tier,
		item_gmv_1yr,
		gl_pla_spend_90d
from wandajuan.pmt_dedup_upc
where product_tier in ('t1', 't2')
and num_hz_per_upc > 1 and legit_upc = 1
order by 1 desc, upc, vendor_rnk


with t as (
select distinct upc, num_hz_per_upc, legit_upc from wandajuan.pmt_dedup_upc
where num_hz_per_upc > 1 and legit_upc = 1
and product_tier = 't1'
)
select *,  sum(num_hz_per_upc) over (order by num_hz_per_upc desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_total_hz_cnt, 
			sum(num_hz_per_upc) over () total_hz_w_dup_upc,
			1.0000*(sum(num_hz_per_upc) over (order by num_hz_per_upc desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))/(sum(num_hz_per_upc) over ()) cum_perc
from t
order by 2 desc



select product_tier, count(*) from wandajuan.pmt_dedup_upc
group by 1


select num_hz_per_upc, count(distinct upc) from wandajuan.pmt_dedup_upc
where num_hz_per_upc > 1 and legit_upc = 1
group by 1
order by 1 asc




select * from dm.mp_admp_conversions_daily 