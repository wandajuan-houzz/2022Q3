-- perform on product level to compare BS vs non-BS


-- BS products
-- 46K best sellers to use in May
select * from shop.product_best_seller_rank_monthly 
where mt = '2022-04'


-- VP, MPL & PLA metrics
select if(bs.bs_rank is null, 0, 1) if_bs, perf.* 
from mp.daily_house_channel_performance perf
left join shop.product_best_seller_rank_monthly bs
on perf.house_id = bs.house_id and bs.mt = '2022-04'
where dt >= '2022-05-01'



-- browse ranking

select p.search, pmi.*
from l2.page_views_daily p
join l2.page_modules_daily pm
	on p.page_key = pm.page_key
join l2.page_module_items_daily pmi
	on pm.page_key = pm.page_key and pm.module_key = pmi.module_key 
where p.dt = '2022-05-23' and pm.dt = '2022-05-23' and pmi.dt = '2022-05-23'
and p.page_behavior =  'BROWSE_PRODUCTS' --and p.search != ''
and pm.module_type = 'BROWSE' 


-- mweb test users

select * from wandajuan.visitor_base_mp_mweb_best_seller_label_v2


-- daily browse to click conversion rate
with t as (
select if(bs.bs_rank is null, 0, 1) if_bs, 
		v.test_variant, 
		p.search, pmi.*
from l2.page_views_daily p
join l2.page_modules_daily pm
	on p.page_key = pm.page_key and p.dt = pm.dt
join l2.page_module_items_daily pmi
	on pm.page_key = pm.page_key and pm.module_key = pmi.module_key and pm.dt = pmi.dt
left join shop.product_best_seller_rank_monthly bs
	on pmi.object_id = cast(bs.house_id as varchar) and bs.mt = '2022-04'
join wandajuan.visitor_base_mp_mweb_best_seller_label_v2 v
	on pmi.visitor_id = v.visitor_id and pmi.dt >= v.dt
where p.dt between '2022-05-18' and '2022-05-24'
and p.page_behavior =  'BROWSE_PRODUCTS' --and p.search != ''
and pm.module_type = 'BROWSE' 
)
, dv as (
select 
		dt, 
		visitor_id, 
		test_variant,
		if_bs, 
		count(object_id) imps,
		count(distinct object_id) unique_product_imps,
		count(if(any_click = true, object_id, null)) clicks,
		count(distinct if(any_click = true, object_id, null)) unique_product_clicks
from t
group by 1, 2, 3, 4
)
select 
		dt, test_variant, if_bs,
		count(distinct visitor_id) n_visitors,
		sum(imps) imps,
		sum(unique_product_imps) unique_product_imps,
		sum(clicks) clicks,
		sum(unique_product_clicks) unique_product_clicks
from dv
group by 1, 2, 3


