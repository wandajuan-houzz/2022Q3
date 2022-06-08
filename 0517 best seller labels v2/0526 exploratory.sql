show tables from wandajuan like '%mp_mweb_best_seller_label_v2'

select * from wandajuan.visitor_base_mp_mweb_best_seller_label_v2
select * from wandajuan.mp_metrics_mp_mweb_best_seller_label_v2
select * from wandajuan.xo_metrics_mp_mweb_best_seller_label_v2

select * from wandajuan.browse_ranking_mp_mweb_best_seller_label_v2

select * from wandajuan.mp_mweb_best_seller_label_v2



-- bs browse ranking

select * from wandajuan.browse_ranking_mp_mweb_best_seller_label_v2
where if_bs= 1

-- daily browse to click conversion rate
with dv as (
select 
		dt, 
		visitor_id, 
		test_variant,
		if_bs, 
		count(object_id) imps,
		count(distinct object_id) unique_product_imps,
		count(if(any_click = true, object_id, null)) clicks,
		count(distinct if(any_click = true, object_id, null)) unique_product_clicks
from wandajuan.browse_ranking_mp_mweb_best_seller_label_v2
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

-- clicks to topic pages


-- 27 page views to bs pages are from not in test mweb users!!
select *
--base.dt, test_variant, topic, url, count(distinct base.visitor_id), sum(active_time_on_page), sum(total_time_on_page) 
from abtest.mp_visitor_metrics_daily base
inner join (
		select dt, visitor_id, topic, url, active_time_on_page, total_time_on_page
		from l2.page_views_daily
		where dt >= '2022-05-14'
		and url like '%products%/label--best-seller'
) pv
on base.dt = pv.dt and base.visitor_id = pv.visitor_id
where base.dt >= '2022-05-14'
and base.test_name = 'mp_mweb_best_seller_label_v2' --'mp_dweb_best_seller_label_v2' --
--and base.site_id = 101
--and base.device_cat in ('Smartphone', 'Tablet')
--and base.test_variant in ('control', 'treatment_a', 'treatment_b')
group by 1, 2, 3, 4


select pv.dt, test_variant, count(distinct vb.visitor_id) visitors, count(1) page_views, sum(active_time_on_page) active_time_on_page, sum(total_time_on_page) total_time_on_page 
from wandajuan.visitor_base_mp_mweb_best_seller_label_v2 vb
join (
		select dt, visitor_id, topic, url, active_time_on_page, total_time_on_page
		from l2.page_views_daily
		where dt >= '2022-05-14'
		and url like '%products%/label--best-seller'
) pv
on vb.visitor_id = pv.visitor_id and pv.dt >= vb.dt
group by 1, 2



select * from ranking.impressions_n_clicks 