select dt, topic, url, count(distinct visitor_id) n_visitors, count(distinct session_id) n_sess, count(*) 
from l2.page_views_daily
where dt >= '2022-05-14'
and url like '%products%/label--best-seller'
group by 1, 2, 3


desc l2.page_views_daily


select * from shop.product_master_table_v1


-- dweb
select test_variant, count(distinct visitor_id) 
from abtest.mp_visitor_metrics_daily 
where dt >= '2022-05-14'
and test_name = 'mp_dweb_best_seller_label_v2'
and site_id = 101
and device_cat = 'Personal computer'
and test_variant in ('control', 'treatment_a', 'treatment_b')
group by 1


select base.dt, test_variant, topic, url, count(distinct base.visitor_id), sum(active_time_on_page), sum(total_time_on_page) 
from abtest.mp_visitor_metrics_daily base
inner join (
		select dt, visitor_id, topic, url, active_time_on_page, total_time_on_page
		from l2.page_views_daily
		where dt >= '2022-05-14'
		and url like '%products%/label--best-seller'
) pv
on base.dt = pv.dt and base.visitor_id = pv.visitor_id
where base.dt >= '2022-05-14'
and base.test_name = 'mp_dweb_best_seller_label_v2'
and base.site_id = 101
and base.device_cat = 'Personal computer'
and base.test_variant in ('control', 'treatment_a', 'treatment_b')
group by 1, 2, 3, 4

-- mweb
select test_variant, count(distinct visitor_id) 
from abtest.mp_visitor_metrics_daily 
where dt >= '2022-05-14'
and test_name = 'mp_mweb_best_seller_label_v2'
and site_id = 101
and device_cat in ('Smartphone', 'Tablet')
and test_variant in ('control', 'treatment_a', 'treatment_b')
group by 1


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



with ss as (
select distinct sa.dt, sa.session_id, sa.visitor_id, sa.device_cat, sa.medium, sa.landing_page_class, sa.landing_page_url 
from l2.page_views_daily pv
join l2.session_analytics sa
on pv.dt = sa.dt and pv.session_id = sa.session_id 
where pv.dt >= '2022-05-14'
and url like '%products%/label--best-seller'
)
select *
from ss
join l2.page_views_daily pv
on ss.dt = pv.dt and ss.session_id = pv.session_id 


select * from c2.users
where user_id in (
11375865,
70068174,
73160447,
73187723
)

-- 
select distinct visitor_id from l2.page_transition_daily
where dt >= '2022-05-14'
and comp_id = 'best_seller_topic'


and visitor_id = 'd96b0beb-c796-48ce-afb4-79e0098e45fb'
and anchor_text 
