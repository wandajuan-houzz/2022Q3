-- non-paid organic traffic to MP bath products
-- following /Users/wj/Google Drive/2022/2022Q2/04 2022 bath goals/mpl sessions.sql



-- make a tmp table to handle the huge amount of pv before filtering for bath

-- 2022 web
 
create table wandajuan.mpl_product_pv_2022YTD as (
select ss.device_cat, ss.medium, pv.*
from l2.session_summary_daily ss
join l2.page_views_daily pv
on ss.dt=pv.dt and ss.session_id =pv.session_id
where ss.dt >= '2022-01-01'
    and ss.site_id=101 
    and pv.dt >= '2022-01-01'
    and pv.site_id=101 
    and pv.page_behavior in ('BROWSE_PRODUCTS','VIEW_PRODUCT','pvp')
)
    
-- 2021 web
--> re-use those created previously





with ss as (
		select substr(dt, 1, 7) as mt,
				device_cat,
				medium,
				session_id,
				count(if(lower(t.name) like '%bath%' or lower(t.name) like '%powder%', 1, null)) bp_pv,
				count(if(vl.l1_category_name = 'bath products', 1, null)) vp_pv,
				count(*) pv
--		from wandajuan.mpl_product_pv_2021q1 pv
		from wandajuan.mpl_product_pv_2021q2 pv
--		from wandajuan.mpl_product_pv_2022YTD pv
		left join c2.topics t
		on pv.topic = t.topic_id and t.namespace = 'products' and t.locale_id = 1001
		left join (
					select distinct house_id, l1_category_name
					from shop.vl_pupil
					) vl
		on coalesce(pv.page_id, cast(regexp_extract(pv.url, '(.*pv~)(\d+)(.*)', 2) as bigint)) = vl.house_id
		where substr(dt, 1, 7) between '2021-01' and '2021-04'
		group by 1, 2, 3, 4
)
select mt, 
		if(device_cat = 'Personal computer', 'dWeb', 'mWeb') device_cat,
		medium, 
		count(if(bp_pv+vp_pv>0, 1, null)) n_sess,
		sum(bp_pv+vp_pv) n_pv
from ss
--where medium != 'PAID' or medium is NULL
group by 1, 2, 3




-- app

with ms as (
select 
--		ce.*
		
		substr(ms.dt, 1, 7) mt,
		ms.session_id, 
		count(if(lower(t.name) like '%bath%' or lower(t.name) like '%powder%', 1, null)) bp_pv,
		count(if(vl.l1_category_name = 'bath products', 1, null)) vp_pv,
		count(*) pv
from l2.mobile_summary_daily ms -- main app table excluding bot traffics
inner join l2.mobile_client_event_daily ce
on ms.dt = ce.dt and ms.session_id = ce.session_id
left join c2.topics t
on ce.topic_id = cast(t.topic_id as varchar) and t.namespace = 'products' and t.locale_id = 1001
left join (
			select distinct house_id, l1_category_name
			from shop.vl_pupil
			) vl
on cast(ce.object_id as bigint) = vl.house_id 
--where ms.dt >= '2022-04-01'
where 
--ms.dt between '2021-01-01' and '2021-04-30' 
ms.dt >= '2022-01-01'
and ms.country = 'US'
and ce.entity_type = 'Product'
and ((lower(t.name) like '%bath%' or lower(t.name) like '%powder%') or 
(vl.l1_category_name = 'bath products'))
group by 1, 2
)
select 
		mt, 
		'APP' as device_cat,
		'APP' as medium,
 		count(if(bp_pv+vp_pv>0, 1, null)) n_sess,
		sum(bp_pv+vp_pv) n_pv
from ms
group by 1, 2, 3


