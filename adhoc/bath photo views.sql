-- non-paid organic traffic to MP bath photos
-- modified query from Tim's dash: 


-- make a tmp table to handle the huge amount of pv before filtering for bath

-- 2022 web

create table wandajuan.photos_pv_2022YTD as (
select ss.device_cat, ss.medium, pv.*
from l2.session_summary_daily ss
join l2.page_views_daily pv
on ss.dt=pv.dt and ss.session_id =pv.session_id
where ss.dt >= '2022-01-01'
    and ss.site_id=101 
    and pv.dt >= '2022-01-01'
    and pv.site_id=101 
    and pv.page_behavior in ('BROWSE_PHOTOS','VIEW_PHOTO','BROWSE_IDEABOOKS')
)


-- 2021 Web
create table wandajuan.photos_pv_2021T04 as (
select ss.device_cat, ss.medium, pv.*
from l2.session_summary_daily ss
join l2.page_views_daily pv
on ss.dt=pv.dt and ss.session_id =pv.session_id
where ss.dt between '2021-01-01' and '2021-04-30'
    and ss.site_id=101 
    and pv.dt between '2021-01-01' and '2021-04-30'
    and pv.site_id=101 
    and pv.page_behavior in ('BROWSE_PHOTOS','VIEW_PHOTO','BROWSE_IDEABOOKS')
)



with ss as (
select 
    substring(ss.dt,1,7) mt,
    medium,
    device_cat,
    session_id,
    
    
--    count(if(page_behavior = 'BROWSE_DISCUSSIONS' and url like '%bathroom%',1,null)) as browse_discussions,
    count(if(page_behavior = 'BROWSE_IDEABOOKS' and url like '%bathroom%',1,null)) as browse_ideabooks,
    count(if(page_behavior = 'BROWSE_PHOTOS' and topic in (712,713),1,null)) as browse_photos,
    count(if(page_behavior = 'VIEW_PHOTO' and h.category_id in (1007,1018) ,1,null)) as view_photo
--    count(if(page_behavior = 'BROWSE_PRODUCTS' and topic in(469,476,480,602) ,1,null)) as browse_products,
--    count(if(page_behavior = 'VIEW_PRODUCT' and h.category_id in (20001,12001,12013,12008) ,1,null)) as view_product,
--    count(if(page_behavior = 'pvp' and topic in(469,476,480,602) ,1,null)) as pla_page
--from l2.session_summary_daily ss
--join l2.page_views_daily pv
--on ss.dt=pv.dt and ss.session_id =pv.session_id

from wandajuan.photos_pv_2021T04 ss
--from wandajuan.photos_pv_2022YTD ss

left join c2.houses h
on h.house_id = ss.page_id and page_behavior in ('VIEW_PHOTO') and h.category_id in (20001,12001,12013,12008,1007,1018)
where ss.dt >= '2021-01-01' 
    and ss.site_id=101 
    and ss.page_behavior in ('BROWSE_PHOTOS','VIEW_PHOTO','BROWSE_IDEABOOKS')
group by 1,2,3,4
)
select 
		mt,
		if(device_cat = 'Personal computer', 'dWeb', 'mWeb') device_cat,
		medium,
		count(distinct if(browse_ideabooks + browse_photos + view_photo > 0, session_id, null)) n_sess,
		sum(browse_ideabooks + browse_photos + view_photo ) n_photo_views
from 
ss
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
and ce.entity_type = 'Photo'
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


-- app

with ce as (
    select
    ce.dt,
    ce.session_id,
    count(if(entity_type='Photo' and ce.object_id is null and topic_id in ('712', '713'), 1, null)) as browse_photos,
    count(if(entity_type='Photo' and ce.object_id is not null and h.category_id in (1007, 1018), 1, null)) as view_photo
    --count(if(entity_type ='Photo' and ce.object_id is null and topic_id in ('712','713'),1,null)) as browse_photos,
    --count(if(entity_type ='Photo' and ce.object_id is not null and h.category_id in (1007,1018) ,1,null))as view_photo
    from l2.mobile_client_event_daily ce
    left join c2.houses h
    on object_id is not null
    and cast(h.house_id as varchar) = ce.object_id
    and entity_type in ('Product','Photo')
    and ce.event_type='View'
    and h.category_id in (20001,12001,12013,12008,1007,1018)
    where
--            ce.dt between '2021-01-01' and '2021-04-30'
	    ce.dt >= '2022-01-01'
    and ce.event_type='View'
    and entity_type in ('Product','Photo')
    and (topic_id is null or topic_id in ('712','713','469','476','480','602') )
    group by 1,2
) 
, ms as (
    select ce.* from ce
    join l2.mobile_summary_daily ms
    on ms.dt=ce.dt
       and ms.session_id = ce.session_id
    where
--        ms.dt between '2021-01-01' and '2021-04-30'
        ms.dt >= '2022-01-01'
        and ms.country ='US'
        and browse_photos + view_photo > 0
)
select
        substr(dt, 1, 7) mt,
        'APP' device_cat,
        'APP' medium,
        count(distinct if(browse_photos + view_photo > 0, session_id, null)) n_sess,
        sum(browse_photos + view_photo ) n_photo_views
from ms
group by 1, 2, 3