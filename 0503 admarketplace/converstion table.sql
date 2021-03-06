-- admarketplace conversion table

with t as (
select sa.session_id,
		ad.request_id,
		sa.visitor_id,
		sa.user_id,
		sa.start_ts,
		sa.medium,
		sa.refid,
		ad.campaign_id,
		ad.adgroup_id,
		ad.kwd_id,
		ad.adcid,
		sa.dt,
--		os.*,
		os.created,
		os.order_date,
		os.conversion_ts,
		os.order_id,
		po.gmv,
		po.adjusted_cm
from l2.session_analytics sa
join dm.mp_admp_click_events ad
on sa.landing_page_key = ad.request_id and sa.dt = ad.dt
left join (
select *,
		row_number() over (partition by order_id order by start_ts desc) rk
from dm.order_sess 
where 
-- this is to attribute an order to last admarketplace touch 
-- remove dt filter to relax to window to 30d
medium = 'PAID' and refid like '%us-dsp-mpl-admp-%'
and dt = '2022-05-06'
) os
on sa.dt = os.dt and sa.session_id = os.session_id and rk = 1
left join mp.order_metrics_with_adjustments po
on os.order_id = po.order_id and os.order_date = substr(po.created, 1, 10)
where sa.dt = '2022-05-06'
)
select dt, campaign_id, adgroup_id, kwd_id, adcid, 
		from_unixtime(conversion_ts, 'America/New_York') as conversion_time_in_EST, 
		count(distinct order_id) conversions, 
		sum(gmv) as revenue, 
		sum(adjusted_cm) as net_revenue
from t
group by 1, 2, 3, 4, 5, 6

create table wandajuan.mp_admp_conversions_daily_0602 as (
select * from dm.mp_admp_conversions_daily 
where dt = '2022-06-02'
and adcid = '1652809847020100000'

select * from dm.mp_admp_click_events 
where adcid = '1652809847020100000'



select * from dm.order_sess 
where session_id = 'f5d95ccc72b365038eec2d3addd45000'
and dt = '2022-06-02'




select from_unixtime(1654220576)