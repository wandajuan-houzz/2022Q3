-- daily report to adMarketplace
-- m_refid=us-dsp-mpl-admp-%CAMPAIGN_ID%_%ADGROUP_ID%_kwd-%KEYWORD_ID%


Campaign_id | Adgroup_id | Keyword_id | Ad_Click_id | imps** | clicks | spend** | conversions* | conv_value* | dt | GMV | net revene



Ad_Click_id | conversions* | conv_value* | dt



-- stg table from l2.raw_web_request 
-- visitor click from ivy.active_admin_comments_daily arketplace

-- Campaign_id | adgroup_id | Keyword_id | adclick_id

select ts, session_id, request_id, visitor_id, user_id,  
		regexp_extract(json_extract_scalar(event_metadata, '$.refid'), 'us-dsp-mpl-admp-(\d+)_(\d+)_kwd-(\d+)', 1) campaign_id,
		regexp_extract(json_extract_scalar(event_metadata, '$.refid'), 'us-dsp-mpl-admp-(\d+)_(\d+)_kwd-(\d+)', 2) adgroup_id,
		regexp_extract(json_extract_scalar(event_metadata, '$.refid'), 'us-dsp-mpl-admp-(\d+)_(\d+)_kwd-(\d+)', 3) kwd_id,
--		json_extract_scalar(event_metadata, '$.adcid') adcid,
--		json_extract_scalar(event_metadata, '$.refid') refid,
		json_extract_scalar(event_metadata, '$.adcid') adcid,
		json_extract_scalar(event_metadata, '$.refid') refid,
		event_metadata
from l2.raw_web_request 
where 
event_type = 'm_refid' 
and json_extract_scalar(event_metadata, '$.refid') like 'us-dsp-mpl-admp-%' 
and event_metadata like '%adcid%'
and dt = '2022-04-27' and hr = '11'



select *
from dm.gtm_data_checkout_confirmation 


select * from dm.mp_admp_click_events




--- 




create table wandajuan.mp_admp_click_events as (
select ts, session_id, request_id, visitor_id, user_id,  
		regexp_extract(json_extract_scalar(event_metadata, '$.refid'), 'us-dsp-mpl-admp-([0-9]+)_([0-9]+)_kwd-([0-9]+)', 1) campaign_id,
		regexp_extract(json_extract_scalar(event_metadata, '$.refid'), 'us-dsp-mpl-admp-([0-9]+)_([0-9]+)_kwd-([0-9]+)', 2) adgroup_id,
		regexp_extract(json_extract_scalar(event_metadata, '$.refid'), 'us-dsp-mpl-admp-([0-9]+)_([0-9]+)_kwd-([0-9]+)', 3) kwd_id,
		json_extract_scalar(event_metadata, '$.adcid') adcid,
		json_extract_scalar(event_metadata, '$.refid') refid,
		event_metadata,
		dt
from l2.raw_web_request 
where 
event_type = 'm_refid' 
and json_extract_scalar(event_metadata, '$.refid') like 'us-dsp-mpl-admp-%' 
and event_metadata like '%adcid%'
and dt = '2022-04-27' and hr = '11'
)



select * from wandajuan.mp_admp_click_events ad
left join (
select *,
		row_number() over (partition by order_id order by start_ts desc) rk
from dm.order_sess 
where dt = '2022-04-27'
) os
on ad.visitor_id = os.visitor_id and os.rk = 1


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
join wandajuan.mp_admp_click_events ad
on sa.landing_page_key = ad.request_id and sa.dt = ad.dt
left join (
select *,
		row_number() over (partition by order_id order by start_ts desc) rk
from dm.order_sess 
where dt = '2022-04-27'
and medium = 'PAID' and refid like '%us-dsp-mpl-admp-%'
) os
on sa.dt = os.dt and sa.session_id = os.session_id and rk = 1
left join mp.order_metrics_with_adjustments po
on os.order_id = po.order_id and os.order_date = substr(po.created, 1, 10)
where sa.dt = '2022-04-27'
)
select dt, campaign_id, adgroup_id, kwd_id, adcid, from_unixtime(conversion_ts, 'America/New_York') as conversion_time, count(distinct order_id) conversions, sum(gmv) as revenue, sum(adjusted_cm) as net_revenue
from t
group by 1, 2, 3, 4, 5, 6



select * from mp.order_metrics_with_adjustments
where substr(created, 1, 10) = '2022-04-27'
and order_id = 1731287841631156

select * from c2.orders_daily
where substr(created, 1, 10) = '2022-04-27'
and order_id = 1731287841631156



select *,
		row_number() over (partition by order_id order by start_ts desc) rk
from dm.order_sess 
where dt = '2022-04-27'
and order_id = 1731287841631156



select session_id, user_id, visitor_id, medium, refid, landing_page_url, landing_page_metadata from l2.session_summary_daily
where dt = '2022-04-27'
and refid like 'us-dsp-mpl-admp%'



desc l2.session_summary_daily