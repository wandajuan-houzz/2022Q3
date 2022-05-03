-- daily report to adMarketplace
-- m_refid=us-dsp-mpl-admp-%CAMPAIGN_ID%_%ADGROUP_ID%_kwd-%KEYWORD_ID%


Campaign_id | Adgroup_id | Keyword_id | Ad_Click_id | imps** | clicks | spend** | conversions* | conv_value* | dt



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



select * from dm.gtm_data_checkout_confirmation 