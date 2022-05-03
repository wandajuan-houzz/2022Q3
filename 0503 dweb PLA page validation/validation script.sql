
-- use cookie: https://www.houzz.com/ad/test/kv=pla_page_command_to_products:treatment to set Treatment
-- make sure in US location to search a furniture, e.g. bath vanity on google / goolge shopping center
-- to find and land on https://www.houzz.com/pvp/kraus-kfd1-30-turino-30-undermount-single-basin-fireclay-kitchen-gloss-white-prpv-pv~172846816?m_refid=PLA_HZ_172846816


-- there are 17 pvpViewProduct in hourly page_views
-- since dt = 2022-04-26, hr = 13
select * from l2.page_views
where page_behavior = 'pvpViewProduct'
and dt >= '2022-04-20'



-- my test session today was not landing in pvpViewProduct by mistake
-- >> check a couple tests later
select * from l2.page_views
where dt = '2022-05-02'
and hr = '20'
and visitor_id = 'd96b0beb-c796-48ce-afb4-79e0098e45fb'

select * from l2.session_summary
where dt = '2022-05-02'
and hr = '20'
and visitor_id = 'd96b0beb-c796-48ce-afb4-79e0098e45fb'



-- pvp landing page has been successfully classified as pvpViewProduct in landing_page_type
-- since the test traffics from 2022-04-26 hr = 20
select visitor_id, user_name, session_id, landing_page_type, landing_page_url, dt, hr, medium, refid 
from l2.session_summary
where dt >= '2022-04-26'
and session_id in (
					select distinct session_id from l2.page_views
					where page_behavior = 'pvpViewProduct'
					and dt >= '2022-04-26' 
					)
					
-- only the 04-26's pvp session is stays in the daily session table
select 		
		visitor_id, user_name, session_id, landing_page_type, landing_page_url, dt, is_houzzer,  
from l2.session_summary_daily 
where dt >= '2022-04-26'
and session_id in (
					select distinct session_id from l2.page_views
					where page_behavior = 'pvpViewProduct'
					and dt >= '2022-04-26' 
					)
					

-- none of the pvp session stays in the session analytics
-- bcs session_analytics specifically filter out houzzer's sessions
-- >>> use incognito to generate signedout paid traffic
select visitor_id, session_id, landing_page_type, landing_page_url, dt 
from l2.session_analytics 
where dt >= '2022-04-26'
and session_id in (
					select distinct session_id from l2.page_views
					where page_behavior = 'pvpViewProduct'
					and dt >= '2022-04-26' 
					)
					

					
