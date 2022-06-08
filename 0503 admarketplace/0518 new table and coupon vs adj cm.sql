-- confirm the conv_value/net revenue sent to AdMP if align with internal adj CM %
-- adj CM % --> Looks ok
select substr(created, 1, 7), sum(gmv), sum(adjusted_cm), 1.0000*sum(adjusted_cm)/sum(gmv), 1.0000*sum(adjusted_cm-coupons_promotions)/sum(gmv) from mp.order_metrics_with_adjustments 
where substr(created, 1, 4) = '2022'
and order_status in (0, 1, 2, 3, 4, 5, 20, 99)
        and is_replacement_order = 0
        and order_id not in (select order_id from logs.marketplace_gift_cards_purchased)
group by 1



-- modified conversion table to send over converted data
with os as (

	-- daily orders with 30d look back sessions from admp
	select *,
			row_number() over (partition by order_id order by start_ts desc) rk
	from dm.order_sess
	where 
	medium = 'PAID'
	and refid like '%us-dsp-mpl-admp-%'
--	and dt >= '2022-04-27'
	and dt = '2022-05-17'
)

--, web_requests as (
	select 
			os.dt,
	        sa.session_id,
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
	        sa.dt as session_dt,
	        os.created,
	        os.order_date,
	        os.conversion_ts,
	        po.adjusted_cm + po.coupons_promotions
--	        ,
--	        os.order_id,
--	        po.gmv,
--	        po.adjusted_cm,
	        
--	        po.coupons_promotions 
            ,po.*
	from os
	left join 
		l2.session_analytics sa
	on 
		sa.session_id = os.session_id 
--		and sa.dt >= '2022-04-27'
		and sa.dt BETWEEN '2022-04-17' AND '2022-05-17'
	left JOIN
	    dm.mp_admp_click_events ad
	ON 
		sa.landing_page_key = ad.request_id 
		and ad.dt BETWEEN '2022-04-17' AND '2022-05-17'
	left join
	    mp.order_metrics_with_adjustments po
	on os.order_id = po.order_id
	    and os.order_date = substr(po.created, 1, 10)
	where rk = 1
)
select 
		dt, 
		campaign_id, 
		adgroup_id, 
		kwd_id, 
		adcid, 
		from_unixtime(conversion_ts, 'America/New_York') conversion_time_in_est,
        count(distinct order_id) conversions,
        sum(gmv) as revenue,
        sum(adjusted_cm) as net_revenue
from web_requests
group by 1, 2, 3, 4, 5, 6


-- missing m_refid in the click
select * from dm.mp_admp_click_events 
where request_id = '3293b2c2-86f1-4ae3-bdad-c7e72e4bf649'


select * from l2.raw_web_request 
where dt >= '2022-05-16'
--and session_id = 'f18a14808a588e6f11bc5cd18db8b503'
and request_id = '3293b2c2-86f1-4ae3-bdad-c7e72e4bf649'
and event_type = 'm_refid'

select * from l2.session_analytics 
where dt >= '2022-05-16'
and session_id = 'f18a14808a588e6f11bc5cd18db8b503'

-- validated to have only 1 converted row after undo/redo 2022-04-27
select * from dm.mp_admp_conversions_daily
where dt = '2022-04-27' 


-- sftp
-- houzz_conversions_daily_upload-2022-04-27.csv looks fine


-- negative adj cm in orders
select * from mp.order_metrics_with_adjustments 
where order_id = 1733043707059291
in 
(1733043707059291,
1733049011632923)



-- + listing level adj cm
select po.*, m.adjusted_cm_rate, m.adjusted_cm_dollar 
from mp.order_item_margins_with_replacement po
join shop.adjusted_contribution_margin m
on po.item_id = m.vendor_listing_id 
where order_id = 1733117854536029


order_id in 
(1733043707059291,
1733049011632923)


-- listing level adj cm
select * from shop.adjusted_contribution_margin 
where house_id in 
(52686548,
175782300)


-- or workaround by removing coupon amount
select adjusted_cm - coupon_value as conv_value 
from mp.order_metrics_with_adjustments 
where order_id = 1733043707059291
in 
(1733043707059291,
1733049011632923)



-- 4 conversions on 05-17
select *, conversion_time_in_est at time zone 'America/New_York' from dm.mp_admp_conversions_daily 
where dt = '2022-05-17'





-- os
select *,
		row_number() over (partition by order_id order by start_ts desc) rk
from dm.order_sess
where 
medium = 'PAID'
and refid like '%us-dsp-mpl-admp-%'
and dt = '2022-05-17'




select substr(created, 1, 7) order_mt, 
		count(distinct order_id) tot_orders, 
		sum(gmv) gmv, 
		sum(adjusted_cm) adjCM, 
--		sum(coupons_promotions) coupon_amount, 
--		1.0000*sum(adjusted_cm)/sum(gmv), 1.0000*sum(adjusted_cm-coupons_promotions)/sum(gmv), 
		count(distinct if(adjusted_cm< 0, order_id, null)) num_negative_adjCM_orders,
		count(distinct if(adjusted_cm-coupons_promotions < 0, order_id, null)) num_negative_adjCM_b4coupon_orders,
		
		1.00*count(distinct if(adjusted_cm< 0, order_id, null))/count(distinct order_id) negative_adjCM_orders_perc,
		1.00*count(distinct if(adjusted_cm-coupons_promotions < 0, order_id, null))/count(distinct order_id) negative_adjCM_b4coupon_orders_perc
		
from mp.order_metrics_with_adjustments 
	where substr(created, 1, 4) = '2022'
	and order_status in (0, 1, 2, 3, 4, 5, 20, 99)
	and is_replacement_order = 0
	and order_id not in (select order_id from logs.marketplace_gift_cards_purchased)
group by 1



select *, adjusted_cm - coupons_promotions
from mp.order_metrics_with_adjustments 
where substr(created, 1, 4) = '2022'
and order_status in (0, 1, 2, 3, 4, 5, 20, 99)
and is_replacement_order = 0
and order_id not in (select order_id from logs.marketplace_gift_cards_purchased)
and substr(created, 1, 10) = '2022-05-17'
and adjusted_cm - coupons_promotions < 0