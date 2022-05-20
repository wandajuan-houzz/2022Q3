('wandajuan.visitor_base_mp_checkout_dweb_v2',
 'wandajuan.mp_metrics_mp_checkout_dweb_v2',
 'wandajuan.mp_checkout_dweb_v2',
 'wandajuan.xo_metrics_mp_checkout_dweb_v2',
 'wandajuan.xo_funnel_action_mp_checkout_dweb_v2')
 
select 
		session_id, visitor_id, bool_or(cast(isusersignedin as boolean))
--		dt, ts, visitor_id, app, isusersignedin, action, session_id 
from wandajuan.xo_funnel_action_mp_checkout_dweb_v2
where session_id in 
( 
select session_id
from wandajuan.xo_funnel_action_mp_checkout_dweb_v2
group by session_id
having count(distinct isusersignedin) > 1
)
group by 1, 2
order by 1, 2
order by dt, visitor_id
, ts 


select dt, ts, visitor_id, app, isusersignedin, action, session_id 
from wandajuan.xo_funnel_action_mp_checkout_dweb_v2
where visitor_id in (

'5eb58af5-b503-491e-8b9f-750a43b07bad',
'2e8d9f19-d387-427b-bfba-e38be739de2c',
'64287fe8-7c2f-44b9-b050-dd751ee0ecff',
'c0b03871-6155-4b5b-bd5a-4509704a3d6c',
'e1e36ec1-2cd6-47f8-bd91-5b56135ee241'
)


select dt, visitor_id, session_id, cast(bool_or(cast(isusersignedin as boolean)) as int) + cast(bool_and(cast(isusersignedin as boolean)) as int) as signinstatus_xo
from wandajuan.xo_funnel_action_mp_checkout_dweb_v2
group by 1, 2, 3

 select * from wandajuan.mp_checkout_dweb_v2
 
 
 select * from wandajuan.visitor_base_mp_checkout_dweb_v2
 
 -- xo
 select count(*), count(distinct session_id) from wandajuan.xo_metrics_mp_checkout_dweb_v2
 
 
 -- xo funnel
 select dt, test_variant, 
 			count(distinct t1.visitor_id), 
 			1.0000*sum(checkout_order_confirmation)/count(distinct visitor_id) xo_per_visitor
 from wandajuan.xo_metrics_mp_checkout_dweb_v2 t1
 group by 1, 2
 
 
 -- mp
 select is_trade_program, count(*), count(distinct session_id) from wandajuan.mp_metrics_mp_checkout_dweb_v2
 group by 1
 
 
 select t1.dt, t1.test_variant, count(distinct t1.visitor_id), count(t1.visitor_id)
 from wandajuan.xo_metrics_mp_checkout_dweb_v2 t1
 group by 1, 2

 select 
 			t1.dt, 
 			t1.test_variant, count(distinct t1.visitor_id), count(t1.visitor_id),
 			sum(item_gmv+placed_order_item_coupon_amount) gmv_w_coupon,
 			sum(item_gmv+placed_order_item_coupon_amount)/count(distinct t1.visitor_id) avg_gmv_w_coupon
 from wandajuan.xo_metrics_mp_checkout_dweb_v2 t1
 left join wandajuan.mp_metrics_mp_checkout_dweb_v2 t2
-- on t1.visitor_id = t2.visitor_id and t1.dt = t2.order_date
 on t1.session_id = t2.session_id
group by 1, 2
 
select * from wandajuan.mp_metrics_mp_checkout_dweb_v2



select * from shahidhya.mp_view_cart_jukwaa_migration_dweb
where visitor_id = '6633e6ea-419d-4cbe-8170-3c12c07852d4'


select * from mp.cal_dim base
join wandajuan.visitor_base_mp_checkout_dweb_v2 vis
on 1=1
left join wandajuan.mp_metrics_mp_checkout_dweb_v2 mp
on vis.visitor_id = mp.visitor_id

where base.date between '2022-04-29' and '2022-05-04' 


-- before dimension

with xo as (

select dt, test_variant, visitor_id, signin_status, session_id,

	-- xo metrics
		sum(coalesce(browse_products, 0)) browse_products,
		sum(coalesce(view_product, 0)) view_product,
 	    sum(coalesce(sm.cart_add,0)) as cart_add,
        sum(coalesce(sm.view_cart,0)) as view_cart,
	    sum(coalesce(sm.checkout_launch,0)) as checkout_launch,
    	sum(coalesce(sm.checkout_shipping,0)) as checkout_shipping,
    	sum(coalesce(sm.checkout_billing_and_payment,0)) as checkout_billing_and_payment,
    	sum(coalesce(sm.checkout_order_review,0)) as checkout_order_review,
	    sum(coalesce(sm.checkout_order_confirmation,0)) as checkout_order_confirmation,
	    sum(coalesce(sm.signup, 0)) as signup

from wandajuan.xo_metrics_mp_checkout_dweb_v2 sm
group by 1, 2, 3, 4, 5

)

, mp as (

select 

		order_date, test_variant, visitor_id,
		session_id,
	
	-- mp metrics
	    coalesce(count(distinct order_id),0) as order_numbers,
    	count(1) units_sold,
	    sum(coalesce(va.item_gmv,0)) as order_gmv,
	    sum(coalesce(va.item_gmv,0)+coalesce(placed_order_item_coupon_amount, 0)) as order_gmv_w_coupon,
	    sum(coalesce(net_commission, 0)) as net_commission,
	    sum(coalesce(net_commission, 0)+coalesce(placed_order_item_coupon_amount, 0)) as net_commission_w_coupon,
    	coalesce(count(distinct va.new_orders),0) as new_orders,
    	coalesce(count(distinct va.old_orders),0) as old_orders,    
	    coalesce(count(distinct cc_orders), 0) as cc_orders,
	    coalesce(count(distinct apple_pay_orders), 0) as apple_pay_orders,
	    coalesce(count(distinct afterpay_orders), 0) as afterpay_orders,
	    coalesce(count(distinct paypal_orders), 0) as paypal_orders,
	    sum(coalesce(cc_gmv, 0)) as cc_gmv,
	    sum(coalesce(apple_pay_gmv, 0)) as apple_pay_gmv,
	    sum(coalesce(afterpay_gmv, 0)) as afterpay_gmv,
	    sum(coalesce(paypal_gmv, 0)) as paypal_gmv
 from wandajuan.mp_metrics_mp_checkout_dweb_v2 va
 group by 1, 2, 3, 4
)
select *  from mp
where session_id is null

where session_id in (
select session_id
from mp
group by 1
having count(*)>1
)


, xo_sign as (

select dt, visitor_id, session_id, cast(bool_or(cast(isusersignedin as boolean)) as int) + cast(bool_and(cast(isusersignedin as boolean)) as int) as signinstatus_xo
from wandajuan.xo_funnel_action_mp_checkout_dweb_v2
group by 1, 2, 3

)

select 
		coalesce(order_date, xo.dt) dt,
		coalesce(xo.test_variant, mp.test_variant) test_variant,
		coalesce(mp.visitor_id, xo.visitor_id) visitor_id,
		coalesce(mp.session_id, xo.session_id) session_id,
		signin_status,
		signinstatus_xo,
		
		
		browse_products,
		view_product,
		cart_add,
		view_cart,
		checkout_launch,
		checkout_shipping,
		checkout_billing_and_payment,
		checkout_order_review,
		checkout_order_confirmation,
		signup,
		
		order_numbers,
		units_sold,
		order_gmv,
		order_gmv_w_coupon,
		net_commission,
		net_commission_w_coupon,
		new_orders,
		old_orders,    
		cc_orders,
		apple_pay_orders,
		afterpay_orders,
		paypal_orders,
		cc_gmv,
		apple_pay_gmv,
		afterpay_gmv,
		paypal_gmv
from xo
full join mp
--on xo.visitor_id = mp.visitor_id and xo.dt = mp.order_date
on xo.session_id = mp.session_id
left join xo_sign
on coalesce(mp.session_id, xo.session_id) = xo_sign.session_id
group by 1, 2



select 
		test_variant,
        cast(crc32(to_utf8(visitor_id)) % 100 as int) as bucket
        COUNT(DISTINCT visitor_id) as visitors,

		sum(browse_products) as browse_products,
		sum(view_product) as view_product,
		sum(cart_add) as cart_add,
		sum(view_cart) as view_cart,
		sum(checkout_launch) as checkout_launch,
		sum(checkout_shipping) as checkout_shipping,
		sum(checkout_billing_and_payment) as checkout_billing_and_payment,
		sum(checkout_order_review) as checkout_order_review,
		sum(checkout_order_confirmation) as checkout_order_confirmation,
		sum(signup) as signup,
		
		sum(order_numbers) as order_numbers,
		sum(units_sold) as units_sold,
		sum(order_gmv) as order_gmv,
		sum(order_gmv_w_coupon) as order_gmv_w_coupon,
		sum(net_commission) as net_commission,
		sum(net_commission_w_coupon) as net_commission_w_coupon,
		sum(new_orders) as new_orders,
		sum(old_orders) as old_orders,    
		sum(cc_orders) as cc_orders,
		sum(apple_pay_orders) as apple_pay_orders,
		sum(afterpay_orders) as afterpay_orders,
		sum(paypal_orders) as paypal_orders,
		sum(cc_gmv) as cc_gmv,
		sum(apple_pay_gmv) as apple_pay_gmv,
		sum(afterpay_gmv) as afterpay_gmv,
		sum(paypal_gmv) as paypal_gmv
from wandajuan.mp_checkout_dweb_v2


select count(*), count(distinct session_id) from wandajuan.mp_checkout_dweb_v2_by_signinstatus


with t as (
select test_variant, visitor_id, session_id, 
		count(*) units_sold_per_xo, 
		sum(item_gmv) gmv_per_xo, 
		count(distinct house_id) nunique_item_per_xo 
from wandajuan.mp_metrics_mp_checkout_dweb_v2
group by 1, 2, 3
--having count(*) > 1
order by 4 desc
)
select test_variant, 
		count(*) xo,
		count(distinct visitor_id) n_visitors,
		avg(units_sold_per_xo) avg_units_sold_per_xo,
		avg(gmv_per_xo) avg_gmv_per_xo,
		avg(nunique_item_per_xo) nunique_item_per_xo
from t
group by 1
		