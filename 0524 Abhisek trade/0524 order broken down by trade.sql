with t as (

select 
    DATE(CAST(m.created as TIMESTAMP)) as dt,
    DATE(CAST(m.settlement_date as TIMESTAMP)) as sett_dt,
    m.user_id,
    m.checkout_id,
    m.order_id,
    t.full_name as TAM_name,
    t.trade_sales_flag, --
    if(os.paid_date <= 30,1,0) as paid_any_touch, -- 
    m.acc_class, 
    m.coupon_reason,
    m.seller_type,

    case
        when m.coupon_reason = 10 then 'TRADE_PROGRAM_CREDIT'
        when m.coupon_reason = 15 then 'TRADE_SIGNUP_PROMOTION'
        when m.coupon_reason = 2 then 'GENERAL_MARKETING'
        when m.coupon_reason = 19 then 'TRADE_PROGRAM_FIRST_ORDER_PROMO'
        when m.coupon_reason = 4 then 'PRICE_MATCH'
        when m.coupon_reason = 11 then 'INCENTIVE_FOR_REVIEWS'
        when m.coupon_reason = 1 then 'SHOP_HOUZZ_PROMOTION'
        when m.coupon_reason = 6 then 'CUSTOMER_SERVICE_CONCESSION'
        when m.coupon_reason = 14 then 'EMPLOYEE_COUPON'
        when m.coupon_reason = 9 then 'TRADE_PROGRAM_REFERRAL_CODE'
        when m.coupon_reason = 18 then 'USER_REFERRAL'
        when m.coupon_reason = 13 then 'COUPON_FOR_GIFT_CARD'
        when m.coupon_reason = 17 then 'PROACTIVE_DISCOUNT'
        when m.coupon_reason = 3 then 'COUPON_REPLACEMENT'
        when m.coupon_reason = 12 then 'MP_SELLER_CREATED_COUPONS'
        when m.coupon_reason = 16 then 'TRADE_PROGRAM_MARKETING'
        when m.coupon_reason = 7 then 'REFUND_AS_STORE_CREDIT'
        when m.coupon_reason = 20 then 'ADS_REWARD'
        when m.coupon_reason = 8 then 'SALES_OFFER'
        else 'N/A' end as coupon_reason_desc,

    sum(gmv) as gmv,
    count(distinct checkout_id) count_checkouts,
    sum(initial_margin)  initial_margin,
    sum(adjusted_cm) adjusted_cm,
    sum(initial_margin-coupons_promotions) initial_margin_w_coupons,
    sum(coupons_promotions) coupons_promotions,
    sum(trade_credit_issued) trade_credit_issued,
    sum(trade_credit_redeemed) trade_credit_redeemed,
    sum(return_allowances) return_allowances,
    sum(goodwill_refunds) goodwill_refunds,
    sum(sales_return_cogs) sales_return_cogs,
    sum(vendor_penalty) vendor_penalty,
    sum(chargebacks_est) chargebacks_est
 

    from mp.order_metrics_with_adjustments as m

    left join 
        (
        select 

            order_id,
            full_name,
            case 
                when assigned_flag = 'TRUE' then 'TAM'
                when assisted_flag = 'TRUE' then 'TSR' end as trade_sales_flag

        from
            shop.master_trade_ideabook_pros_all

        ) as t
    on m.order_id = t.order_id
    
    left join 
    	(
    	select  *,
    			row_number() over (partition by order_id order by start_ts desc) rk,
                date_diff('day', cast(order_date as date), cast(session_dt as date)) paid_date
    	from dm.order_sess 
    	where medium = 'PAID'
    	and dt >= '2019-01-01'
    ) os
	on m.order_id = os.order_id and os.rk = 1 and substr(m.created, 1, 10) = os.order_date

    where 
        YEAR(CAST(m.created as TIMESTAMP)) > 2018
        and m.order_status in (0,1,2,3,4,5,20,99)
        and m.is_replacement_order = 0
        AND m.order_id NOT IN (select order_id from logs.marketplace_gift_cards_purchased)

    GROUP BY DATE(CAST(m.created as TIMESTAMP)),
        DATE(CAST(m.settlement_date as TIMESTAMP)),
        m.user_id,
        m.checkout_id,
        m.order_id,
        t.full_name,
        t.trade_sales_flag,
        if(os.paid_date <= 30,1,0),
        m.acc_class,
        m.coupon_reason,
        m.seller_type,

        case
            when m.coupon_reason = 10 then 'TRADE_PROGRAM_CREDIT'
            when m.coupon_reason = 15 then 'TRADE_SIGNUP_PROMOTION'
            when m.coupon_reason = 2 then 'GENERAL_MARKETING'
            when m.coupon_reason = 19 then 'TRADE_PROGRAM_FIRST_ORDER_PROMO'
            when m.coupon_reason = 4 then 'PRICE_MATCH'
            when m.coupon_reason = 11 then 'INCENTIVE_FOR_REVIEWS'
            when m.coupon_reason = 1 then 'SHOP_HOUZZ_PROMOTION'
            when m.coupon_reason = 6 then 'CUSTOMER_SERVICE_CONCESSION'
            when m.coupon_reason = 14 then 'EMPLOYEE_COUPON'
            when m.coupon_reason = 9 then 'TRADE_PROGRAM_REFERRAL_CODE'
            when m.coupon_reason = 18 then 'USER_REFERRAL'
            when m.coupon_reason = 13 then 'COUPON_FOR_GIFT_CARD'
            when m.coupon_reason = 17 then 'PROACTIVE_DISCOUNT'
            when m.coupon_reason = 3 then 'COUPON_REPLACEMENT'
            when m.coupon_reason = 12 then 'MP_SELLER_CREATED_COUPONS'
            when m.coupon_reason = 16 then 'TRADE_PROGRAM_MARKETING'
            when m.coupon_reason = 7 then 'REFUND_AS_STORE_CREDIT'
            when m.coupon_reason = 20 then 'ADS_REWARD'
            when m.coupon_reason = 8 then 'SALES_OFFER'
            else 'N/A' end
)

select 
	
	case acc_class when '20 - Trade' then coalesce(trade_sales_flag, 'Organic Trade')
				   when '10 - Consumer' then 'Consumer'
				   when '30 - Pro' then 'Organic Pro'
		else 'NA' end as source, 
	paid_any_touch,
	date_trunc('month', sett_dt) sett_mt,
	sum(gmv) gmv,
	count(distinct order_id) order_cnt
from t
where seller_type != 7
group by 1, 2, 3