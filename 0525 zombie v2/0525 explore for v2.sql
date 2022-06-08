
-- 98969	98969 --> change every run
select 
        COUNT(*), COUNT(DISTINCT house_id)	
from (
    select *,
        ntile(10) over (partition by leaf_category_name order by num_reviews + num_ext_reviews desc) review_decile
    from shop.product_master_table_daily 
    where dt = '2022-02-16'
) pmt
where 
seller_type_desc = 'Direct'
and top_leaf <= 200
and (gl_pla_spend_90d = 0 or gl_pla_spend_90d is null)
and adjusted_cm_rate > 0.1
and weighted_rating >= 4 and review_decile <= 2


-- 98980
select count(*) from dm.zombie_skus 




-- 
with gl as (
select dt, house_id, 
		sum(imps) imps,
		sum(clicks) clicks,
		sum(spend) spend,
		sum(conv_value) conv_value
from dm.gl_pla_performance 
where dt >= '2022-02-26'
group by 1, 2
)
select gl.*
from gl
join dm.zombie_skus z
on gl.house_id = z.house_id



--

select 
       dt, COUNT(*), COUNT(DISTINCT house_id)	
from (
    select *,
        ntile(10) over (partition by dt, leaf_category_name order by coalesce(num_reviews, 0) + coalesce(num_ext_reviews, 0) desc) review_decile
    from shop.product_master_table_daily 
    where dt in ('2022-02-16', '2022-03-16', '2022-04-16', '2022-05-16', '2022-05-23')
) pmt
where 
seller_type_desc = 'Direct'
and top_leaf <= 200
and (gl_pla_spend_90d = 0 or gl_pla_spend_90d is null)
and adjusted_cm_rate > 0.1
and weighted_rating >= 3 
and review_decile <= q
group by 1
order by 1


select 
       COUNT(*), COUNT(DISTINCT house_id)	
from (
    select *,
        ntile(10) over (partition by dt, leaf_category_name order by coalesce(num_reviews, 0) + coalesce(num_ext_reviews, 0) desc) review_decile
    from shop.product_master_table_daily 
    where dt = '2022-05-23'
) pmt
where 
seller_type_desc = 'Direct'
and top_leaf <= 200
and (gl_pla_spend_90d = 0 or gl_pla_spend_90d is null)
and adjusted_cm_rate > 0.1
and (weighted_rating >= 4 or avg_rating_ext >= 4)
and review_decile <= 2
group by 1
order by 1


select 
       if(top_leaf <= 200, 1, 0) if_top200_leaf,
       ceil(weighted_rating) weighted_rating,
       review_decile,
       ceil(avg_rating_ext) avg_rating_ext, 
--       COUNT(*), 
       COUNT(DISTINCT house_id)	product_cnt
from (
    select *,
        ntile(10) over (partition by leaf_category_name order by coalesce(num_reviews, 0) + coalesce(num_ext_reviews, 0) desc) review_decile
    from shop.product_master_table_daily 
    where dt = '2022-05-23'
) pmt
where 
seller_type_desc = 'Direct'
--and top_leaf <= 200
and (gl_pla_spend_90d = 0 or gl_pla_spend_90d is null)
and adjusted_cm_rate > 0.1
--and weighted_rating >= 3 and review_decile <= 3
group by 1, 2, 3, 4




with pmt as (
    select *,
        ntile(10) over (partition by leaf_category_name order by num_reviews + num_ext_reviews desc) review_decile
    from shop.product_master_table_daily 
    where dt = '2022-05-23'
)

select *
from pmt
where seller_type_desc = 'Direct'
--and top_leaf <= 200
and (gl_pla_spend_90d = 0 or gl_pla_spend_90d is null)
and adjusted_cm_rate > 0.1
--and weighted_rating >= 3 and review_decile <= 3
and weighted_rating is null 




select * from ranking.weighted_product_ratings 