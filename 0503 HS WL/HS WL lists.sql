-- HS WL list

select leaf_category_name, sum(item_gmv_90d) gmv_90d  from shop.product_master_table_v1 
where vendor_name = 'Homesquare'
group by 1
order by 2 desc


-- top 2 leafs from HS
--desks and hutches
--bar stools and counter stools

select leaf_category_name, house_id, product_code, item_gmv_90d 
from shop.product_master_table_daily 
where vendor_name = 'Homesquare'
and leaf_category_name
= 'desks and hutches'
and dt = '2022-05-24'
order by 4 desc
limit 20

select leaf_category_name, house_id, product_code, item_gmv_90d 
from shop.product_master_table_daily 
where vendor_name = 'Homesquare'
and leaf_category_name
= 'bar stools and counter stools'
and dt = '2022-05-24'
order by 4 desc
limit 20

