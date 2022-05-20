
-- 1530
select test_group, device_cat, count(distinct visitor_id) n_visitors,
		sum(browse_products) browse_products,
		sum(search_products) search_products,
		sum(view_product) view_product,
		sum(cart_add) cart_add,
		sum(checkout_launch) checkout_launch
from abtest.web_visitor_metrics 
where test_name = 'pla_page_command_to_products'
and dt = '2022-05-16'
--and test_group like '%treatment%'
group by 1, 2


select * from l2.test_selection 
where test_name = 'pla_page_command_to_products'
and dt = '2022-05-16'



select * from l2.session_analytics 
where dt = '2022-05-16'
and test_set like '%pla_page_command_to_products=treatment%'
and 