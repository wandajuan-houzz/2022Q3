select 
		po.order_id,
		po.pretty_order_id,
		po.user_id,
		u.name,
		u.first_name,
		u.last_name,
		u.email,
		po.house_id,
		po.sku_name,
		po.status_name,
		po.created,
		po.product_price,
		po.shipping,
		product_price + shipping as order_value,
		po.shipping_address_id,
		add.line1,
		add.line2,
		add.city,
		add.state,
		add.zip,
		add.country,
		add.phone
from mp.order_item_margins_with_replacement po
left join c2.users u
on po.user_id = u.user_id
left join c2.addresses add
on po.shipping_address_id = add.address_id
where po.user_id = 8129743

order_id = 1677053237541654
and order_date = '2022-09-05'


select * from c2.users
where user_id = 8129743

select * from c2.addresses