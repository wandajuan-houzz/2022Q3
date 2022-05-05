-- v3

-- sanity check v3 for Amazon crawler validation in phase 2 


-- 2022-03-30 should be the day of use -> 99.997% coverage
select dt, count(distinct pmt.house_id), count(distinct c.house_id)
from shop.product_master_table_daily pmt
left join c2.comp_products_crawler c
on pmt.house_id = c.house_id
where pmt.dt >= '2022-03-15'
and seller_type_desc = 'Direct' -- direct
and availability = 1 and quantity > 0 -- instock
group by 1



-- coverage

select count(distinct pmt.house_id) n_inputs,
		count(distinct c.house_id) n_added,
		count(distinct if(c.upc is not null and c.upc!='', c.house_id, NULL)) n_w_upc,
		count(distinct if(c.crawled_time is not null, c.house_id, NULL)) n_crawled,
		count(distinct if(c.price > 0, c.house_id, NULL)) n_crawled_w_price
from shop.product_master_table_daily pmt
left join c2.comp_products_crawler c
on pmt.house_id = c.house_id
where pmt.dt = '2022-03-30'
and seller_type_desc = 'Direct' -- direct
and availability = 1 and quantity > 0 -- instock





select starsBreakdown, count(*) from wandajuan.tmp_amz_validation_v1
group by 1
order by 2 desc


-- table for accuarcy validation
drop table wandajuan.tmp_amz_validation_v1
create table wandajuan.tmp_amz_validation_v1 as (
with amz as (
-- crawled products
select *,
		json_extract_scalar("attribute", '$.asin') asin
		,
		json_extract_scalar("attribute", '$.url') url,
		json_extract_scalar("attribute", '$.titleSearched') titleSearched,
		json_extract_scalar("attribute", '$.titleCrawled') titleCrawled,
		cast(json_extract_scalar("attribute", '$.titleSimilarity') as double) titleSimilarity,
		json_extract_scalar("attribute", '$.price') price_2,  -- the default price field is still not good
		cast(replace(regexp_extract(replace(json_extract_scalar("attribute", '$.price'), '$'), '([0-9.,]*)'), ',') as decimal(10, 2)) amz_price,
		json_extract_scalar("attribute", '$.rating') rating,
		json_extract_scalar("attribute", '$.numberOfReviews') numberOfReviews,
		json_extract_scalar("attribute", '$.organicFound') organicFound,
		json_extract_scalar("attribute", '$.adsFound') adsFound
--		,json_extract("attribute", '$.starsBreakdown') starsBreakdown   -- the format seems to fall off
from c2.comp_products_crawler 
where crawled_time is not null and price > 0
)

-- 1)  price format issue
--select *
--from amz
--where price != amz_price

-- 2) starsBreakdown format falls off
--select starsBreakdown, count(*)
--from amz
--group by 1
--order by 2 desc

, t as (
select amz.*,
		pa.mpn as mpn_hz,
		pmt.final_display_price,
		1.0000*amz_price/final_display_price -1 as price_diff_perc,
		amz_price - final_display_price as price_diff,
		amz.mpn = pa.mpn if_same_mpn,
		ceil(titleSimilarity*10)*0.1 titleSimilarityBin
from shop.product_master_table_daily pmt
join amz
on pmt.house_id = amz.house_id
left join c2.product_attributes pa
on pmt.house_id = pa.house_id
where pmt.dt = '2022-03-30'
and pmt.trade_type is null
--and ceil(titleSimilarity*10)*0.1 = 0
)


--select ceil(titleSimilarity*10)*0.1 titleSimilarityBin, crawled_time, count(*)
--from t
--group by 1, 2

--, t2 as (
-- table with all needed fields for validation
select 	*,	
		case when price_diff_perc > 1 then '8_>100%'
			when price_diff_perc > 0.5 then '7_(50%, 100%]'
			when price_diff_perc > 0.25 then '6_(25%, 50%]'
			when price_diff_perc > 0 then '5_(0, 25%]'
			when price_diff_perc > -0.25 then '4_(-25%, 0%]'
			when price_diff_perc > -0.5 then '3_(-50%, -25%]'
			when price_diff_perc > -0.75 then '2_(-75%, -50%]'
			when price_diff_perc > -1 then '1_(-100%, -75%]'
			when price_diff_perc <= -1 then '0_<-100%'
			else 'other'
			end as price_diff_perc_bin
from t 
)





select if_same_mpn,
		titleSimilarityBin,
		price_diff_perc_bin,
		count(*)
from wandajuan.tmp_amz_validation_v1
group by 1, 2, 3





select * from wandajuan.tmp_amz_validation_v1
where house_id = 152199364

-- if same mpn
select 
--		* 
		house_id,	
		upc,
		mpn_hz,
		mpn,
		'https://houzz.com/photos/'||cast(house_id as varchar) hz_link,
		url,
		titleSearched,
		titleCrawled,
		titleSimilarity,
		final_display_price,
		price,
		amz_price,
		price_diff_perc,
		price_diff,
		titleSimilarityBin,
		price_diff_perc_bin
--		,rnk
from wandajuan.tmp_amz_validation_v1
where if_same_mpn = true 
order by titleSimilarityBin asc, price_diff_perc_bin desc
limit 10

, abs(price_diff_perc) desc



where if_same_mpn is true
and tit



, t3 as (
select *,
		row_number() over (partition by titleSimilarityBin, price_diff_perc_bin order by abs(price_diff) desc) rnk
from t2
)
select 
--		*
		house_id,	
		upc,
		'https://houzz.com/photos/'||cast(house_id as varchar) hz_link,
		url,
		titleSearched,
		titleCrawled,
		titleSimilarity,
		final_display_price,
		price,
		amz_price,
		price_diff_perc,
		price_diff,
		titleSimilarityBin,
		price_diff_perc_bin,
		rnk
from t3
where rnk <= 5

select 	
		pmt.house_id, 
		c.house_id,
		c.upc,
--		trade_type,
--		c.*,
--		pmt.vendor_name, 
--		pmt.l1_category_name, 
--		pmt.leaf_category_name,
		'https://houzz.com/photos/'||cast(c.house_id as varchar) hz_link,
		pmt.final_display_price,
--		pmt.item_gmv_1yr, 
--		pmt.num_reviews as num_int_reviews,
--		pmt.rating avg_rating_int,
--		pmt.num_ext_reviews,
--		pmt.avg_rating_ext,
--		pmt.has_comp_price,
--		pmt.min_competitor_price,
--		pmt.is_price_competitive,
		c.price,
--		json_extract_scalar("attribute", '$.asin') asin,
		json_extract_scalar("attribute", '$.url') url,
--		json_extract_scalar("attribute", '$.titleCrawled') titleCrawled,
		json_extract_scalar("attribute", '$.price') price_2,
		cast(replace(regexp_extract(replace(json_extract_scalar("attribute", '$.price'), '$'), '([0-9.,]*)'), ',') as decimal(10, 2)) amz_price
--		json_extract_scalar("attribute", '$.rating') rating,
--		json_extract_scalar("attribute", '$.numberOfReviews') numberOfReviews,
--		json_extract_scalar("attribute", '$.organicFound') organicFound,
--		json_extract_scalar("attribute", '$.adsFound') adsFound,
--		json_extract("attribute", '$.starsBreakdown') starsBreakdown
from c2.comp_products_crawler c 
-- tablesample bernoulli(0.05e0)
--join shop.product_master_table_v2 pmt
--on c.house_id = pmt.house_id
where crawled_time is not null and c.price > 0
and trade_type is null