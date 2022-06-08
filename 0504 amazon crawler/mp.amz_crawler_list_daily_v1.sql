select * from c2.comp_products_crawler

-- crawler base:
-- direct, in stock 
-- 2812193 -> 2043861 (70% w upc) and w upc
select * from shop.product_master_table_v1 pmt
left join c2.product_attributes pa
on pmt.house_id = pa.house_id
where seller_type_desc = 'Direct' -- direct
and availability = 1 and quantity > 0 -- instock
and (upc is not null and trim(upc) != '' and upc not in ('8888888888888', '000000000000'))


select upload_attempted_time is not null,
		download_attempted_time is not null,
		crawled_time is not null,
		count(*)
from c2.comp_products_crawler
group by 1, 2,3 
where upload_attempted_time is not null

-- currently not good
-- too many NULLs in upload_attempted_time, and crawled_time 
-- for a successful crawl, is upload_attempted_time < crawled_time 
-- for a fail crawl, upload_attempted_time > crawled_time or crawled_time is null
select upload_attempted_time is not null,
		upload_attempted_time < crawled_time,
		count(*)
from c2.comp_products_crawler
group by 1, 2
where upload_attempted_time is not null

-- 2M to complete in 14days



-- DS driven Amazon crawler data pipelines
load_with:
    - t1: |-
        select
            pmt.house_id,
            pmt.product_tier,
            upload_attempted_time comp_attempted_time,
            crawled_time comp_crawled_time
        from shop.product_master_table_v2 pmt
        left join c2.comp_products_crawler c
            on pmt.house_id = c.house_id
        join c2.product_attributes pa
            on pa.house_id = c.house_id
        where seller_type_desc ='Direct'
            and (pa.upc is not null and pa.upc != '' and pa.upc not in ('8888888888888', '000000000000'))
            and availability=1
            and quantity>0
    - t2: |-
        SELECT
            *,
            row_number()over(partition by product_tier order by comp_attempted_time desc) as comp_attempted_rk,
            count(*)over(partition by product_tier) as cnt
        FROM t1
load_query: |
    select
        *
    from t2
    where (product_tier = 't1'
        or product_tier = 't2'
        or (product_tier = 't3' and comp_attempted_rk >= cnt*6/7 )
        or (product_tier = 't4' and comp_attempted_rk >= cnt*13/14 )
        or (product_tier = 't5' and comp_attempted_rk >= cnt*13/14 )
        or (product_tier is null and comp_attempted_rk >= cnt*6/7 )
        )