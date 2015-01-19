DROP TABLE IF EXISTS tmp_Coupons_Show_Print_Use_by_Goods_v2;
CREATE EXTERNAL TABLE tmp_Coupons_Show_Print_Use_by_Goods_v2
(
  Good_ID string,
  Good_Name string,
  n_Show float,
  n_Print float,
  n_Use float,
  pc_of_Print float,
  pc_of_Use float,
  AVGCost float,
  AVG_Cost_by_Use_With_Discount string,
  ExpensiveCategory int,
  Classification_Level_2 string,
  Classification_Level_3 string,
  Brand_ID string,
  Brand_Name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/senov/Coupons_Show_Print_Use_by_Goods_v2';



DROP TABLE IF EXISTS tmp_goods_purchases_count_before;
CREATE TABLE tmp_goods_purchases_count_before AS
SELECT g.good_id, SUM(IF(g.first_year_month > t.year_month, t.cnt, 0)) as before_purchases,   SUM(t.cnt) as all_purchases
FROM
(
  SELECT sku as good_id, min(from_unixtime(unix_timestamp(startdate), 'yyyy-MM'))  as first_year_month
  FROM coupons_list
  GROUP BY good_id
) g
JOIN tmp_goods_year_month_count t ON t.good_id = g.good_id
GROUP BY good_id;




-- SELECT
--   n_Show ,
--   n_Print ,
--   n_Use ,
--   IF(n_Show > 0, (n_Print / n_Show), 0) as pc_show_print,
--   IF(n_Print > 0, (n_Use/ n_Print), 0)  as pc_print_use,
--   IF(n_Show > 0, (n_Use / n_Show), 0)  as pc_show_use,
--   AVGCost,
--   IF(AVG_Cost_by_Use_With_Discount = 'null', 0, cast(AVG_Cost_by_Use_With_Discount as float)) as AVG_Cost_by_Use_With_Discount,
--   ExpensiveCategory,
--   Brand_Name,
--   concat_ws('|', Classification_Level_2, Classification_Level_3) as group23,
--   IF(before_purchases IS NULL, 0 , before_purchases) as before_purchases,
--   IF(all_purchases IS NULL, 0 , all_purchases) as all_purchases
-- FROM tmp_Coupons_Show_Print_Use_by_Goods_v2 t
-- LEFT OUTER JOIN tmp_goods_purchases_count_before tt
--   ON t.good_id = tt.good_id
--   ;
--




select good_name, cnt from (select product_id, count(*) as cnt from bk_transactions group by product_id) t join goods g on t.product_id = g.id;

select cnt, sr
from (
  select good_name, cnt from (select product_id, count(*) as cnt from bk_transactions group by product_id) t join goods g on t.product_id = g.id
) t1
join (
  select good_name, sum(rank) as sr from tmp_consumer_lda_offers_all_goods group by good_name
) t2
 ON t1.good_name = t2.good_name
limit 10000


select good_name, sum(cnt) as coverage
from (
  select good_name, count(*) as cnt
  from tmp_consumer_lda_offers_all_goods_groups_alt
  where rank > 90
  group by good_name
) t
group by good_name
order by coverage desc limit 50
;



drop table if exists tmp_group_consumer_coverage;
create table tmp_group_consumer_coverage as
select group_id, count(distinct r.consumer_id) as coverage
from bk_products pg
join tmp_lda_offers_all_goods_groups_alt r
  ON pg.product_id = r.product_id
left outer join goods_recommended_info gri
  ON r.product_id = gri.good_id
WHERE r.rank > 88 AND (gri.recommended_goods_exception is null or gri.recommended_goods_exception != 1)
group by group_id
;

drop table if exists tmp_good_consumer_coverage;
create table tmp_good_consumer_coverage as
select good_id, good_name, count(distinct r.consumer_id) as coverage
from tmp_lda_offers_all_goods_groups_alt r
join goods g
  ON g.id = r.product_id
left outer join goods_recommended_info gri
  ON r.product_id = gri.good_id
WHERE r.rank > 88 AND (gri.recommended_goods_exception is null or gri.recommended_goods_exception != 1)
group by good_id, good_name
;


select avg(cnt), max(cnt), min(cnt)
from (
  select consumer_id, SUM(IF(gri.recommended_goods_exception IS NULL, 0, gri.recommended_goods_exception)) as cnt
  from tmp_lda_offers_all_goods_groups_alt r
  left outer join goods_recommended_info gri
    ON r.product_id = gri.good_id
    where r.rank > 88
  group by consumer_id
) t
;


select count(distinct consumer_id)
from tmp_lda_offers_all_goods_groups_alt r
join bk_products pg
  ON pg.product_id = r.product_id
JOIN tmp_group_consumer_coverage gcc
  ON pg.group_id = gcc.group_id
WHERE gcc.coverage >= 236072
;
select count(distinct consumer_id)
from tmp_lda_offers_all_goods_groups_alt r
join bk_products pg
  ON pg.product_id = r.product_id
JOIN tmp_group_consumer_coverage gcc
  ON pg.group_id = gcc.group_id
WHERE gcc.coverage >= 236072 AND r.rank > 88
;

select count(distinct consumer_id)
from tmp_lda_offers_all_goods_groups_alt r
JOIN tmp_good_consumer_coverage gcc
  ON r.product_id = gcc.good_id
WHERE gcc.coverage > 1000
;
select count(distinct consumer_id)
from tmp_lda_offers_all_goods_groups_alt r
JOIN tmp_good_consumer_coverage gcc
  ON r.product_id = gcc.good_id
WHERE gcc.coverage > 10000 AND r.rank > 88
;




DROP TABLE IF EXISTS tmp_chosen_groups_4_coupons;
CREATE EXTERNAL TABLE tmp_chosen_groups_4_coupons
(
  group_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/senov/chosen_groups_4_coupons'
;


select good_id, good_name, coverage
from tmp_good_consumer_coverage gcc
join bk_products pg
  ON pg.product_id = gcc.good_id
join tmp_chosen_groups_4_coupons tc
  ON tc.group_id = pg.group_id
  ;

