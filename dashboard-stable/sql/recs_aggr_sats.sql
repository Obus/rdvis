-- Required consumer_goods_S_recs, consumer_goods_LG_recs (see simplate_printer_recs.sql)


-- in hive only
-- ######################################################################|
DROP TABLE IF EXISTS S_recs_aggr_info_basket_offer;
CREATE TABLE S_recs_aggr_info_basket_offer AS
SELECT place, source, count(*) as cnt
FROM
(
  SELECT discount_card_id, source, bk_rank(discount_card_id) as place
  FROM
  (
      SELECT
          discount_card_id,
          rank,
          IF(bought_already > 0, 'basket', 'offer') as source
      FROM consumer_goods_S_recs
      DISTRIBUTE BY discount_card_id
      SORT BY discount_card_id, rank DESC
  ) t
) t
GROUP BY place, source
;

-- ######################################################################|
DROP TABLE IF EXISTS LG_recs_aggr_info_basket_offer;
CREATE TABLE LG_recs_aggr_info_basket_offer AS
SELECT place, source, count(*) as cnt
FROM
(
  SELECT discount_card_id, source, bk_rank(discount_card_id) as place
  FROM
  (
      SELECT
          discount_card_id,
          rank,
          IF(bought_already > 0, 'basket', 'offer') as source
      FROM consumer_goods_LG_recs
      DISTRIBUTE BY discount_card_id
      SORT BY discount_card_id, rank DESC
  ) t
) t
GROUP BY place, source
;


DROP TABLE IF EXISTS tmp_good_id_group23;
CREATE TABLE tmp_good_id_group23 AS
SELECT good_id, concat_ws('|', Classification_Level_2, Classification_Level_3) as group23 FROM goods_retail_classificator
;


DROP TABLE IF EXISTS tmp_group23_periodicity;
CREATE TABLE tmp_group23_periodicity AS
SELECT group23, AVG((max_timestamp - min_timestamp) / cnt / 24 / 3600) as avg_periodicity
FROM
(
  SELECT consumer_id, gg.group23, max(timestamp) as max_timestamp, min(timestamp) as min_timestamp, count(*) as cnt
  FROM bk_transactions t
  join tmp_good_id_group23 gg ON t.product_id = gg.good_id
  WHERE consumer_id != '0000000000000'
  GROUP BY group23, consumer_id
) t
WHERE cnt > 2
GROUP BY group23
;

DROP TABLE IF EXISTS S_recs_periodicity;
CREATE TABLE S_recs_periodicity ()

-- ######################################################################|
DROP TABLE IF EXISTS coupons_year_month_stats;
CREATE TABLE coupons_year_month_stats (year_month string, stat_name string, val float);

CREATE TABLE tmp_goods_year_month_count AS
SELECT good_id, year_month, count(distinct cheque_id) as cnt
FROM
(
  SELECT product_id as good_id, cheque_id, concat_ws('-', cast(year(from_unixtime(t.`timestamp`)) as string),  cast(month(from_unixtime(t.`timestamp`)) as string))  as year_month
  FROM bk_transactions t
) t
GROUP BY good_id, year_month
;

INSERT INTO TABLE coupons_year_month_stats
SELECT year_month, 'before month purchases', sum(cnt)
FROM
(
  SELECT cl.good_id, cl.year_month, sum(t.cnt) as cnt
  FROM
  (
    SELECT
      sku AS good_id,
      concat_ws('-', cast(year(startdate) as string), cast(month(startdate) as string)) as year_month
    FROM coupons_list
    WHERE year(startdate) <= year(enddate) and month(startdate) <= month(enddate)
  ) cl
  JOIN tmp_goods_year_month_count t
    ON t.good_id = cl.good_id
  WHERE t.year_month < cl.year_month
  GROUP BY cl.good_id, cl.year_month
) t
GROUP BY year_month
;

INSERT INTO TABLE coupons_year_month_stats
SELECT year_month, 'in month purchases', sum(cnt)
FROM
(
  SELECT cl.good_id, cl.year_month, sum(t.cnt) as cnt
  FROM
  (
    SELECT
      sku AS good_id,
      concat_ws('-', cast(year(startdate) as string), cast(month(startdate) as string)) as year_month
    FROM coupons_list
    WHERE year(startdate) <= year(enddate) and month(startdate) <= month(enddate)
  ) cl
  JOIN tmp_goods_year_month_count t
    ON t.good_id = cl.good_id
  WHERE t.year_month = cl.year_month
  GROUP BY cl.good_id, cl.year_month
) t
GROUP BY year_month
;

INSERT INTO TABLE coupons_year_month_stats
SELECT year_month, 'goods count', count(distinct good_id)
FROM
(
  SELECT
    sku AS good_id,
    concat_ws('-', cast(year(startdate) as string), cast(month(startdate) as string)) as year_month
  FROM coupons_list
  WHERE year(startdate) <= year(enddate) and month(startdate) <= month(enddate)
) cl
GROUP BY year_month
;


-- ######################################################################