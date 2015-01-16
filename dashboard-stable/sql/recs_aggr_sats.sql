-- Required consumer_goods_S_recs, consumer_goods_LG_recs (see simplate_printer_recs.sql)

--

-- #######################         archive_coupons_4_LG and archive_coupons_4_S tables          ########################

CREATE EXTERNAL TABLE tmp_archive_1417716790_coupon_4_LG_versioned (
  Create_Version string,
  Discount_Card_ID string,
  Coupon_ID int,
  Rank float,
  Rec_Version string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/backup/ulybka_radugi/1417716790/Coupons_4_LG_versioned';

INSERT INTO TABLE tmp_archive_1417716790_coupon_4_LG_versioned
SELECT
  substr(version, 2, 10),
  discount_card_id,
  coupon_id,
  rank,
  rec_version
FROM tmp_coupons_loyalty_generator_bo;



DROP TABLE IF EXISTS archive_coupon_4_LG_versioned;
CREATE EXTERNAL TABLE archive_coupon_4_LG_versioned (
  Create_Version string,
  Discount_Card_ID string,
  Coupon_ID int,
  Rank float,
  Rec_Version string
)
PARTITIONED BY(ctimestamp string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
;
ALTER TABLE archive_coupon_4_LG_versioned ADD PARTITION(ctimestamp='1417716790')
  LOCATION '/backup/ulybka_radugi/1417716790/Coupons_4_LG_versioned';
ALTER TABLE archive_coupon_4_LG_versioned ADD PARTITION(ctimestamp='1421316233')
  LOCATION '/backup/ulybka_radugi/1421316233/Coupons_4_LG_versioned';
ALTER TABLE archive_coupon_4_LG_versioned ADD PARTITION(ctimestamp='1421321302')
  LOCATION '/backup/ulybka_radugi/1421321302/Coupons_4_LG_versioned';


DROP TABLE IF EXISTS archive_coupon_4_S_versioned;
CREATE EXTERNAL TABLE archive_coupon_4_S_versioned (
  Create_Version string,
  Discount_Card_ID string,
  Coupon_ID int,
  Rank float,
  Rec_Version string
)
PARTITIONED BY(ctimestamp string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

ALTER TABLE archive_coupon_4_S_versioned ADD PARTITION(ctimestamp='1421316233')
  LOCATION '/backup/ulybka_radugi/1421316233/Coupons_4_S_versioned';
ALTER TABLE archive_coupon_4_S_versioned ADD PARTITION(ctimestamp='1421321302')
  LOCATION '/backup/ulybka_radugi/1421321302/Coupons_4_S_versioned';



-- ########################         coupons_4_LG_* and coupons_4_S_* statistics tables          ########################
set hive.exec.dynamic.partition.mode=nonstrict;


-- ######################## rec_version

DROP TABLE IF EXISTS coupons_4_LG_aggr_info;
CREATE TABLE coupons_4_LG_aggr_info (
  place int,
  rec_version string,
  cnt int
)
PARTITIONED BY(ctimestamp string)
;
INSERT OVERWRITE TABLE coupons_4_LG_aggr_info PARTITION(ctimestamp)
SELECT place, rec_version, count(*) as cnt, ctimestamp
FROM
(
  SELECT ctimestamp, discount_card_id, rec_version, bk_rank(discount_card_id) as place
  FROM
  (
      SELECT
          ctimestamp,
          discount_card_id,
          rank,
          rec_version
      FROM archive_coupon_4_LG_versioned
      DISTRIBUTE BY ctimestamp, discount_card_id
      SORT BY ctimestamp, discount_card_id, rank DESC
  ) t
) t
GROUP BY place, rec_version, ctimestamp
;

DROP TABLE IF EXISTS coupons_4_S_aggr_info;
CREATE TABLE coupons_4_S_aggr_info (
  place int,
  rec_version string,
  cnt int
)
PARTITIONED BY(ctimestamp string)
;
INSERT OVERWRITE TABLE coupons_4_S_aggr_info PARTITION(ctimestamp)
SELECT place, rec_version, count(*) as cnt, ctimestamp
FROM
(
  SELECT ctimestamp, discount_card_id, rec_version, bk_rank(discount_card_id) as place
  FROM
  (
      SELECT
          ctimestamp,
          discount_card_id,
          rank,
          rec_version
      FROM archive_coupon_4_S_versioned
      DISTRIBUTE BY ctimestamp, discount_card_id
      SORT BY ctimestamp, discount_card_id, rank DESC
  ) t
) t
GROUP BY place, rec_version, ctimestamp
;


-- ######################## periodicity

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
WHERE cnt >= 2
GROUP BY group23
;


DROP TABLE IF EXISTS coupons_4_S_goods_periodicity;
CREATE TABLE coupons_4_S_goods_periodicity (place int, periodicity string, val float)
PARTITIONED BY (ctimestamp string)
;
INSERT OVERWRITE TABLE coupons_4_S_goods_periodicity PARTITION(ctimestamp)
SELECT place, periodicity_weeks, count(*) as val, ctimestamp
FROM (
  SELECT ctimestamp, good_id, bk_rank(discount_card_id) as place
  FROM (
    SELECT a.ctimestamp,
        a.discount_card_id,
        cl.sku as good_id,
        a.rank
    FROM archive_coupon_4_S_versioned a
    JOIN coupons_list cl ON cl.couponid = a.coupon_id
    DISTRIBUTE BY ctimestamp, discount_card_id
    SORT BY ctimestamp, discount_card_id, rank DESC
  ) t
) t
JOIN tmp_good_id_group23 gg
  ON gg.good_id = t.good_id
JOIN (
  SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
  FROM tmp_group23_periodicity
) gp
ON gg.group23 = gp.group23
GROUP BY place, periodicity_weeks, ctimestamp
;

DROP TABLE IF EXISTS coupons_4_LG_goods_periodicity;
CREATE TABLE coupons_4_LG_goods_periodicity (place int, periodicity string, val float)
PARTITIONED BY (ctimestamp string)
;
INSERT OVERWRITE TABLE coupons_4_LG_goods_periodicity PARTITION(ctimestamp)
SELECT place, periodicity_weeks, count(*) as val, ctimestamp
FROM (
  SELECT ctimestamp, good_id, bk_rank(discount_card_id) as place
  FROM (
    SELECT a.ctimestamp,
        a.discount_card_id,
        cl.sku as good_id,
        a.rank
    FROM archive_coupon_4_LG_versioned a
    JOIN coupons_list cl ON cl.couponid = a.coupon_id
    DISTRIBUTE BY ctimestamp, discount_card_id
    SORT BY ctimestamp, discount_card_id, rank DESC
  ) t
) t
JOIN tmp_good_id_group23 gg
  ON gg.good_id = t.good_id
JOIN (
  SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
  FROM tmp_group23_periodicity
) gp
ON gg.group23 = gp.group23
GROUP BY place, periodicity_weeks, ctimestamp
;

-- INSERT INTO TABLE coupons_4_S_goods_periodicity PARTITION(ctimestamp)
-- SELECT -1, periodicity_weeks, count(*) * 1400000 / 374 as val
-- FROM coupons_list_product_ids t
-- JOIN tmp_good_id_group23 gg
--   ON gg.good_id = t.product_id
-- JOIN
-- (
--   SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
--   FROM tmp_group23_periodicity
-- ) gp
-- ON gg.group23 = gp.group23
-- GROUP BY periodicity_weeks
-- ;



-- ######################################################################|

DROP TABLE IF EXISTS tmp_goods_year_month_count;
CREATE TABLE tmp_goods_year_month_count AS
SELECT good_id, year_month, count(distinct cheque_id) as cnt
FROM
(
  SELECT product_id as good_id, cheque_id, from_unixtime(t.timestamp, 'yyyy-MM')  as year_month
  FROM bk_transactions t
) t
GROUP BY good_id, year_month
;

DROP TABLE IF EXISTS coupons_year_month_stats;
CREATE TABLE coupons_year_month_stats (year_month string, stat_name string, val float);


INSERT INTO TABLE coupons_year_month_stats
SELECT year_month, 'previous month purchases', sum(cnt)
FROM
(
  SELECT cl.good_id, cl.year_month, sum(t.cnt) as cnt
  FROM
  (
    SELECT
      sku AS good_id,
      from_unixtime(unix_timestamp(startdate), 'yyyy-MM')  as year_month
    FROM coupons_list
    WHERE year(startdate) <= year(enddate) and month(startdate) <= month(enddate)
  ) cl
  JOIN tmp_goods_year_month_count t
    ON t.good_id = cl.good_id
  WHERE add_months(cast(concat(t.year_month, '-01') as timestamp), 1) = cast(concat(cl.year_month, '-01') as timestamp)
  GROUP BY cl.good_id, cl.year_month
) t
GROUP BY year_month
;

INSERT INTO TABLE coupons_year_month_stats
SELECT year_month, 'this month purchases', sum(cnt)
FROM
(
  SELECT cl.good_id, cl.year_month, sum(t.cnt) as cnt
  FROM
  (
    SELECT
      sku AS good_id,
      from_unixtime(unix_timestamp(startdate), 'yyyy-MM')  as year_month
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




-- ######################################################################|
DROP TABLE IF EXISTS coupons_goods_count;
CREATE TABLE coupons_goods_count (year_month string, stat_name string, val float);

INSERT INTO TABLE coupons_goods_count
SELECT year_month, 'goods count', count(distinct good_id)
FROM
(
  SELECT
    sku AS good_id,
    from_unixtime(unix_timestamp(startdate), 'yyyy-MM')  as year_month
  FROM coupons_list
  WHERE year(startdate) <= year(enddate) and month(startdate) <= month(enddate)
) cl
GROUP BY year_month
;

INSERT INTO TABLE coupons_goods_count
SELECT year_month, 'group23 count', count(distinct group23)
FROM
(
  SELECT
    sku AS good_id,
    from_unixtime(unix_timestamp(startdate), 'yyyy-MM')  as year_month
  FROM coupons_list
  WHERE year(startdate) <= year(enddate) and month(startdate) <= month(enddate)
) cl
JOIN tmp_good_id_group23 gg ON cl.good_id = gg.good_id
GROUP BY year_month
;




-- ######################################################################

DROP TABLE IF EXISTS tmp_coupons_loyalty_generator_bo;
CREATE EXTERNAL TABLE tmp_coupons_loyalty_generator_bo (
  Version string, Discount_Card_ID string, Coupon_ID int, Rank float, CDate string, Rec_Version string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/senov/coupons_for_loyalty_generator_bo/1417716790';


DROP TABLE IF EXISTS LG_december_recs_aggr_info_basket_offer;
CREATE TABLE LG_december_recs_aggr_info_basket_offer AS
SELECT place, source, count(*) as cnt
FROM
(
  SELECT discount_card_id, source, bk_rank(discount_card_id) as place
  FROM
  (
      SELECT
          discount_card_id,
          rank,
          Rec_Version as source
      FROM tmp_coupons_loyalty_generator_bo
      WHERE discount_card_id != '0000000000000'
      DISTRIBUTE BY discount_card_id
      SORT BY discount_card_id, rank DESC
  ) t
) t
GROUP BY place, source
;


-- ######################################################################
DROP TABLE IF EXISTS LG_december_periodicity;
CREATE TABLE LG_december_periodicity (place int, periodicity string, val float);

INSERT INTO TABLE LG_december_periodicity
SELECT place, periodicity_weeks, count(*) as val
FROM
(
  SELECT good_id, bk_rank(discount_card_id) as place
  FROM
  (
      SELECT
          discount_card_id,
          rank,
          cl.sku as good_id
      FROM tmp_coupons_loyalty_generator_bo t
      JOIN coupons_list cl ON cl.couponid = t.coupon_id
      WHERE discount_card_id != '0000000000000'
      DISTRIBUTE BY discount_card_id
      SORT BY discount_card_id, rank DESC
  ) t
) t
JOIN tmp_good_id_group23 gg
  ON gg.good_id = t.good_id
JOIN
(
  SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
  FROM tmp_group23_periodicity
) gp
ON gg.group23 = gp.group23
GROUP BY place, periodicity_weeks
;



INSERT INTO TABLE LG_december_periodicity
SELECT -1, periodicity_weeks, cast(count(*) * 1200000. / 374 * 1.5 as float) as val
FROM coupons_list_product_ids t
JOIN tmp_good_id_group23 gg
  ON gg.good_id = t.product_id
JOIN
(
  SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
  FROM tmp_group23_periodicity
) gp
ON gg.group23 = gp.group23
GROUP BY periodicity_weeks
;



INSERT INTO TABLE S_recs_periodicity
SELECT -1, periodicity_weeks, cast(count(*) * 1400000. / 236 * 1.5 as float) as val
FROM coupons_list_product_ids t
JOIN tmp_good_id_group23 gg
  ON gg.good_id = t.product_id
JOIN
(
  SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
  FROM tmp_group23_periodicity
) gp
ON gg.group23 = gp.group23
GROUP BY periodicity_weeks
;

INSERT INTO TABLE LG_recs_periodicity
SELECT -1, periodicity_weeks, cast(count(*)  * 1400000. / 236 as float) as val
FROM coupons_list_product_ids t
JOIN tmp_good_id_group23 gg
  ON gg.good_id = t.product_id
JOIN
(
  SELECT group23, concat(cast(floor(avg_periodicity / 7) as string), '-', cast(ceil(avg_periodicity / 7) as string), 'w') as periodicity_weeks
  FROM tmp_group23_periodicity
) gp
ON gg.group23 = gp.group23
GROUP BY periodicity_weeks
;

