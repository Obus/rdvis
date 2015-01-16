DROP TABLE IF EXISTS good_aggr_info;
CREATE TABLE good_aggr_info AS
SELECT g.id,
  g.good_name,
  concat_ws('|', g_cl.classification_level_2, g_cl.classification_level_3, g_cl.classification_level_4) as good_group,
  g_c.cost
FROM Goods g
JOIN Goods_Retail_Classificator g_cl
  ON g.id = g_cl.good_id
JOIN bk_product_cost g_c
  ON g.id = g_c.product_id
;


DROP TABLE IF EXISTS consumer_goods_count;
CREATE TABLE consumer_goods_count AS
SELECT t.consumer_id as discount_card_id, t.cnt as good_count, t.product_id as good_id, g.good_name, g.good_group, from_unixtime(max_timestamp, 'yyyy-MM-dd') as last_date
FROM (
  SELECT consumer_id, product_id, count(distinct cheque_id) as cnt, max(`timestamp`) as max_timestamp
  FROM bk_transactions
  WHERE consumer_id != '0000000000000'
  GROUP BY consumer_id, product_id
) t
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = t.product_id
;


DROP TABLE IF EXISTS consumer_goods_S_recs;
CREATE TABLE consumer_goods_S_recs AS
SELECT t.consumer_id as discount_card_id, t.rank, g.good_name, g.cost, t.meta_info as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count) as bought_already
FROM Coupons_List_S_recs_hive_filtered t
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = t.product_id
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.consumer_id = cgc.discount_card_id AND t.product_id = cgc.good_id
WHERE consumer_id != '0000000000000'
;

DROP TABLE IF EXISTS consumer_goods_LG_recs;
CREATE TABLE consumer_goods_LG_recs AS
SELECT t.consumer_id as discount_card_id, t.rank, g.good_name, g.cost, t.meta_info as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count)  as bought_already
FROM Coupons_List_LG_recs_hive_filtered_no_S t
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = t.product_id
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.consumer_id = cgc.discount_card_id AND t.product_id = cgc.good_id
WHERE consumer_id != '0000000000000'
;

DROP TABLE IF EXISTS tmp_consumer_lda_offers_all_goods_groups_alt;
CREATE TABLE tmp_consumer_lda_offers_all_goods_groups_alt AS
SELECT t.consumer_id as discount_card_id, t.rank, g.good_name, g.cost, t.meta_info as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count)  as bought_already
FROM tmp_lda_offers_all_goods_groups_alt t
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = t.product_id
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.consumer_id = cgc.discount_card_id AND t.product_id = cgc.good_id
WHERE consumer_id != '0000000000000'
;





DROP TABLE IF EXISTS tmp_lda_offers_all_goods_groups_alt;
CREATE EXTERNAL TABLE tmp_lda_offers_all_goods_groups_alt
(
  consumer_id string,
  product_id string,
  rank double,
  meta_info string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/senov/lda_offers_all_goods_groups_alt/output'
;

DROP TABLE IF EXISTS tmp_lda_offers_all_goods;
CREATE EXTERNAL TABLE tmp_lda_offers_all_goods
(
  consumer_id string,
  product_id string,
  rank double,
  meta_info string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/senov/lda_offers_all_goods/output'
;

DROP TABLE IF EXISTS tmp_consumer_lda_offers_all_goods_groups_alt;
CREATE TABLE tmp_consumer_lda_offers_all_goods_groups_alt AS
SELECT t.consumer_id as discount_card_id, t.rank, g.good_name, g.cost, t.meta_info as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count)  as bought_already
FROM tmp_lda_offers_all_goods_groups_alt t
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = t.product_id
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.consumer_id = cgc.discount_card_id AND t.product_id = cgc.good_id
WHERE consumer_id != '0000000000000'
;

DROP TABLE IF EXISTS tmp_consumer_lda_offers_all_goods;
CREATE TABLE tmp_consumer_lda_offers_all_goods AS
SELECT t.consumer_id as discount_card_id, t.rank, g.good_name, g.cost, t.meta_info as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count)  as bought_already
FROM tmp_lda_offers_all_goods t
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = t.product_id
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.consumer_id = cgc.discount_card_id AND t.product_id = cgc.good_id
WHERE consumer_id != '0000000000000'
;




-- select cnt, count(*) / 1710362 FROM
-- (
-- SELECT r1.consumer_id, count(*) as cnt
-- FROM Coupons_List_LG_recs_hive r1
-- JOIN Coupons_List_S_recs_hive r2 ON r1.consumer_id = r2.consumer_id AND r1.product_id = r2.product_id
-- WHERE r1.rank > 100 - 12 AND r2.rank > 100 - 6
-- GROUP BY r1.consumer_id
-- ) t
-- GROUP BY cnt
-- ORDER BY cnt DESC
-- ;
-- +-----+----------------------+
-- | cnt | count(*) / 1710362   |
-- +-----+----------------------+
-- | 6   | 0.2115867868907284   |
-- | 5   | 0.3438389066174296   |
-- | 4   | 0.2712051600772234   |
-- | 3   | 0.1284646174318653   |
-- | 2   | 0.03817905215387152  |
-- | 1   | 0.006289896524829247 |
-- +-----+----------------------+
--
-- select cnt, count(*) / 1710362 FROM
-- (
-- SELECT r1.consumer_id, count(*) as cnt
-- FROM Coupons_List_LG_recs_hive r1
-- JOIN Coupons_List_S_recs_hive r2 ON r1.consumer_id = r2.consumer_id AND r1.product_id = r2.product_id
-- WHERE r1.rank > 100 - 6 AND r2.rank > 100 - 3
-- GROUP BY r1.consumer_id
-- ) t
-- GROUP BY cnt
-- ORDER BY cnt DESC
-- ;
-- +-----+--------------------+
-- | cnt | count(*) / 1710362 |
-- +-----+--------------------+
-- | 3   | 0.2940324913673246 |
-- | 2   | 0.4204536817352116 |
-- | 1   | 0.2376385817739169 |
-- +-----+--------------------+
-- 0.2940324913673246 + 0.4204536817352116 + 0.2376385817739169




