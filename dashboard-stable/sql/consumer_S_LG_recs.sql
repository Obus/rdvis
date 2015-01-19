-- ----- common stats
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


-- ------


DROP TABLE IF EXISTS consumer_goods_S_recs_versioned;
CREATE TABLE consumer_goods_S_recs_versioned AS
SELECT t.Discount_Card_ID as discount_card_id, t.rank, g.good_name, g.cost, t.Rec_Version as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count) as bought_already, ctimestamp
FROM archive_coupon_4_S_versioned t
JOIN coupons_list cl
  ON cl.couponid = t.coupon_id
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = cl.sku
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.Discount_Card_ID = cgc.discount_card_id AND cl.sku = cgc.good_id
WHERE t.Discount_Card_ID != '0000000000000'
;


DROP TABLE IF EXISTS consumer_goods_LG_recs_versioned;
CREATE TABLE consumer_goods_LG_recs_versioned AS
SELECT t.Discount_Card_ID as discount_card_id, t.rank, g.good_name, g.cost, t.Rec_Version as algorithm, IF(cgc.good_id IS NULL, 0, cgc.good_count) as bought_already, ctimestamp
FROM archive_coupon_4_LG_versioned t
JOIN coupons_list cl
  ON cl.couponid = t.coupon_id
LEFT OUTER JOIN good_aggr_info g
  ON g.ID = cl.sku
LEFT OUTER JOIN consumer_goods_count cgc
  ON t.Discount_Card_ID = cgc.discount_card_id AND cl.sku = cgc.good_id
WHERE t.Discount_Card_ID != '0000000000000'
;



CREATE TABLE tmp_devices_stats AS
SELECT Discount_Card_ID, Target, good_name, count(*) as cnt
FROM Devices_Simplate_Printer
WHERE Target='ShowCoupon' OR Target='PrintCoupon'
GROUP BY Discount_Card_ID, Target, good_name
;

CREATE TABLE tmp_deviced_consumers AS
SELECT Discount_Card_ID, sum(cnt) as cnt
FROM tmp_devices_stats
GROUP BY Discount_Card_ID
;