-- ----- common stats
DROP TABLE IF EXISTS good_aggr_info;
CREATE TABLE good_aggr_info AS
SELECT g.id,
  g.good_name,
  concat_ws('|', g_cl.classification_level_2, g_cl.classification_level_3, g_cl.classification_level_4) as good_group,
  concat_ws('|', g_cl.classification_level_2, g_cl.classification_level_3) as group23,
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



-- TRASH:
DROP TABLE IF EXISTS tmp_discount_card_id_coupon_show_print;
CREATE TABLE tmp_discount_card_id_coupon_show_print AS
SELECT t_show.discount_card_id, t_show.coupon_id, IF(t_print.coupon_id IS NULL, 0 , 1) as printed, t_show.date_time_of_session
FROM
(
  SELECT discount_card_id, IF(length(value) > 5, cast(substr(value, 10, 4) as int), cast(value as int)) as coupon_id, date_time_of_session
  FROM Devices_Simplate_Printer
  WHERE description='Купоны на принтере' and target = 'ShowCoupon'
) t_show
LEFT OUTER JOIN
(
  SELECT discount_card_id, IF(length(value) > 5, cast(substr(value, 10, 4) as int), cast(value as int)) as coupon_id
  FROM Devices_Simplate_Printer
  WHERE description='Купоны на принтере' and target = 'PrintCoupon'
) t_print
  ON t_print.discount_card_id = t_show.discount_card_id AND t_show.coupon_id = t_print.coupon_id
;



DROP TABLE IF EXISTS tmp_tmp_tmp;
CREATE TABLE tmp_tmp_tmp AS
SELECT r.discount_card_id, r.coupon_id, r.rec_version, csp.printed, csp.date_time_of_session
FROM archive_coupon_4_LG_versioned r
JOIN tmp_discount_card_id_coupon_show_print csp
  ON r.coupon_id = csp.coupon_id AND r.discount_card_id = csp.discount_card_id
;

DROP TABLE IF EXISTS tmp_coupons_on_printer_effectivity_by_method_december;
CREATE TABLE tmp_coupons_on_printer_effectivity_by_method_december AS
SELECT rec_version, name, sshow, print, round(print / sshow, 3) as print_show FROM
(
  SELECT rec_version, coupon_id, count(*) as sshow, sum(printed) as print
  FROM tmp_tmp_tmp
  WHERE date_time_of_session like '2014-12-%'
  GROUP BY rec_version, coupon_id
) t
JOIN coupons_list cl
  ON cl.couponid = t.coupon_id
;


SELECT name, sshow, print, print / sshow FROM
(
  SELECT coupon_id, count(*) as sshow, sum(printed) as print
  FROM tmp_tmp_tmp
  WHERE date_time_of_session like '2014-12-%' AND rec_version='GROUP_OFFER'
  GROUP BY coupon_id
) t
JOIN coupons_list cl
  ON cl.couponid = t.coupon_id
WHERE sshow > 100 ORDER BY print / sshow DESC
LIMIT 20
;

DROP VIEW IF EXISTS tmp_1;
CREATE VIEW tmp_1 AS SELECT rec_version, name as item, sshow, print, print_show FROM tmp_coupons_on_printer_effectivity_by_method_december;

DROP VIEW IF EXISTS tmp_2;
CREATE VIEW tmp_2 AS SELECT rec_version, discount_card_id as item, sshow, print, print_show FROM tmp_coupons_on_printer_effectivity_by_method_december;

SELECT go.item, go.print_show - bo.print_show as abs_print_show, go.print_show / bo.print_show as rel_print_show, go.sshow as group_offers_show, bo.sshow as basket_offers
FROM (SELECT * FROM tmp_1 WHERE rec_version='GROUP_OFFER') go
JOIN (SELECT item, sum(sshow) as sshow, sum(print) as print, round(sum(print) / sum(sshow), 3) as print_show FROM tmp_1 WHERE rec_version!='GROUP_OFFER' GROUP BY item) bo
  ON go.item = bo.item
WHERE go.sshow > 100 AND bo.sshow > 100
ORDER BY rel_print_show DESC
limit 20
;


-- TRASH 2:
-- Discount_Card_ID        Max_cheque_date
DROP TABLE IF EXISTS for_sms_sending_2015_20_discount_cards;
CREATE EXTERNAL TABLE for_sms_sending_2015_20_discount_cards (
  Discount_Card_ID string, Max_cheque_date string
)
PARTITIONED BY (ggroup string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

ALTER TABLE for_sms_sending_2015_20_discount_cards ADD PARTITION (ggroup='lost') LOCATION '/user/senov/sms_sending_2015_20/group_lost';
ALTER TABLE for_sms_sending_2015_20_discount_cards ADD PARTITION (ggroup='shownotprint') LOCATION '/user/senov/sms_sending_2015_20/group_shownotprint';


DROP TABLE IF EXISTS tmp_consumer_group23;
CREATE TABLE tmp_consumer_group23 AS
SELECT consumer_id, group23, count(distinct cheque_id) as cnt
FROM bk_transactions t
JOIN for_sms_sending_2015_20_discount_cards dc
  ON t.consumer_id = dc.discount_card_id
JOIN tmp_good_id_group23 gg
  ON t.product_id = gg.good_id
WHERE consumer_id NOT LIKE '00%'
GROUP BY t.consumer_id, gg.group23
;




DROP TABLE IF EXISTS for_sms_sending_2015_20_discount_cards_recs;
CREATE TABLE for_sms_sending_2015_20_discount_cards_recs AS
SELECT dc.ggroup, dc.discount_card_id, r.product_id as good_id, rank, meta_info, IF(cg.cnt IS NULL, 0, cg.cnt) as group_cnt, IF(cg.cnt IS NULL, 0, cg.cnt) / cc.c_cnt as group_p, cc.c_cnt as cheque_cnt
FROM for_sms_sending_2015_20_discount_cards dc
JOIN Coupons_List_S_recs_hive r
  ON dc.discount_card_id = r.consumer_id
JOIN tmp_good_id_group23 gg
  ON gg.good_id = r.product_id
LEFT OUTER JOIN tmp_consumer_group23 cg
  ON cg.consumer_id = r.consumer_id AND gg.group23 = cg.group23
JOIN consumer_count cc
  ON cc.consumer_id = r.consumer_id
;

DROP TABLE IF EXISTS for_sms_sending_2015_20_discount_cards_recs_top1;
CREATE TABLE for_sms_sending_2015_20_discount_cards_recs_top1 AS
SELECT t.ggroup, t.discount_card_id, t.good_id, t.meta_info, t.group_cnt, t.rank, t.cheque_cnt
FROM for_sms_sending_2015_20_discount_cards_recs t
JOIN (
  SELECT discount_card_id, max(rank) as max_rank
  FROM for_sms_sending_2015_20_discount_cards_recs
  WHERE group_cnt > 0
  GROUP BY discount_card_id
) mr
  ON t.discount_card_id = mr.discount_card_id AND t.rank = mr.max_rank
;


select t.discount_card_id, g.good_name, g.cost, g.group23, t.group_cnt
FROM for_sms_sending_2015_20_discount_cards_recs_top1 t
JOIN good_aggr_info g ON t.good_id = g.id
limit 20;


select t.discount_card_id, name, regexp_replace(cl.name, '  ', ' ')
FROM for_sms_sending_2015_20_discount_cards_recs_top1 t
JOIN coupons_list cl ON t.good_id = cl.sku
  WHERE cl.isActive = 1
limit 20;
Действительно до 31/01/15


select count(*)
from for_sms_sending_2015_20_discount_cards_recs_top1 rt
join discount_card_valid_phone dp
  on dp.discount_card_id = rt.discount_card_id
  ;



SELECT count(*) -- t.discount_card_id, phone_number, concat(dch_pretty_first_name, 'пециально для вас скидка ', discount_p, ' на «', name, '». Просто введите код ', '1234567', ' на Терминале Скидок. Ваша «Улыбка».') as text
FROM (
  SELECT
    sku,
    concat(cast(-discount as string), '%') as discount_p,
    name,
    couponid,
    concat(cast((- discount / 100) * cost as string), ' р.') as discount_a
  FROM coupons_list cl
  JOIN bk_product_cost pc
    ON cl.sku = pc.product_id
  WHERE isActive = 1
) cl
JOIN for_sms_sending_2015_20_discount_cards_recs_top1 t
  ON t.good_id = cl.sku
join discount_card_valid_phone dp
  on dp.discount_card_id = t.discount_card_id
JOIN (
  SELECT discount_card_holder_id as discount_card_id,
    IF(first_name IS NULL, 'С', concat(first_name, ', с')) as dch_pretty_first_name
  FROM discount_card_holders_first_name
) fn
  ON fn.discount_card_id = t.discount_card_id
WHERE t.ggroup!='lost'



concat('Специально для вас скидка ', discount_p, ' на «', name, '». Просто введите код ', '1234567', ' на Терминале Скидок. Ваша «Улыбка».')