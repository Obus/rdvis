drop table temp_goods_groups;

select * into temp_goods_groups
from (
select distinct g.id, g.id_parent, g.name
from goods g
join goods g1 on g.id = g1.id_parent
where g1.id is not null or g.id LIKE 'СТМ %' or g.id LIKE 'СИ %'
) t;


create table temp_goods_groups as select *
from (
select distinct g.id, g.id_parent, g.name
from goods g
join goods g1 on g.id = g1.id_parent
left outer join goods g2 on g1.id = g2.id_parent
where g2.id is null
) t;



create table transaction_goods as
select t.good_id, g.good_name
from
(
  select distinct good_id
  from transactions
) t
left outer join goods g on g.id = t.good_id
;
