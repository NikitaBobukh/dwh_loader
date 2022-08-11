create or replace table `organic-reef-315010.public.brands_stage2` as
select id, count(*) as cnt
from `organic-reef-315010.public.brands`
group by 1