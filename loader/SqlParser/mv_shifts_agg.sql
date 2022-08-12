create or replace table analytics.mv_shifts_agg as
with _shifts as (
    select id as shift_id,
           warehouse_id,
           cast(FORMAT_DATE('%u', start_date) as int64) as shift_start_dow,
           date_trunc(start_date, isoweek) as shift_start_week,
           date_trunc(start_date, day) as shift_start_dt,
           created_at as shift_created_dttm,
           start_date as plan_shift_start,
           end_date as plan_shift_end,
           max_count,
           if(date_trunc(start_date, day) = date_trunc(created_at, day), 'Emergency', 'Regular') as shift_type,
           extract(hour from start_date) start_hour,
           timestamp_diff(end_date, start_date, SECOND) / 3600 as shift_len,
           format('Смена %d ч.',
                      cast(
                          round(timestamp_diff(end_date, start_date, SECOND) / 3600) as int64)) as shift_len_label,
           format_timestamp('%H:%M', start_date) || ' - ' ||
                format_timestamp('%H:%M', end_date) ||
                format(', shift_id = %i', cast(id as int64)) as shift_interval,
           FORMAT_DATE('%Y-%m-%d', date_trunc(start_date, day)) || '__' || warehouse_id as dt_wh_key
    from snp.shifts
    where role_id = 1
),
_calendas as (
    select	dt
	from (
		select GENERATE_TIMESTAMP_ARRAY('2021-02-01',
									date_add(date_trunc(CURRENT_TIMESTAMP(), isoweek), INTERVAL 6 day),
									INTERVAL 1 DAY) as arr_dt)
	cross join unnest(arr_dt) as dt
),
_shift_dynamic as (
    select t1.shift_id,
           countif(TIMESTAMP_DIFF(t1.dt, plan_shift_start, day) = 0 and type = 10) as n_emp_count,
           countif(TIMESTAMP_DIFF(t1.dt, plan_shift_start, day) = -1 and type = 10) as nm1_emp_count,
           countif(TIMESTAMP_DIFF(t1.dt, plan_shift_start, day) = -2 and type = 10) as nm2_emp_count,
           countif(TIMESTAMP_DIFF(t1.dt, plan_shift_start, day) = -3 and type = 10) as nm3_emp_count
    from (
        select shift_id,
               shift_created_dttm,
               plan_shift_start,
               t2.dt
        from _shifts as t1
        cross join _calendas as t2
        where t2.dt > t1.shift_created_dttm
            and t2.dt < t1.plan_shift_end
        ) as t1
    join (
        select shift_id,
               employee_id,
               created_at as status_dttm,
               lead(created_at, 1, '2099-01-01')
                   over (partition by shift_id, employee_id order by created_at) as next_status_dttm,
               type
        from snp.employee_shift_status_history
        where type in (0, 10)
        )as t2
            on t1.shift_id = t2.shift_id
            and t1.dt >= t2.status_dttm
            and t1.dt < t2.next_status_dttm
    group by 1
),
_shift_stat as (
    select t1.shift_id,
           t1.max_count,
           t1.shift_len,
           t1.max_count * t1.shift_len as shift_sh_ttl,
           sum(paid_sh)                as fact_sh,
           sum(
               TIMESTAMP_DIFF(LEAST(fact_shift_end, t1.plan_shift_end),
                              GREATEST(fact_shift_start, t1.plan_shift_start), second) / 3600
               ) as trim_fact_sh,
           sum(
               TIMESTAMP_DIFF(t1.plan_shift_start, LEAST(fact_shift_start, t1.plan_shift_start), second) / 3600
               ) as early_start_sh,
           sum(
               TIMESTAMP_DIFF(GREATEST(fact_shift_start, t1.plan_shift_start), t1.plan_shift_start, second) / 3600
               ) as late_start_sh,
           sum(
               TIMESTAMP_DIFF(t1.plan_shift_end, LEAST(fact_shift_end, t1.plan_shift_end), second) / 3600
               ) as early_end_sh,
           sum(
               TIMESTAMP_DIFF(GREATEST(fact_shift_end, t1.plan_shift_end), t1.plan_shift_end, second) / 3600
               ) as late_end_sh,
           count(*) as assigned_emp_cnt,
           countif(removed_dttm is null) as not_removed_cnt,
           countif(paid_sh > 0) as working_cnt
    from _shifts as t1
    join analytics.mv_couriers_shift_agg as t2
        on t1.shift_id = t2.shift_id
    group by 1, 2, 3, 4
),
_final as (
    select t1.*,
           t2.nm3_emp_count,
           t2.nm2_emp_count,
           t2.nm1_emp_count,
           t2.n_emp_count,
           t3.shift_sh_ttl,
           COALESCE(t3.fact_sh, 0) as fact_sh,
           COALESCE(t3.trim_fact_sh, 0) as trim_fact_sh,
           COALESCE(t3.early_start_sh, 0) as early_start_sh,
           COALESCE(t3.late_start_sh, 0) as late_start_sh,
           COALESCE(t3.early_end_sh, 0) as early_end_sh,
           COALESCE(t3.late_end_sh, 0) as late_end_sh,
           assigned_emp_cnt,
           not_removed_cnt,
           working_cnt,
           (t1.max_count - not_removed_cnt) * t1.shift_len as not_filled_sh,
           (not_removed_cnt - working_cnt) * t1.shift_len as skiped_sh
    from _shifts as t1
    left join _shift_dynamic as t2
        on t1.shift_id = t2.shift_id
    left join _shift_stat as t3
        on t1.shift_id = t3.shift_id
)
-- проверка
-- select t1._w,
--        t1.warehouse_id,
--        shift_cnt_agg,
--        shift_cnt_raw,
--        max_count_agg,
--        max_count_raw,
--        fact_sh_agg,
--        fact_sh_raw
-- from (
--     select date_trunc(plan_shift_start, isoweek) as _w,
--            warehouse_id,
--            count(*) as shift_cnt_agg,
--            sum(max_count) as max_count_agg,
--            sum(fact_sh) as fact_sh_agg,
--     from _final
--     group by 1, 2
--     ) as t1
-- join (
--     select date_trunc(start_date, isoweek) as _w,
--            warehouse_id,
--            count(*) as shift_cnt_raw,
--            sum(max_count) as max_count_raw
--     from snp.shifts
--     where role_id = 1
--     group by 1, 2
--     ) as t2
--         on t1.warehouse_id = t2.warehouse_id
--         and t1._w = t2._w
-- join (
--     select date_trunc(plan_shift_start, isoweek) as _w,
--            warehouse_id,
--            sum(paid_sh) as fact_sh_raw
--     from analytics.mv_couriers_shift_agg
--     where role_id = 1
--     group by 1, 2
--     ) as t3
--         on t1.warehouse_id = t3.warehouse_id
--         and t1._w = t3._w
select *
from _final