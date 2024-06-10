WITH procedure_derived AS
(
    SELECT stay_id, starttime,endtime,
    case itemid WHEN 225792 THEN 'Invasive'
                WHEN 225794 THEN 'Noninvasive' END AS ventilation_status
    FROM `physionet-data.mimiciv_icu.procedureevents`
    WHERE itemid IN
    (
         225792  
        ,225794 
    )
),
highflow AS (
    SELECT stay_id, charttime,
CASE
WHEN value = 'High flow nasal cannula' THEN 'HighFlow'
ELSE NULL END AS ventilation_status
FROM `physionet-data.mimiciv_icu.chartevents`
WHERE itemid = 226732
AND value = 'High flow nasal cannula'
)
, highflow_overstay AS (
    SELECT stay_id,ventilation_status,charttime,
    LAG(charttime) OVER(PARTITION BY stay_id ORDER BY charttime ASC) AS previous_time
    FROM highflow
)
,highflow_overstay2 AS (
    SELECT stay_id,ventilation_status,charttime,previous_time,
    charttime - previous_time AS difference
    FROM highflow_overstay
)
,highflow_gap AS (
    SELECT *,
    CASE WHEN difference > interval 24 hour
    THEN 1 else 0 end as temp
    from highflow_overstay2
)
,highflow_gap2 AS (
    SELECT *,
    SUM(temp) OVER(PARTITION BY stay_id ORDER BY charttime) AS g
    FROM highflow_gap
)
, highflow_final AS (
    SELECT stay_id,ventilation_status
, MIN (charttime) AS starttime
, MAX (charttime) AS endtime
FROM highflow_gap2
WHERE ventilation_status IS NOT NULL
GROUP BY stay_id,ventilation_status,g
),
vent_and_hf AS(
    SELECT
    CAST(stay_id as int) AS stay_id,starttime,endtime, ventilation_status FROM procedure_derived
    UNION ALL
    SELECT
    CAST(stay_id as int) AS stay_id,
    CAST (starttime as datetime),
    CAST (endtime as datetime),
    ventilation_status FROM highflow_final
    WHERE starttime != endtime
),
creating_lag_lead AS (
    SELECT stay_id,starttime,endtime,ventilation_status,
    LAG(ventilation_status) OVER(PARTITION BY stay_id ORDER BY starttime ASC) AS previous_status,
    LAG(endtime) OVER(PARTITION BY stay_id ORDER BY starttime ASC) AS previous_time,
    LEAD(ventilation_status) OVER(PARTITION BY stay_id ORDER BY starttime ASC) AS next_status,
    LEAD(starttime) OVER(PARTITION BY stay_id ORDER BY starttime ASC) AS next_time,
    LEAD(endtime) OVER(PARTITION BY stay_id ORDER BY starttime ASC) AS next_endtime,
    ROW_NUMBER() OVER (ORDER BY stay_id) AS row_num
    from vent_and_hf
    ORDER BY stay_id,starttime
),

combine_same_treatment_step1 AS (
    SELECT stay_id,
    ROW_NUMBER() OVER(PARTITION BY stay_id,ventilation_status ORDER BY starttime) AS row_number
    FROM creating_lag_lead
    where (previous_status = ventilation_status and previous_time > starttime)
    or (next_status = ventilation_status and next_time < endtime)
),
combine_same_treatment_step2 as (
    select *,
    case when previous_time < starttime then 1 else 0 end as ind
    from creating_lag_lead 
    where stay_id in (select distinct(stay_id) from combine_same_treatment_step1 where row_number >=2)
    and ((previous_status = ventilation_status and previous_time > starttime)
    or (next_status = ventilation_status and next_time < endtime))
),
combine_same_treatment_step3 as (
    select *,
    sum(ind) over (PARTITION BY stay_id order by starttime) as g
    from combine_same_treatment_step2
),
combine_same_treatment_step4 as (
    select stay_id,
    starttime,
    endtime,
    ventilation_status,
    FIRST_VALUE(previous_status) over par as previous_status,
    FIRST_VALUE(previous_time) over par as previous_time,
    LAST_VALUE(next_status) over par as next_status,
    LAST_VALUE(next_time) over par as next_time,
    LAST_VALUE(next_endtime) over par as next_endtime,
    g
    From combine_same_treatment_step3
    WINDOW par as (partition by stay_id,g order by starttime)
),
combine_same_treatment_step5 as (
    select stay_id,
    min(starttime) as starttime,
    max(endtime) as endtime,
    min(ventilation_status) as ventilation_status,
    min(previous_status) as previous_status,
    min(previous_time) as previous_time,
    max(next_status) as next_status,
    max(next_time) as next_time,
    max(next_endtime) as next_endtime,
    from combine_same_treatment_step4
    group by stay_id,g
    union all
    select stay_id,starttime,endtime,ventilation_status, previous_status, previous_time,next_status, next_time,next_endtime from creating_lag_lead
    where row_num not in (select row_num from combine_same_treatment_step2)
),
 close_gap_cutoff_overlap AS (
    select stay_id,starttime,endtime,ventilation_status,
    CASE WHEN
    ventilation_status = 'HighFlow' and previous_status in ('Invasive','Noninvasive') and previous_time is not null and starttime - previous_time < interval 6 hour then previous_time
    WHEN
    ventilation_status = 'Noninvasive' and previous_status ='Invasive' and previous_time is not null and starttime - previous_time < interval 6 hour then previous_time
    end as starttime_new,
    CASE WHEN
    ventilation_status = 'HighFlow' and next_status in ('Invasive','Noninvasive') and next_time is not null and next_time - endtime < interval 6 hour then next_time
    WHEN
    ventilation_status = 'Noninvasive' and next_status ='Invasive' and next_time is not null and next_time - endtime < interval 6 hour then next_time
    end as endtime_new
    from combine_same_treatment_step5
),
new_time as (
    SELECT stay_id,ventilation_status,
    COALESCE(starttime_new, starttime) as starttime,
    COALESCE(endtime_new, endtime) as endtime
    FROM close_gap_cutoff_overlap
    where not (stay_id = 31296377 and starttime = '2138-12-19T12:43:00')
)
select * from new_time
order by stay_id,starttime
