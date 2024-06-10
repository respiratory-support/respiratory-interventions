with gcs as (
with base as
(
  select 
    subject_id
  , ce.stay_id, ce.charttime
  , max(case when ce.ITEMID = 223901 then ce.valuenum else null end) as GCSMotor
  , max(case
      when ce.ITEMID = 223900 and ce.VALUE = 'No Response-ETT' then 0
      when ce.ITEMID = 223900 then ce.valuenum
      else null 
    end) as GCSVerbal
  , max(case when ce.ITEMID = 220739 then ce.valuenum else null end) as GCSEyes
  , max(case
      when ce.ITEMID = 223900 and ce.VALUE = 'No Response-ETT' then 1 
    else 0 end)
    as endotrachflag
  , ROW_NUMBER ()
          OVER (PARTITION BY ce.stay_id ORDER BY ce.charttime ASC) as rn
  from `physionet-data.mimiciv_icu.chartevents` ce
  where ce.ITEMID in
  (
    223900, 223901, 220739
  )
  group by ce.subject_id, ce.stay_id, ce.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  , case
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
     
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
     
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS

  from base b

  left join base b2
    on b.stay_id = b2.stay_id
    and b.rn = b2.rn+1
    and b2.charttime > DATETIME_ADD(b.charttime, INTERVAL 6 HOUR)
)

, gcs_stg as
(
  select
    subject_id
  , gs.stay_id, gs.charttime
  , GCS
  , coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
  , coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
  , coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
  , case when coalesce(GCSMotor,GCSMotorPrev) is null then 0 else 1 end
  + case when coalesce(GCSVerbal,GCSVerbalPrev) is null then 0 else 1 end
  + case when coalesce(GCSEyes,GCSEyesPrev) is null then 0 else 1 end
    as components_measured
  , EndoTrachFlag
  from gcs gs
)
, gcs_priority as
(
  select 
      subject_id
    , stay_id
    , charttime
    , gcs
    , gcsmotor
    , gcsverbal
    , gcseyes
    , EndoTrachFlag
    , ROW_NUMBER() over
      (
        PARTITION BY stay_id, charttime
        ORDER BY components_measured DESC, endotrachflag, gcs, charttime DESC
      ) as rn
  from gcs_stg
)
select
  gs.subject_id
  , gs.stay_id
  , gs.charttime
  , GCS AS gcs
  , GCSMotor AS gcs_motor
  , GCSVerbal AS gcs_verbal
  , GCSEyes AS gcs_eyes
  , EndoTrachFlag AS gcs_unable
from gcs_priority gs
where rn = 1),

difference_intime_to_charttime AS
  (
      SELECT
    gcs.* EXCEPT(stay_id),
    icu.stay_id,
    CAST(
        FLOOR(DATETIME_DIFF(charttime, intime, minute) / 60) AS INT64
    ) AS hr
    FROM `physionet-data.mimiciv_icu.icustays` icu
    INNER JOIN gcs
      ON icu.stay_id = gcs.stay_id
  ),

gcs_median as (select stay_id, hr,gcs,
PERCENTILE_CONT(gcs, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_gcs
from difference_intime_to_charttime),

gcs_median_collapse as (select stay_id,hr, max(median_gcs) as gcs
from gcs_median
group by stay_id,hr),

gcs_motor_median as (select stay_id, hr,gcs_motor,
PERCENTILE_CONT(gcs_motor, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_gcs_motor
from difference_intime_to_charttime),

gcs_motor_median_collapse as (select stay_id,hr, max(median_gcs_motor) as gcs_motor
from gcs_motor_median
group by stay_id,hr),

gcs_verbal_median as (select stay_id, hr,gcs_eyes,
PERCENTILE_CONT(gcs_verbal, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_gcs_verbal
from difference_intime_to_charttime),

gcs_verbal_median_collapse as (select stay_id,hr, max(median_gcs_verbal) as gcs_verbal
from gcs_verbal_median
group by stay_id,hr),

gcs_eyes_median as (select stay_id, hr,gcs_eyes,
PERCENTILE_CONT(gcs_eyes, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_gcs_eyes
from difference_intime_to_charttime),

gcs_eyes_median_collapse as (select stay_id,hr, max(median_gcs_eyes) as gcs_eyes
from gcs_eyes_median
group by stay_id,hr),

gcs_unable_median as (select stay_id, hr,gcs_unable,
PERCENTILE_CONT(gcs_eyes, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_gcs_unable
from difference_intime_to_charttime),

gcs_unable_median_collapse as (select stay_id,hr, max(median_gcs_unable) as gcs_unable
from gcs_unable_median
group by stay_id,hr),


join_with_hourly_table AS (
   SELECT a.stay_id, a.hr,b.*EXCEPT(stay_id,hr),c.*EXCEPT(stay_id,hr),d.*EXCEPT(stay_id,hr), e.*EXCEPT(stay_id,hr),
   f.*EXCEPT(stay_id,hr)
   FROM  `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs` a
  LEFT JOIN 
  gcs_median_collapse b
  ON  a.stay_id  = b.stay_id
  AND a.hr  = b.hr
  LEFT JOIN 
  gcs_motor_median_collapse c
  ON  a.stay_id  = c.stay_id
  AND a.hr  = c.hr
  LEFT JOIN 
  gcs_verbal_median_collapse d
  ON  a.stay_id  = d.stay_id
  AND a.hr  = d.hr
  LEFT JOIN 
  gcs_eyes_median_collapse e
  ON  a.stay_id  = e.stay_id
  AND a.hr  = e.hr
  LEFT JOIN 
  gcs_unable_median_collapse f
  ON  a.stay_id  = f.stay_id
  AND a.hr  = f.hr)
select * from join_with_hourly_table
order by stay_id, hr
