WITH vital_signs AS
  (
    SELECT
    chartevent.stay_id,
    chartevent.* EXCEPT(stay_id, subject_id,storetime,itemid,value,warning,hadm_id,valueuom,valuenum),
    CASE WHEN itemid in (220045) and valuenum > 0 then valuenum    -- Heart Rate
    else null
    END as heart_rate,
    CASE WHEN itemid in (225309,220050)and valuenum > 0 then valuenum    -- Arterial Blood Pressure systolic
    else null
    END as sbp,
    CASE WHEN itemid in (225310,220051) and valuenum > 0 then valuenum   -- Arterial Blood Pressure diastolic
    else null
    END as dbp,
    CASE WHEN itemid in (225312,220052) and valuenum > 0 then valuenum   -- Arterial Blood Pressure mean
    else null
    END as mbp, 
    CASE WHEN itemid in (220179) and valuenum > 0 then valuenum    -- Non Invasive Blood Pressure systolic
    else null
    END as 	sbp_ni,
    CASE WHEN itemid in (220180) and valuenum > 0 then valuenum    -- Non Invasive Blood Pressure diastolic
    else null
    END as dbp_ni,
    CASE WHEN itemid in (220181) and valuenum > 0 then valuenum   -- Non Invasive Blood Pressure mean
    else null
    END as mbp_ni,
    CASE WHEN itemid in (220277) and valuenum > 0 then valuenum  --  SpO2
    else null
    END as spo2,
    CASE WHEN itemid in (220621,226537) and valuenum > 0 then valuenum  --Glucose (serum) -- Glucose (whole blood)
    else null
    END as glucose,
    CASE WHEN itemid in (223762) and valuenum > 0  then valuenum  --Temp Celsius
    WHEN itemid in (223761) and valuenum > 0 then (valuenum-32)/1.8 --Temp Fahrenheit 
    else null
    END as temperature
    FROM `physionet-data.mimiciv_icu.chartevents` chartevent
      where itemid in (220045,225309,220050,225310,220051,225312,220052,220179,220180,220181,220181,220277,220621,226537,223762,223761,224642)),
 difference_intime_to_charttime AS
  (
      SELECT
    vs.* EXCEPT(stay_id),
    icu.stay_id,
    CAST(
        FLOOR(DATETIME_DIFF(charttime, intime, minute) / 60) AS INT64
    ) AS hr
    FROM `physionet-data.mimiciv_icu.icustays` icu
    INNER JOIN vital_signs vs
      ON icu.stay_id = vs.stay_id
  ), 
 heart_rate_median as (select stay_id, hr,heart_rate,
PERCENTILE_CONT(heart_rate, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_heart_rate
from difference_intime_to_charttime),

heart_rate_median_collapse as (select stay_id,hr, max(median_heart_rate) as heart_rate
from heart_rate_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
sbp_median as (select stay_id, hr,sbp,
PERCENTILE_CONT(sbp, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_sbp
from difference_intime_to_charttime),

sbp_median_collapse as (select stay_id,hr, max(median_sbp) as sbp
from sbp_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
dbp_median as (select stay_id, hr,dbp,
PERCENTILE_CONT(dbp, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_dbp
from difference_intime_to_charttime),

dbp_median_collapse as (select stay_id,hr, max(median_dbp) as dbp
from dbp_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
mbp_median as (select stay_id, hr,mbp,
PERCENTILE_CONT(mbp, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_mbp
from difference_intime_to_charttime),

mbp_median_collapse as (select stay_id,hr, max(median_mbp) as mbp
from mbp_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
sbp_ni_median as (select stay_id, hr,sbp_ni,
PERCENTILE_CONT(sbp_ni, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_sbp_ni
from difference_intime_to_charttime),

sbp_ni_median_collapse as (select stay_id,hr, max(median_sbp_ni) as sbp_ni
from sbp_ni_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
dbp_ni_median as (select stay_id, hr,dbp_ni,
PERCENTILE_CONT(dbp_ni, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_dbp_ni
from difference_intime_to_charttime),

dbp_ni_median_collapse as (select stay_id,hr, max(median_dbp_ni) as dbp_ni
from dbp_ni_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
mbp_ni_median as (select stay_id, hr,mbp_ni,
PERCENTILE_CONT(mbp_ni, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_mbp_ni
from difference_intime_to_charttime),

mbp_ni_median_collapse as (select stay_id,hr, max(median_mbp_ni) as mbp_ni
from mbp_ni_median
group by stay_id,hr),
------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
temperature_median as (select stay_id, hr,temperature,
PERCENTILE_CONT(temperature, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_temperature
from difference_intime_to_charttime),

temperature_median_collapse as (select stay_id,hr, max(median_temperature) as temperature
from temperature_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
spo2_median as (select stay_id, hr,spo2,
PERCENTILE_CONT(spo2, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_spo2
from difference_intime_to_charttime),

spo2_median_collapse as (select stay_id,hr, max(median_spo2) as spo2
from spo2_median
group by stay_id,hr),
------------------------------------------------------------------------------------------
glucose_median as (select stay_id, hr,glucose,
PERCENTILE_CONT(glucose, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_glucose
from difference_intime_to_charttime),

glucose_median_collapse as (select stay_id,hr, max(median_glucose) as glucose
from glucose_median
group by stay_id,hr),

join_with_hourly_table AS (
   SELECT a.stay_id, a.hr,b.*EXCEPT(stay_id,hr),c.*EXCEPT(stay_id,hr),d.*EXCEPT(stay_id,hr), e.*EXCEPT(stay_id,hr),
   f.*EXCEPT(stay_id,hr),g.*EXCEPT(stay_id,hr),h.*EXCEPT(stay_id,hr),i.*EXCEPT(stay_id,hr),
   j.*EXCEPT(stay_id,hr),k.*EXCEPT(stay_id,hr),
   FROM  `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs` a
  LEFT JOIN 
  heart_rate_median_collapse b
  ON  a.stay_id  = b.stay_id
  AND a.hr  = b.hr
  LEFT JOIN 
  sbp_median_collapse c
  ON  a.stay_id  = c.stay_id
  AND a.hr  = c.hr
  LEFT JOIN 
  dbp_median_collapse d
  ON  a.stay_id  = d.stay_id
  AND a.hr  = d.hr
  LEFT JOIN 
  mbp_median_collapse e
  ON  a.stay_id  = e.stay_id
  AND a.hr  = e.hr
  LEFT JOIN 
  sbp_ni_median_collapse f
  ON  a.stay_id  = f.stay_id
  AND a.hr  = f.hr
  LEFT JOIN 
  dbp_ni_median_collapse g
  ON  a.stay_id  = g.stay_id
  AND a.hr  = g.hr
  LEFT JOIN 
  mbp_ni_median_collapse h
  ON  a.stay_id  = h.stay_id
  AND a.hr  = h.hr
  LEFT JOIN 
  temperature_median_collapse i
  ON  a.stay_id  = i.stay_id
  AND a.hr  = i.hr
  LEFT JOIN 
  spo2_median_collapse j
  ON  a.stay_id  = j.stay_id
  AND a.hr  = j.hr
  LEFT JOIN
  glucose_median_collapse k
  ON  a.stay_id  = k.stay_id
  AND a.hr  = k.hr)

  select * from join_with_hourly_table
  order by stay_id, hr
