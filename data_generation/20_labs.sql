with specimen_type as (select *  FROM `physionet-data.mimiciv_hosp.labevents`labevents
where itemid = 52033
AND value= 'ART.'),
labs_art as (select * 
from `physionet-data.mimiciv_hosp.labevents`
where specimen_id in (select distinct(specimen_id) from specimen_type) ),

labs as (SELECT labs_art.subject_id,
labs_art.hadm_id,labs_art.charttime,
  CASE WHEN itemid in (50803) and valuenum > 0 then valuenum    
  else null
  END as calculated_bicarbonate,
  CASE WHEN itemid in (50818) and valuenum > 0 then valuenum   
  else null
  END as pCO2,
  CASE WHEN itemid in (50820) and valuenum > 0 then valuenum   
  else null
  END as pH,
  CASE WHEN itemid in (50821) and valuenum > 0 then valuenum   
  else null
  END as pO2, 
 CASE WHEN itemid = 50817 AND valuenum >0 and  valuenum <=100 THEN valuenum
 ELSE NULL END AS so2
FROM labs_art),
difference_intime_to_charttime AS
  (
      SELECT
      l.*,
    icu.stay_id,icu.intime,icu.outtime,
    CAST(
        FLOOR(DATETIME_DIFF(l.charttime, intime, minute) / 60) AS INT64
    ) AS hr
    FROM `physionet-data.mimiciv_icu.icustays` icu
    INNER JOIN labs l
      ON icu.subject_id = l.subject_id
      and  icu.hadm_id = icu.hadm_id 
      where l.charttime between intime and outtime
  ),


calculated_bicarbonate_median as (select stay_id, hr,calculated_bicarbonate,
PERCENTILE_CONT(calculated_bicarbonate, 0.5) OVER (PARTITION BY stay_id, hr) AS median_calculated_bicarbonate
from difference_intime_to_charttime),

calculated_bicarbonate_median_collapse as (select stay_id,hr, max(median_calculated_bicarbonate) as calculated_bicarbonate
from calculated_bicarbonate_median
group by stay_id,hr),


pCO2_median as (select stay_id, hr,pCO2,
PERCENTILE_CONT(pCO2, 0.5) OVER (PARTITION BY stay_id, hr) AS median_pCO2
from difference_intime_to_charttime),

pCO2_median_collapse as (select stay_id,hr, max(median_pCO2) as pCO2
from pCO2_median
group by stay_id,hr),

pH_median as (select stay_id, hr,pH,
PERCENTILE_CONT(pH, 0.5) OVER (PARTITION BY stay_id, hr) AS median_pH
from difference_intime_to_charttime),

pH_median_collapse as (select stay_id,hr, max(median_pH) as pH
from pH_median
group by stay_id,hr),

pO2_median as (select stay_id, hr, pO2,
PERCENTILE_CONT(pO2, 0.5) OVER (PARTITION BY stay_id, hr) AS median_pO2
from difference_intime_to_charttime),

 pO2_median_collapse as (select stay_id,hr, max(median_pO2) as  pO2
from  pO2_median
group by stay_id,hr),

so2_median as (select stay_id, hr, so2,
PERCENTILE_CONT(so2, 0.5) OVER (PARTITION BY stay_id, hr) AS median_so2
from difference_intime_to_charttime),

so2_median_collapse as (select stay_id,hr, max(median_so2) as  so2
from  so2_median
group by stay_id,hr),

join_with_hourly_table AS (
   SELECT a.stay_id, a.hr,b.*EXCEPT(stay_id,hr),c.*EXCEPT(stay_id,hr),d.*EXCEPT(stay_id,hr), e.*EXCEPT(stay_id,hr),f.*EXCEPT(stay_id,hr)
   FROM  `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs` a
  LEFT JOIN 
  calculated_bicarbonate_median_collapse b
  ON  a.stay_id  = b.stay_id
  AND a.hr  = b.hr
  LEFT JOIN 
  pCO2_median_collapse c
  ON  a.stay_id  = c.stay_id
  AND a.hr  = c.hr
  LEFT JOIN 
  pH_median_collapse d
  ON  a.stay_id  = d.stay_id
  AND a.hr  = d.hr
  LEFT JOIN 
  pO2_median_collapse e
  ON  a.stay_id  = e.stay_id
  AND a.hr  = e.hr
  LEFT JOIN 
  so2_median_collapse f
  ON  a.stay_id  = f.stay_id
  AND a.hr  = f.hr
  )

  select * from join_with_hourly_table
  order by stay_id, hr
