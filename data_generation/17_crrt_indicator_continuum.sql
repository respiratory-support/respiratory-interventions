WITH crrt_settings AS
(
  SELECT  ce.stay_id,
          ce.charttime,
          MIN(ce.itemid) as itemid,
          MIN(ce.value) as value,
          MIN(ce.valuenum) as valuenum,
          max(
          CASE
                    WHEN ce.itemid IN ( 224144, -- Blood Flow (ml/min)
                                        224191  -- Hourly Patient Fluid Removal    
                                      ) THEN 1
                    ELSE 0
          END ) AS rrt
  FROM     `physionet-data.mimiciv_icu.chartevents` ce
  WHERE    ce.value IS NOT NULL and ce.itemid IN (224144, 224191) 
  AND      ce.valuenum IS NOT NULL AND  ce.valuenum >0
  GROUP BY stay_id,
           charttime, 
           ce.value,
           ce.valuenum )

,hr_table AS
(
  SELECT      crrt_settings.stay_id,
              itemid,
              value,
              charttime,
              rrt,
              cast(floor(datetime_diff(charttime, intime, minute) / 60) AS int64 ) AS hr
  FROM       `physionet-data.mimiciv_icu.icustays` icu
  INNER JOIN crrt_settings
  ON         icu.stay_id = crrt_settings.stay_id ) 

, crrt AS (
    SELECT 
    stay_id,
    MIN(hr) AS start_hr,
    MAX(hr) AS end_hr
    FROM hr_table
    WHERE rrt = 1
    GROUP BY stay_id
)

, crrt_hourly AS (
    SELECT 
    h.stay_id,
    h.hr,
    crrt.start_hr,
    crrt.end_hr,
    CASE WHEN h.hr >= crrt.start_hr AND h.hr <= crrt.end_hr
    THEN 1
    ELSE 0 END AS crrt
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs` h 
    INNER JOIN crrt
    ON h.stay_id = crrt.stay_id
)

SELECT * FROM crrt_hourly
