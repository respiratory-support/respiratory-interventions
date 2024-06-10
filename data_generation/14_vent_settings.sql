WITH vent_settings AS
  (
    SELECT
    vent_set.stay_id,
    vent_set.* EXCEPT(stay_id, subject_id,storetime,itemid,value,warning,hadm_id,valueuom,valuenum),
    CASE WHEN itemid in (227187) and valuenum > 0 then valuenum
    else null
    END as Pinsp_Draeger,

    CASE WHEN itemid in (229663) and valuenum > 0 then valuenum
    else null
    END as Pinsp_Hamilton,

    CASE WHEN itemid in (224695) and valuenum > 0 then valuenum 
    else null
    END as Peak_Insp_Pressure,  

    CASE WHEN itemid in (220339) then valuenum
    else null
    END as PEEP_set,

    CASE WHEN itemid in (224700) then valuenum 
    else null
    END as 	Total_PEEP_Level,   

    CASE WHEN itemid in (224702) and valuenum > 0 then valuenum
    else null
    END as PCV_Level, 
    
    CASE WHEN itemid in (220210) and valuenum > 0 then valuenum 
    else null
    END as respiratory_rate,

    CASE WHEN itemid in (224688) and valuenum > 0 then valuenum
    else null
    END as respiratory_rate_set,

    CASE WHEN itemid in (224690) and valuenum > 0 then valuenum
    else null
    END as respiratory_total,

    CASE WHEN itemid in (224684) and valuenum > 0 then valuenum
    else null
    END as Tidal_Volume_Set, 

    CASE WHEN itemid in (224685) and valuenum > 0 then valuenum
    else null
    END as Tidal_Volume_Observed, 


    CASE WHEN itemid in (223835) and valuenum > 0 then valuenum
    else null
    END as Inspired_O2_Fraction,

    CASE WHEN itemid in (226871) and valuenum > 0 then valuenum
    else null
    END as Expiratory_Ratio

    FROM
`physionet-data.mimiciv_icu.chartevents` vent_set
      where itemid in (227187,229663,
      224695,
      220339,224700,
      224702,
      220210,224688,224690,
      224684,224685,
      223835,
      226871)),
 difference_intime_to_charttime AS
  (
    SELECT
    icu.stay_id,
    vent_settings.* EXCEPT(stay_id,charttime),
    CAST(
        FLOOR(DATETIME_DIFF(charttime, intime, minute) / 60) AS INT64
    ) AS hr
    FROM `physionet-data.mimiciv_icu.icustays` icu
    INNER JOIN vent_settings 
      ON icu.stay_id = vent_settings.stay_id
      order by icu.stay_id, hr),
  ------------------------------------------
Pinsp_Draeger_median as (select stay_id, hr,Pinsp_Draeger,
PERCENTILE_CONT(Pinsp_Draeger, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Pinsp_Draeger
from difference_intime_to_charttime),

Pinsp_Draeger_median_collapse as (select stay_id,hr, max(median_Pinsp_Draeger) as pinsp_draeger
from Pinsp_Draeger_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
Pinsp_Hamilton_median as (select stay_id, hr,Pinsp_Hamilton,
PERCENTILE_CONT(Pinsp_Hamilton, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Pinsp_Hamilton
from difference_intime_to_charttime),

Pinsp_Hamilton_median_collapse as (select stay_id,hr, max(median_Pinsp_Hamilton) as pinsp_hamilton
from Pinsp_Hamilton_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
Peak_Insp_Pressure_median as (select stay_id, hr,Peak_Insp_Pressure,
PERCENTILE_CONT(Peak_Insp_Pressure, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Peak_Insp_Pressure
from difference_intime_to_charttime),

Peak_Insp_Pressure_median_collapse as (select stay_id,hr, max(median_Peak_Insp_Pressure) as ppeak
from Peak_Insp_Pressure_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
PEEP_set_median as (select stay_id, hr,PEEP_set,
PERCENTILE_CONT(PEEP_set, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_PEEP_set
from difference_intime_to_charttime),

PEEP_set_median_collapse as (select stay_id,hr, max(median_PEEP_set) as set_peep
from PEEP_set_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
Total_PEEP_Level_median as (select stay_id, hr,Total_PEEP_Level,
PERCENTILE_CONT(Total_PEEP_Level, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Total_PEEP_Level
from difference_intime_to_charttime),

Total_PEEP_Level_median_collapse as (select stay_id,hr, max(median_Total_PEEP_Level) as total_peep
from Total_PEEP_Level_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
PCV_Level_median as (select stay_id, hr,PCV_Level,
PERCENTILE_CONT(PCV_Level, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_PCV_Level
from difference_intime_to_charttime),

PCV_Level_median_collapse as (select stay_id,hr, max(median_PCV_Level) as pcv_level
from PCV_Level_median
group by stay_id,hr
order by stay_id,hr),

------------------------------------------
respiratory_rate_median as (select stay_id, hr,respiratory_rate,
PERCENTILE_CONT(respiratory_rate, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_respiratory_rate
from difference_intime_to_charttime),

respiratory_rate_median_collapse as (select stay_id,hr, max(median_respiratory_rate) as rr
from respiratory_rate_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------

respiratory_rate_set_median as (select stay_id, hr,respiratory_rate_set,
PERCENTILE_CONT(respiratory_rate_set, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_respiratory_rate_set
from difference_intime_to_charttime),

respiratory_rate_set_median_collapse as (select stay_id,hr, max(median_respiratory_rate_set) as set_rr
from respiratory_rate_set_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
respiratory_total_median as (select stay_id, hr,respiratory_total,
PERCENTILE_CONT(respiratory_total, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_respiratory_total
from difference_intime_to_charttime),

respiratory_total_median_collapse as (select stay_id,hr, max(median_respiratory_total) as total_rr
from respiratory_total_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------

Tidal_Volume_Set_median as (select stay_id, hr,Tidal_Volume_Set,
PERCENTILE_CONT(Tidal_Volume_Set, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Tidal_Volume_Set
from difference_intime_to_charttime),

Tidal_Volume_Set_median_collapse as (select stay_id,hr, max(median_Tidal_Volume_Set) as set_tv
from Tidal_Volume_Set_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
Tidal_Volume_Observed_median as (select stay_id, hr,Tidal_Volume_Observed,
PERCENTILE_CONT(Tidal_Volume_Observed, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Tidal_Volume_Observed
from difference_intime_to_charttime),

Tidal_Volume_Observed_median_collapse as (select stay_id,hr, max(median_Tidal_Volume_Observed) as total_tv
from Tidal_Volume_Observed_median
group by stay_id,hr
order by stay_id,hr),

------------------------------------------
Inspired_O2_Fraction_median as (select stay_id, hr,Inspired_O2_Fraction,
PERCENTILE_CONT(Inspired_O2_Fraction, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Inspired_O2_Fraction
from difference_intime_to_charttime),

Inspired_O2_Fraction_median_collapse as (select stay_id,hr, max(median_Inspired_O2_Fraction) as set_fio2
from Inspired_O2_Fraction_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------

Expiratory_Ratio_median as (select stay_id, hr,Expiratory_Ratio,
PERCENTILE_CONT(Expiratory_Ratio, 0.5) OVER (PARTITION BY  stay_id, hr) AS median_Expiratory_Ratio
from difference_intime_to_charttime),

Expiratory_Ratio_median_collapse as (select stay_id,hr, max(median_Expiratory_Ratio) as set_ie_ratio
from Expiratory_Ratio_median
group by stay_id,hr
order by stay_id,hr),
------------------------------------------
join_with_hourly_table AS (
   SELECT a.stay_id, a.hr,b.*EXCEPT(stay_id,hr),c.*EXCEPT(stay_id,hr),d.*EXCEPT(stay_id,hr), e.*EXCEPT(stay_id,hr),
   f.*EXCEPT(stay_id,hr),g.*EXCEPT(stay_id,hr),h.*EXCEPT(stay_id,hr),i.*EXCEPT(stay_id,hr),j.*EXCEPT(stay_id,hr),
   k.*EXCEPT(stay_id,hr),l.*EXCEPT(stay_id,hr),m.*EXCEPT(stay_id,hr),n.*EXCEPT(stay_id,hr)
   FROM  `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`  a
  LEFT JOIN 
  Pinsp_Draeger_median_collapse b
  ON  a.stay_id  = b.stay_id
  AND a.hr  = b.hr
  LEFT JOIN 
  Pinsp_Hamilton_median_collapse c
  ON  a.stay_id  = c.stay_id
  AND a.hr  = c.hr
   LEFT JOIN 
  Peak_Insp_Pressure_median_collapse d
  ON  a.stay_id  = d.stay_id
  AND a.hr  = d.hr
   LEFT JOIN 
  PEEP_set_median_collapse e
  ON  a.stay_id  = e.stay_id
  AND a.hr  = e.hr
  LEFT JOIN 
  Total_PEEP_Level_median_collapse f
  ON  a.stay_id  = f.stay_id
  AND a.hr  = f.hr
  LEFT JOIN 
  PCV_Level_median_collapse g
  ON  a.stay_id  = g.stay_id
  AND a.hr  = g.hr
  LEFT JOIN 
  respiratory_rate_median_collapse h
  ON  a.stay_id  = h.stay_id
  AND a.hr  = h.hr
  LEFT JOIN 
  respiratory_rate_set_median_collapse i
  ON  a.stay_id  = i.stay_id
  AND a.hr  = i.hr

  LEFT JOIN 
  respiratory_total_median_collapse j
  ON  a.stay_id  = j.stay_id
  AND a.hr  = j.hr
  LEFT JOIN 
  Tidal_Volume_Set_median_collapse k
  ON  a.stay_id  = k.stay_id
  AND a.hr  = k.hr
  LEFT JOIN 
  Tidal_Volume_Observed_median_collapse l
  ON  a.stay_id  = l.stay_id
  AND a.hr  = l.hr
  
  LEFT JOIN 
  Inspired_O2_Fraction_median_collapse m
  ON  a.stay_id  = m.stay_id
  AND a.hr  = m.hr

  LEFT JOIN 
  Expiratory_Ratio_median_collapse n
  ON  a.stay_id  = n.stay_id
  AND a.hr  = n.hr
  ),

add_set_pc_dreager as (select * ,
(pinsp_draeger-set_peep) as set_pc_draeger
from join_with_hourly_table
),

add_priority_pc_set as (select *,
CASE WHEN pcv_level IS NOT NULL THEN pcv_level
WHEN pcv_level IS NULL AND pinsp_hamilton IS NOT NULL THEN pinsp_hamilton
WHEN pcv_level IS NULL  AND pinsp_hamilton IS NULL THEN set_pc_draeger
ELSE NULL
END AS set_pc 
from add_set_pc_dreager)

select * from add_priority_pc_set
order by stay_id,hr
