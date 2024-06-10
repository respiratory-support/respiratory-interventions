WITH generate_2160_hrs AS (
    SELECT * ,
    GENERATE_ARRAY(0,2160) AS hr_array
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.invasive_noninvasive_highflow`
)

,expands_2160_hrs as
(
SELECT *
FROM generate_2160_hrs 
CROSS JOIN UNNEST(generate_2160_hrs.hr_array) AS hr_starttimeref_vent  
)

,difference_starttime_to_endtime as
(
SELECT expands_2160_hrs.*EXCEPT (hr_array),
CAST(FLOOR(DATETIME_DIFF(endtime,starttime,MINUTE)/60) AS INT64) AS vent_starttime_endtime_diff
FROM expands_2160_hrs 
)
 
,difference_intime_to_starttime_added as
(
SELECT icu_intime.subject_id,difference_starttime_to_endtime.* ,
CAST(FLOOR(DATETIME_DIFF(starttime,intime,MINUTE)/60) AS INT64) AS icuintime_ventstartime_diff_hr 
FROM difference_starttime_to_endtime 
LEFT JOIN `physionet-data.mimiciv_icu.icustays` icu_intime
ON difference_starttime_to_endtime .stay_id = icu_intime.stay_id
)
,vent_hr_from_icuintime as
(
SELECT difference_intime_to_starttime_added.*EXCEPT(hr_starttimeref_vent,vent_starttime_endtime_diff,icuintime_ventstartime_diff_hr),
icuintime_ventstartime_diff_hr + hr_starttimeref_vent AS hr  
FROM difference_intime_to_starttime_added     
where vent_starttime_endtime_diff >= hr_starttimeref_vent   
)
SELECT * FROM vent_hr_from_icuintime
