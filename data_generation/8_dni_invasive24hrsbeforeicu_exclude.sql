
WITH DNI as 
(
SELECT stay_id, value,itemid  
FROM `physionet-data.mimiciv_icu.chartevents`
WHERE (value LIKE '%DNI (do not intubate)%' AND itemid = 223758) OR (value LIKE '%DNR / DNI%' AND itemid = 223758)
OR (value LIKE '%%DNI (do not intubate)%' AND itemid = 228687) OR (value LIKE '%DNAR (Do Not Attempt Resuscitation) [DNR] / DNI%' AND itemid = 228687)
)

, invasive_24hr_before_icu as
(
SELECT * ,
FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.invasive_noninvasive_highflow_indicator_hr`
where invasive = 1  
AND hr >= -24     
AND hr <=-1     
)

, ventilation_after_exclude as  
(
SELECT * FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.invasive_noninvasive_highflow_indicator_hr`
WHERE stay_id NOT IN (SELECT stay_id FROM DNI)   
AND stay_id NOT IN (SELECT stay_id FROM invasive_24hr_before_icu) 
AND hr > 0  
)

SELECT DISTINCT
    subject_id, 
    stay_id,
    hr,
    invasive,
    noninvasive,
    highflow

FROM ventilation_after_exclude
WHERE hr <= 2160
ORDER BY subject_id, stay_id, hr
