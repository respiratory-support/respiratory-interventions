with PBW_kg_table as (SELECT  
  stay_id,  
  height_inch, 
  CASE 
    WHEN gender = 'M' THEN 50 + 2.3 * (height_inch - 60) 
    WHEN gender = 'F' THEN 45.5 + 2.3 * (height_inch - 60) 
    ELSE NULL 
  END AS pbw_kg 
FROM (
    SELECT 
      pats.subject_id, 
      cevents.stay_id, 
      pats.gender, 
      cevents.VALUENUM as height_inch 
    FROM `physionet-data.mimiciv_icu.chartevents` cevents 
    INNER JOIN 
    `physionet-data.mimiciv_hosp.patients` pats
    on cevents.subject_id=pats.subject_id
    where itemid=226707
))
select * from PBW_kg_table 
where pbw_kg>0
