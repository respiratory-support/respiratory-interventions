
WITH  icustay as (SELECT icustay.stay_id,icustay.intime,icustay.outtime
FROM `physionet-data.mimiciv_icu.icustays` as icustay)
SELECT `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr,
`mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.subject_id,
`mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hadm_id,
`mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id,
icustay.intime,icustay.outtime,
--vent
COALESCE(invasive, 0) AS invasive,
COALESCE(noninvasive, 0) AS noninvasive,
COALESCE(highflow, 0) AS highflow,
--discharge
discharge_outcome,
--icuouttime
icuouttime_outcome,
`mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome_2`.death_outcome,
--elixhauser
`mech-vent.mv_mimiciv_v2_0_2160hrs.elixhauser_score`.elixhauser_vanwalraven,	
--gcs
`mech-vent.mv_mimiciv_v2_0_2160hrs.gcs`.* EXCEPT (stay_id, hr),
--static
`mech-vent.mv_mimiciv_v2_0_2160hrs.static_variables`.*EXCEPT(subject_id,hadm_id,stay_id,
admittime,admission_location,admission_type,dischtime, discharge_location, deathtime,last_careunit,intime,outtime,anchor_year_group),
--vent settings
`mech-vent.mv_mimiciv_v2_0_2160hrs.vent_settings`.* EXCEPT (stay_id, hr),
--pbw
`mech-vent.mv_mimiciv_v2_0_2160hrs.pbw`.* EXCEPT (stay_id),
--labs
`mech-vent.mv_mimiciv_v2_0_2160hrs.labs`.* EXCEPT (stay_id, hr),
--vasopressor
COALESCE(vasopressor, 0) AS vasopressor,
--crrt
COALESCE(crrt,0) AS crrt,
`mech-vent.mv_mimiciv_v2_0_2160hrs.vital_signs`.*EXCEPT(stay_id, hr),
--sepsis3
COALESCE(sepsis3, 0) AS sepsis3,
--sofa
`mech-vent.mv_mimiciv_v2_0_2160hrs.sofa_score`.sofa_24hours,
FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`
--ventilation
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.first_ventilated_stay_per_subject`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id =`mech-vent.mv_mimiciv_v2_0_2160hrs.first_ventilated_stay_per_subject`.stay_id 
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.first_ventilated_stay_per_subject`.hr 

--death_outcome
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome_2`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome_2`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =  `mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome_2`.hr
--discharge
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.discharge_outcome_2`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.discharge_outcome_2`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.discharge_outcome_2`.hr
--iciuoutime
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.icuouttime_outcome_2`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.icuouttime_outcome_2`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.icuouttime_outcome_2`.hr
-- elixhauser score 
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.elixhauser_score` 
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.elixhauser_score`.stay_id
-- static 
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.static_variables`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.static_variables`.stay_id 
--vasopressor 
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.vasopressors_ne_epi_do_phe_vas`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id =`mech-vent.mv_mimiciv_v2_0_2160hrs.vasopressors_ne_epi_do_phe_vas`.stay_id 
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =`mech-vent.mv_mimiciv_v2_0_2160hrs.vasopressors_ne_epi_do_phe_vas`.hr 
--vital signs
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.vital_signs`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id =`mech-vent.mv_mimiciv_v2_0_2160hrs.vital_signs`.stay_id 
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =`mech-vent.mv_mimiciv_v2_0_2160hrs.vital_signs`.hr 
--crrt
LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.crrt`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.crrt`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =  `mech-vent.mv_mimiciv_v2_0_2160hrs.crrt`.hr
--sepsis3 onset
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.sepsis3`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.sepsis3`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =  `mech-vent.mv_mimiciv_v2_0_2160hrs.sepsis3`.hr
--sofa score
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.sofa_score`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.sofa_score`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =  `mech-vent.mv_mimiciv_v2_0_2160hrs.sofa_score`.hr
--vent settings
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.vent_settings`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.vent_settings`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =  `mech-vent.mv_mimiciv_v2_0_2160hrs.vent_settings`.hr
--labs
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.labs`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.labs`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr =  `mech-vent.mv_mimiciv_v2_0_2160hrs.labs`.hr
--gcs
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.gcs`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.gcs`.stay_id
AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.gcs`.hr
--PWB
LEFT JOIN  `mech-vent.mv_mimiciv_v2_0_2160hrs.pbw`
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.pbw`.stay_id
--icustay
LEFT JOIN icustay
ON `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = icustay.stay_id
ORDER BY subject_id, hadm_id, stay_id, hr
