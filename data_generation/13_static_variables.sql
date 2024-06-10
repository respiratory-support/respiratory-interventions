WITH static_variables AS
    (
      SELECT
      patients.subject_id,
      patients.gender,
      patients.anchor_age,
      patients.anchor_year,
      patients.anchor_year_group,
      admissions.hadm_id,
      admissions.insurance,
      admissions.language,
      admissions.marital_status,
      admissions.race,
      admissions.admittime,
      admissions.dischtime,
      admissions.deathtime,
      admissions.admission_type, 
      admissions.admission_location,
      admissions.discharge_location,
      icu_stay.stay_id,
      icu_stay.first_careunit, icu_stay.last_careunit,
      icu_stay.intime, icu_stay.outtime, icu_stay.los
      FROM `physionet-data.mimiciv_hosp.patients` patients   
      INNER JOIN `physionet-data.mimiciv_hosp.admissions` admissions  
         ON patients.subject_id =admissions.subject_id
      INNER JOIN `physionet-data.mimiciv_icu.icustays` icu_stay 
         ON admissions.hadm_id = icu_stay.hadm_id
      ORDER BY subject_id
    )

    SELECT * FROM static_variables
