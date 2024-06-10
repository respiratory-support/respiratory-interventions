WITH generates_2160_hrs AS (
    SELECT
        `physionet-data.mimiciv_hosp.patients`.subject_id,
        `physionet-data.mimiciv_hosp.admissions`.hadm_id,
        `physionet-data.mimiciv_hosp.admissions`.deathtime,
        `physionet-data.mimiciv_hosp.patients`.dod,
        `physionet-data.mimiciv_hosp.admissions`.dischtime,
        `physionet-data.mimiciv_icu.icustays`.stay_id,
        `physionet-data.mimiciv_icu.icustays`.intime,`physionet-data.mimiciv_icu.icustays`.outtime,
        GENERATE_ARRAY(0, 2160) AS hr_array,
        DENSE_RANK() OVER (PARTITION BY  `physionet-data.mimiciv_hosp.patients`.subject_id 
        ORDER BY `physionet-data.mimiciv_icu.icustays`.intime) AS stay_number,  

    FROM `physionet-data.mimiciv_hosp.patients`
    INNER JOIN `physionet-data.mimiciv_hosp.admissions`
        ON
            `physionet-data.mimiciv_hosp.patients`.subject_id = `physionet-data.mimiciv_hosp.admissions`.subject_id
    INNER JOIN `physionet-data.mimiciv_icu.icustays`
        ON
            `physionet-data.mimiciv_hosp.admissions`.hadm_id = `physionet-data.mimiciv_icu.icustays`.hadm_id
    ORDER BY
        `physionet-data.mimiciv_hosp.patients`.subject_id,
        `physionet-data.mimiciv_hosp.admissions`.hadm_id,
        `physionet-data.mimiciv_icu.icustays`.stay_id
)

SELECT * FROM generates_2160_hrs 
WHERE stay_number = 1
