WITH corrected_deathtime AS (
    SELECT
        stay_id,
        deathtime,
        dod,
        intime,
        COALESCE(deathtime, dod) AS hosp_state_deathtime
    FROM  `mech-vent.mv_mimiciv_v2_0_2160hrs.generates_2160_hrs`
),

difference_intime_to_deathtime AS (
    SELECT
        corrected_deathtime.*,
        CAST(
            FLOOR(
                DATETIME_DIFF(hosp_state_deathtime, intime, MINUTE) / 60
            ) AS INT64
        ) AS hr
    FROM
        corrected_deathtime
),

death_outcome_table AS (
    SELECT
        difference_intime_to_deathtime.*,
        CASE
            WHEN hr IS NULL THEN 0
            ELSE 1 END AS death_outcome
    FROM difference_intime_to_deathtime
    ORDER BY stay_id
)

SELECT * FROM death_outcome_table
