
WITH difference_intime_to_dischtime AS (
    SELECT
        stay_id,
        CAST(
            FLOOR(DATETIME_DIFF(dischtime, intime, MINUTE) / 60) AS INT64
        ) AS hr
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.generates_2160_hrs`
)

, disch_outcome_table AS (
    SELECT
        difference_intime_to_dischtime.stay_id,
        difference_intime_to_dischtime.hr,
        CASE
            WHEN hr IS NULL THEN 0 
            ELSE 1 END AS discharge_outcome
    FROM difference_intime_to_dischtime
    ORDER BY stay_id
)

SELECT * FROM disch_outcome_table
