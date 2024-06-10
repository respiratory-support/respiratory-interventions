
WITH difference_intime_to_icuouttime AS (
    SELECT
        stay_id,
        CAST(
            FLOOR(DATETIME_DIFF(outtime, intime, MINUTE) / 60) AS INT64
        ) AS hr
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.generates_2160_hrs`
)

, icuouttime_outcome_table AS (
    SELECT
        difference_intime_to_icuouttime.stay_id,
        difference_intime_to_icuouttime.hr,
        CASE
            WHEN hr IS NULL THEN 0  
            ELSE 1 END AS icuouttime_outcome
    FROM difference_intime_to_icuouttime
    ORDER BY stay_id
)
SELECT * FROM icuouttime_outcome_table
