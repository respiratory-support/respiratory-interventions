WITH icuouttime_outcome AS (
    SELECT
        `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.*, 
        COALESCE(icuouttime_outcome, 0) AS icuouttime_outcome
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`
    LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.icuouttime_outcome`
        ON
            `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.icuouttime_outcome`.stay_id
            AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.icuouttime_outcome`.hr
    ORDER BY stay_id,hr
),
last_hr_per_stay_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY hr DESC) AS rn
    FROM icuouttime_outcome
),
fill_in_values AS (
    SELECT
        *,
        MAX(CASE WHEN icuouttime_outcome = 1 THEN hr ELSE NULL END) OVER (PARTITION BY stay_id ORDER BY hr ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_hr_icuouttime_outcome
    FROM last_hr_per_stay_id
),
final_values AS (
    SELECT
        *,
        CASE
            WHEN icuouttime_outcome = 1 THEN 1
            WHEN hr > last_hr_icuouttime_outcome AND last_hr_icuouttime_outcome IS NOT NULL THEN 1
            ELSE icuouttime_outcome
        END AS updated_icuouttime_outcome
    FROM fill_in_values
),

final as (SELECT 
    subject_id, stay_id, hr,
    updated_icuouttime_outcome AS icuouttime_outcome
FROM final_values
ORDER BY subject_id, stay_id, hr)

SELECT * FROM final
order by stay_id, hr 
