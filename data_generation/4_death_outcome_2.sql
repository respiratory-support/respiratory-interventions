
WITH hr_join_to_death_hr AS (
    SELECT
       `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.*, 
        COALESCE(death_outcome, 0) AS death_outcome
    FROM`mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`
    LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome`
        ON
           `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome`.stay_id
            AND`mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.death_outcome`.hr
    ORDER BY stay_id,hr
),
last_hr_per_stay_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY hr DESC) AS rn
    FROM hr_join_to_death_hr
),
fill_in_values AS (
    SELECT
        *,
        MAX(CASE WHEN death_outcome = 1 THEN hr ELSE NULL END) OVER (PARTITION BY stay_id ORDER BY hr ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_hr_death
    FROM last_hr_per_stay_id
),
final_values AS (
    SELECT
        *,
        CASE
            WHEN death_outcome = 1 THEN 1
            WHEN hr > last_hr_death AND last_hr_death IS NOT NULL THEN 1
            ELSE death_outcome
        END AS updated_death_outcome
    FROM fill_in_values
),

final as (SELECT 
    subject_id, stay_id, hr,
    updated_death_outcome AS death_outcome
FROM final_values
ORDER BY subject_id, stay_id, hr)

SELECT * FROM final
ORDER BY stay_id, hr
