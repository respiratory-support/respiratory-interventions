WITH hr_join_to_discharge_hr AS (
    SELECT
        `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.*, 
        COALESCE(discharge_outcome, 0) AS discharge_outcome
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`
    LEFT JOIN `mech-vent.mv_mimiciv_v2_0_2160hrs.discharge_outcome`
        ON
            `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.stay_id = `mech-vent.mv_mimiciv_v2_0_2160hrs.discharge_outcome`.stay_id
            AND `mech-vent.mv_mimiciv_v2_0_2160hrs.expands_2160_hrs`.hr = `mech-vent.mv_mimiciv_v2_0_2160hrs.discharge_outcome`.hr
    ORDER BY stay_id,hr
),
last_hr_per_stay_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY hr DESC) AS rn
    FROM hr_join_to_discharge_hr
),
fill_in_values AS (
    SELECT
        *,
        MAX(CASE WHEN discharge_outcome = 1 THEN hr ELSE NULL END) OVER (PARTITION BY stay_id ORDER BY hr ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_hr_discharge
    FROM last_hr_per_stay_id
),
final_values AS (
    SELECT
        *,
        CASE
            WHEN discharge_outcome = 1 THEN 1
            WHEN hr > last_hr_discharge AND last_hr_discharge IS NOT NULL THEN 1
            ELSE discharge_outcome
        END AS updated_discharge_outcome
    FROM fill_in_values
),

final as (SELECT 
    subject_id, stay_id, hr,
    updated_discharge_outcome AS discharge_outcome
FROM final_values
ORDER BY subject_id, stay_id, hr)

SELECT * FROM final
order by stay_id, hr 
