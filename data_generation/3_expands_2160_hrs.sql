WITH all_stays_expands_2160_hrs AS (
    SELECT *
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.generates_2160_hrs`
    CROSS JOIN
        UNNEST(`mech-vent.mv_mimiciv_v2_0_2160hrs.generates_2160_hrs`.hr_array) AS hr
)

SELECT
    all_stays_expands_2160_hrs.*EXCEPT(deathtime, dischtime, intime, hr_array)
FROM all_stays_expands_2160_hrs
order by stay_id,hr
