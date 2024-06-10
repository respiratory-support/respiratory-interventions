WITH vent_indicator AS (
    SELECT
        *,
        CASE
            WHEN ventilation_status LIKE "Invasive" THEN 1
            ELSE 0
        END
        AS invasive,
        CASE
            WHEN ventilation_status LIKE "Noninvasive" THEN 1
            ELSE 0
        END
        AS noninvasive,
        CASE
            WHEN ventilation_status LIKE "HighFlow" THEN 1
            ELSE 0
        END
        AS highflow
    FROM `mech-vent.mv_mimiciv_v2_0_2160hrs.invasive_noninvasive_highflow_hr`
    ORDER BY stay_id, hr
),

remove_transition_period_to_one_row AS (SELECT
    subject_id,
    stay_id,
    hr,
    IF(SUM(noninvasive) >= 1, 1, 0) AS noninvasive,
    IF(SUM(highflow) >= 1, 1, 0) AS highflow,
    IF(SUM(invasive) >= 1, 1, 0) AS invasive
    FROM vent_indicator
    GROUP BY subject_id, stay_id, hr
)

SELECT * FROM remove_transition_period_to_one_row
