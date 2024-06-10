WITH stay_id_w_ventilation AS (
    SELECT
        subject_id,
        stay_id,
        MAX(
            CASE
                WHEN
                    invasive > 0 OR noninvasive > 0 OR highflow > 0 THEN 1
                ELSE 0
            END
        ) AS ventilated_stay
    FROM
        `mech-vent.mv_mimiciv_v2_0_2160hrs.dni_invasive24hrsbeforeicu_exclude`
    GROUP BY subject_id, stay_id  
),

chose_first_stay_vent AS (SELECT
    *,  
    RANK() OVER (PARTITION BY subject_id ORDER BY stay_id ASC) AS rank
    FROM stay_id_w_ventilation
    WHERE ventilated_stay = 1
    ORDER BY subject_id, stay_id
),

join_subject_ven_to_this_vent_tabl_beforemajormaintable AS (SELECT
        `mech-vent.mv_mimiciv_v2_0_2160hrs.dni_invasive24hrsbeforeicu_exclude`.*,
    chose_first_stay_vent.*EXCEPT(stay_id, subject_id)
    FROM chose_first_stay_vent 
    INNER JOIN
        `mech-vent.mv_mimiciv_v2_0_2160hrs.dni_invasive24hrsbeforeicu_exclude`
        ON
            chose_first_stay_vent.stay_id =  `mech-vent.mv_mimiciv_v2_0_2160hrs.dni_invasive24hrsbeforeicu_exclude`.stay_id
    WHERE rank = 1
),

final_vent_stay_three_cat AS (
    SELECT
        join_subject_ven_to_this_vent_tabl_beforemajormaintable.*EXCEPT(
            ventilated_stay, rank
        )
    FROM join_subject_ven_to_this_vent_tabl_beforemajormaintable
  
    ORDER BY stay_id, hr
)

SELECT * FROM final_vent_stay_three_cat
