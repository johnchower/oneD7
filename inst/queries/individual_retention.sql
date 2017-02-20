WITH user_group AS(
	SELECT DISTINCT ud.id 
	FROM user_dimensions ud
        left join public.user_platform_action_facts upaf
        ON upaf.user_id=ud.id
	WHERE ud.email IS NOT NULL
        AND upaf.platform_action='Account Created'
), firstweek_by_user AS(
	SELECT sdf.user_id 
		, min(dd.calendar_week_start_date) AS first_week
		, floor((current_date-min(dd.calendar_week_start_date))/7) 
			AS max_relative_session_week
	FROM session_duration_fact sdf
	left join date_dim dd
	ON dd.id=sdf.date_id
	WHERE sdf.user_id IN (SELECT id FROM user_group)
	GROUP BY user_id
), relative_session_week_by_user_0 AS(
	SELECT sdf.user_id
		, floor((dd.calendar_week_start_date-fwbu.first_week)/7) 
                  AS relative_session_week
                , 1 AS active  
	FROM session_duration_fact sdf
	left join date_dim dd
	ON dd.id=sdf.date_id
	left join firstweek_by_user fwbu
	ON fwbu.user_id=sdf.user_id
	WHERE sdf.user_id IN (SELECT id FROM user_group)
        ORDER BY user_id, relative_session_week
), relative_session_week_by_user AS (
        SELECT DISTINCT *
        FROM relative_session_week_by_user_0
        ORDER BY user_id, relative_session_week
), week_series AS (
        SELECT DISTINCT relative_session_week
        FROM relative_session_week_by_user
), user_relweek_empty AS(
        SELECT ug.id AS user_id
          , ws.relative_session_week
        FROM user_group ug
        cross join week_series ws
        WHERE ws.relative_session_week IS NOT NULL
), user_relweek_active AS (
        SELECT ure.user_id
          , ure.relative_session_week
          , rswbu.active
        FROM user_relweek_empty ure
        left join relative_session_week_by_user rswbu
        ON rswbu.user_id=ure.user_id
        AND rswbu.relative_session_week
            =ure.relative_session_week
        ORDER BY user_id, relative_session_week
)
SELECT *
FROM user_relweek_active
;
