WITH user_group AS(
  xyz_userGroupQuery_xyz
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
), relative_session_week_by_user AS(
	SELECT sdf.user_id
		, floor((dd.calendar_week_start_date-fwbu.first_week)/7) AS relative_session_week
	FROM session_duration_fact sdf
	left join date_dim dd
	ON dd.id=sdf.date_id
	left join firstweek_by_user fwbu
	ON fwbu.user_id=sdf.user_id
	WHERE sdf.user_id IN (SELECT id FROM user_group)
), active_users_per_week AS(
	SELECT relative_session_week 
		, count(DISTINCT user_id) active_users
	FROM relative_session_week_by_user
	GROUP BY relative_session_week
	ORDER BY relative_session_week
), eligible_users_per_max_week AS(
	SELECT max_relative_session_week
		, count(DISTINCT user_id) AS eligible_users
	FROM firstweek_by_user	
	GROUP BY max_relative_session_week
	ORDER BY max_relative_session_week
), total_eligible_users_per_max_week AS(
	SELECT max_relative_session_week
		, sum(eligible_users) 
			over(
				ORDER BY max_relative_session_week DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			)
			AS total_eligible_users
	FROM eligible_users_per_max_week
), results AS(
SELECT au.relative_session_week
	, max(teu.total_eligible_users)
		over(
			ORDER BY au.relative_session_week DESC 
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		)		
		AS total_eligible_users
	, au.active_users
	, 1.0*au.active_users/(
		max(teu.total_eligible_users)
		over(
			ORDER BY au.relative_session_week DESC 
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		)		
	) AS pct_active
FROM active_users_per_week au
left join total_eligible_users_per_max_week teu
ON au.relative_session_week=teu.max_relative_session_week
ORDER BY max_relative_session_week
)
SELECT * FROM results
ORDER BY relative_session_week
;
