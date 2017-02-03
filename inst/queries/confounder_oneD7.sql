WITH user_group AS(
  xyz_userGroupQuery_xyz
), user_sessiondate AS(
SELECT sdf.user_id AS user_id
	, dd.sql_date_stamp AS session_date
FROM session_duration_fact sdf
left join date_dim dd
ON dd.id=sdf.date_id
WHERE sdf.user_id IN (SELECT id FROM user_group)
), user_minsessiondate AS(
SELECT user_id
	, min(session_date) AS min_session_date
FROM user_sessiondate
GROUP BY user_id
), user_relative_session_date AS(
SELECT us.user_id
	, (us.session_date - um.min_session_date)
		AS relative_session_date
FROM user_sessiondate us
left join user_minsessiondate um
ON us.user_id=um.user_id
), user_1D7_prelim AS(
SELECT ursd.user_id
	, (
		case
		when (ursd.relative_session_date > 0) AND (ursd.relative_session_date <= 7)
		THEN 1
		ELSE 0
		end
	) AS good_day
FROM user_relative_session_date ursd
), user_1D7_prelim2 AS(
SELECT user_id
	, sum(good_day) AS oneD7
FROM user_1D7_prelim	 
GROUP BY user_id
), user_1D7 AS(
SELECT user_id
	, (case
	when oneD7>0
		THEN 1
	ELSE 0
	end) AS oneD7
FROM user_1D7_prelim2
)
SELECT *
FROM user_1D7
ORDER BY user_id
limit 100
;

