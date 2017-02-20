WITH 
user_group AS(
  xyz_userGroupQuery_xyz
), 
run_date AS (
  xyz_runDateQuery_xyz
),
users_existing AS (
SELECT DISTINCT ug.id
FROM user_group ug
left join public.user_platform_action_facts upaf
ON upaf.user_id=ug.id
WHERE upaf.platform_action='Account Created'
AND upaf.date_id <= (SELECT date_id FROM run_date)
)
SELECT	u.id AS user_id
	, u.account_type AS account_type
FROM user_dimensions u
WHERE u.id IN (SELECT DISTINCT id FROM users_existing)
;
