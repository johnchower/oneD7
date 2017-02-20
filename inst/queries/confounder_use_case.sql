WITH 
user_group AS(
	SELECT DISTINCT ud.id 
	FROM user_dimensions ud
        left join public.user_platform_action_facts upaf
        ON upaf.user_id=ud.id
	WHERE ud.email IS NOT NULL
        AND upaf.platform_action='Account Created'
), 
run_date AS (
SELECT id as date_id FROM date_dim where id=20170201
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
