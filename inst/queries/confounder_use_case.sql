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
),
user_max_updated_date AS (
SELECT  id
        , max(updated_date_id) AS max_updated_date_id
FROM public.user_dimensions
/* WHERE updated_date_id <= (SELECT date_id FROM run_date) */
GROUP BY id
),
user_date_max_updated_time AS (
SELECT  id
        , updated_date_id
        , max(updated_time_id) AS max_updated_time_id
FROM public.user_dimensions
/* WHERE updated_date_id <= (SELECT date_id FROM run_date) */
GROUP BY id, updated_date_id
), results AS (
SELECT  ud.id AS user_id
        , ud.account_type
FROM public.user_dimensions ud
left join user_max_updated_date umud
ON umud.id = ud.id
left join user_date_max_updated_time udmut
ON udmut.id = ud.id
AND udmut.updated_date_id = ud.updated_date_id
WHERE ud.updated_date_id = umud.max_updated_date_id
AND ud.updated_time_id = udmut.max_updated_time_id
AND ud.id IN (SELECT id FROM users_existing)
GROUP BY ud.id, ud.account_type
)
SELECT *
FROM results
;
