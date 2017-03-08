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
user_belongs_to_cohort_prelim AS (
SELECT DISTINCT user_id
	, 1 AS belongs_to_cohort
FROM user_to_cohort_bridges
), user_belongs_to_cohort_prelim2 AS (
SELECT ue.id AS user_id
	, ub.belongs_to_cohort
FROM users_existing ue
left join user_belongs_to_cohort_prelim ub
ON ue.id = ub.user_id
), user_belongs_to_cohort AS (
SELECT u.user_id
, (case
when u.belongs_to_cohort IS NULL
	THEN 0
ELSE u.belongs_to_cohort
END) 
	AS belongs_to_cohort
FROM user_belongs_to_cohort_prelim2 u
)
SELECT count(*)
        , user_id
FROM user_belongs_to_cohort
GROUP BY user_id
ORDER BY count DESC
;
