WITH user_group AS(
	SELECT DISTINCT id 
	FROM user_dimensions 
	WHERE email IS NOT NULL
), user_belongs_to_cohort_prelim AS (
SELECT DISTINCT user_id
	, 1 AS belongs_to_cohort
FROM user_to_cohort_bridges
), user_belongs_to_cohort_prelim2 AS (
SELECT ug.id AS user_id
	, ub.belongs_to_cohort
FROM user_group ug
left join user_belongs_to_cohort_prelim ub
ON ug.id = ub.user_id
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
SELECT *
FROM user_belongs_to_cohort
;