WITH user_group AS(
        xyz_userGroupQuery_xyz
) 
SELECT	u.id AS user_id
	, u.account_type AS account_type
FROM user_dimensions u
WHERE u.id IN (SELECT DISTINCT id FROM user_group)
;
