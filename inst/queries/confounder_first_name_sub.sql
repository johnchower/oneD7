WITH user_group AS(
        xyz_userGroupQuery_xyz
)
SELECT id AS user_id, first_name
FROM PUBLIC.user_dimensions
WHERE user_id IN (SELECT DISTINCT id FROM user_group)
;
