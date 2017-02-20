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
),
uc_seq AS (
SELECT user_id
	, champion_id
FROM user_connected_to_champion_bridges
WHERE user_id IN (SELECT DISTINCT id FROM users_existing)
AND sequence_number <= 2
), user_connected_to_FL_REVEAL_prelim AS (
SELECT ug.id AS user_id
, case
	when us.champion_id=6
		THEN 1
	ELSE 0 
end AS connected_to_FL
, case
	when us.champion_id=9
		THEN 1
	ELSE 0 
end AS connected_to_REVEAL
FROM users_existing ug
left join uc_seq us
ON ug.id=us.user_id
), user_connected_to_FL_REVEAL_prelim2 AS (
SELECT user_id
	, sum(connected_to_FL) AS connected_to_FL
	, sum(connected_to_REVEAL) AS connected_to_REVEAL
FROM user_connected_to_FL_REVEAL_prelim
GROUP BY user_id
), user_connected_to_FL_REVEAL AS (
SELECT user_id
, case
	when connected_to_FL > 0
		THEN 1
	ELSE 0
end AS connected_to_FL
, case
	when connected_to_REVEAL > 0
		THEN 1
	ELSE 0
end AS connected_to_REVEAL
FROM user_connected_to_FL_REVEAL_prelim2
)
SELECT *
FROM user_connected_to_FL_REVEAL
;
