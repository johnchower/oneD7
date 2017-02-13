WITH user_group AS (
  xyz_userGroupQuery_xyz
), uc_seq AS (
SELECT user_id
	, champion_id
FROM user_connected_to_champion_bridges
WHERE user_id IN (SELECT DISTINCT id FROM user_group)
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
FROM user_group ug
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
