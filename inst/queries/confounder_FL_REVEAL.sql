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
uc_seq AS (
SELECT user_id
	, champion_id
FROM user_connected_to_champion_bridges
WHERE user_id IN (SELECT DISTINCT id FROM users_existing)
AND sequence_number <= 2
), user_connected_to_FL_REVEAL_prelim AS (
SELECT ug.id AS user_id
, case
	when us.champion_id=14 OR us.champion_id=32
		THEN 1
	ELSE 0 
end AS connected_to_CFP
, case
	when us.champion_id=5
		THEN 1
	ELSE 0 
end AS connected_to_Cru
, case
	when us.champion_id=29
		THEN 1
	ELSE 0 
end AS connected_to_SummerConnect
, case
	when us.champion_id=11
		THEN 1
	ELSE 0 
end AS connected_to_TYRO
, case
	when us.champion_id=2
		THEN 1
	ELSE 0 
end AS connected_to_CeDAR
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
	, sum(connected_to_CFP) AS connected_to_CFP
	, sum(connected_to_Cru) AS connected_to_Cru
	, sum(connected_to_SummerConnect) AS connected_to_SummerConnect
	, sum(connected_to_TYRO) AS connected_to_TYRO
	, sum(connected_to_CeDAR) AS connected_to_CeDAR
	, sum(connected_to_FL) AS connected_to_FL
	, sum(connected_to_REVEAL) AS connected_to_REVEAL
FROM user_connected_to_FL_REVEAL_prelim
GROUP BY user_id
), user_connected_to_FL_REVEAL AS (
SELECT user_id
, case
	when connected_to_CFP > 0
		THEN 1
	ELSE 0
end AS connected_to_CFP
, case
	when connected_to_Cru > 0
		THEN 1
	ELSE 0
end AS connected_to_Cru
, case
	when connected_to_SummerConnect > 0
		THEN 1
	ELSE 0
end AS connected_to_SummerConnect
, case
	when connected_to_TYRO > 0
		THEN 1
	ELSE 0
end AS connected_to_TYRO
, case
	when connected_to_CeDAR > 0
		THEN 1
	ELSE 0
end AS connected_to_CeDAR
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
SELECT count(*)
        , user_id
FROM user_connected_to_FL_REVEAL
GROUP BY user_id
ORDER BY count DESC
;
