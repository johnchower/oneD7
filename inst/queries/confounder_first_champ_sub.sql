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
        , sequence_number
FROM user_connected_to_champion_bridges
),
user_firstsecond0 AS (
SELECT user_id
        , sum( (sequence_number=1)::INTEGER*champion_id ) AS champ1
        , sum( (sequence_number=2)::INTEGER*champion_id ) AS champ2
FROM uc_seq
GROUP BY user_id
),
user_firstsecond AS (
SELECT ue.id AS user_id
        , uf.champ1
        , uf.champ2
FROM users_existing ue
left join user_firstsecond0 uf
ON ue.id=uf.user_id
),
user_first_champ AS (
SELECT user_id
        , case
        when champ1 IS NULL
          THEN 0
        when champ1!=1
          THEN champ1 
        when champ1=1 AND champ2=0
          THEN champ1
        when champ1=1 AND champ2!=0
          THEN champ2
        END AS first_champ
FROM user_firstsecond
)
SELECT *
FROM user_first_champ
WHERE user_id IN (SELECT id FROM users_existing)
;
