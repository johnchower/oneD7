WITH user_group AS(
	SELECT DISTINCT id 
	FROM user_dimensions 
), uc_seq AS (
SELECT user_id
     , MAX(sequence_number) AS max_sequence
     , MIN(sequence_number) AS min_sequence
FROM user_connected_to_champion_bridges
GROUP BY user_id
), user_confounder AS(
SELECT	u.id AS user_id
	, u.account_type AS account_type
        , ( 
        CASE
        WHEN c.champion_id=6
            THEN 'FamilyLife'
        WHEN c.champion_id=9
            THEN 'REVEAL for Church'
        ELSE 'other'
        END
        ) AS use_case
FROM 	user_dimensions u
LEFT JOIN user_connected_to_champion_bridges c
	ON u.id=c.user_id
LEFT JOIN uc_seq 
	ON uc_seq.user_id=u.id
LEFT JOIN champion_dimensions cd
	ON cd.id=c.champion_id
WHERE uc_seq.min_sequence=c.sequence_number
)
SELECT count(DISTINCT user_id)
  , use_case
FROM user_confounder
WHERE user_id IN (SELECT * FROM user_group)
GROUP BY user_confounder.use_case
;
