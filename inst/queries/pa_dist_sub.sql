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
user_pa_datetime AS(
SELECT upaf.user_id
	, upaf.platform_action
	, to_timestamp(
		COALESCE(dd.sql_date_stamp::TEXT, '') 
		|| ' ' 
		|| COALESCE(td.minute_description::TEXT, '') 
		, 'YYYY-MM-DD HH24:MI'
		) AS date_time
FROM user_platform_action_facts upaf
left join date_dim dd
ON upaf.date_id=dd.id
left join time_dim td
ON upaf.time_id=td.id
WHERE upaf.user_id IN (SELECT id FROM users_existing)
), user_min_datetime AS(
SELECT upd.user_id
	, min(upd.date_time) AS first_action
FROM user_pa_datetime upd
GROUP BY user_id
), user_relative_pa_datetime AS(
SELECT upd.user_id
	, upd.platform_action
	, upd.date_time
	, umd.first_action
	, extract(epoch FROM upd.date_time - umd.first_action)/60 
          AS relative_date_time
FROM user_pa_datetime upd
left join user_min_datetime umd
ON umd.user_id=upd.user_id
), user_pa_count AS(
SELECT urpd.user_id
	, pfc.flash_report_category
	, count(*) AS count_platform_actions
FROM user_relative_pa_datetime urpd
left join pa_flash_cat pfc
ON pfc.platform_action=urpd.platform_action
WHERE urpd.relative_date_time<=xyz_maxTime_xyz
AND urpd.relative_date_time>=xyz_minTime_xyz
GROUP BY user_id, pfc.flash_report_category
), agg_pa_count AS(
SELECT flash_report_category
      , sum(count_platform_actions) AS count_platform_actions
FROM user_pa_count
GROUP BY flash_report_category
), user_pa_count_total AS(
SELECT user_id
	, sum(count_platform_actions) AS total_platform_actions
FROM user_pa_count upc	
GROUP BY user_id
), agg_pa_count_total AS(
SELECT sum(count_platform_actions) AS total_platform_actions
FROM agg_pa_count
), user_pa_pct AS(
SELECT upc.*
	, upct.total_platform_actions
	, 1.0*upc.count_platform_actions/upct.total_platform_actions 
          AS pct_platform_actions
FROM user_pa_count upc
left join user_pa_count_total upct
ON upc.user_id=upct.user_id
), agg_pa_pct AS(
SELECT apc.*
	, apct.total_platform_actions
	, 1.0*apc.count_platform_actions/apct.total_platform_actions 
          AS pct_platform_actions
FROM agg_pa_count apc
left join agg_pa_count_total apct
ON TRUE
), results AS(
xyz_resultQuery_xyz
)
SELECT DISTINCT * FROM results
;
