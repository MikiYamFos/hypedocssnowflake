## Total users by total actions with combined goals and hypes
WITH uniongoalhypes as (SELECT USER_ID, HYPEDOCS_CO.PUBLIC.HYPE_EVENTS.DATE as DATE, STATUS, CAST('hype' as VARCHAR) as type 
FROM HYPEDOCS_CO.PUBLIC.HYPE_EVENTS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.HYPE_EVENTS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid
UNION ALL
SELECT USER_ID, HYPEDOCS_CO.PUBLIC.GOALS.DATE as DATE, STATUS, CAST('goal' as VARCHAR) as type FROM HYPEDOCS_CO.PUBLIC.GOALS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.GOALS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid),
totalbyuser as
(SELECT user_id, status, COUNT(type) OVER (PARTITION BY USER_ID) as total_action from uniongoalhypes),
users_any_actions as
(SELECT * FROM totalbyuser GROUP BY user_id, status, total_action ORDER BY total_action DESC ),
total_users_any_actions as 
(SELECT COUNT(*) as total_users_with_actions FROM users_any_actions),
over_five_total_actions as
(SELECT COUNT(*) as total_users FROM users_any_actions WHERE total_action > 5),
five_total_actions as
(SELECT COUNT(*) as total_users FROM users_any_actions WHERE total_action = 5),
four_total_actions as
(SELECT COUNT(*) as total_users FROM users_any_actions WHERE total_action = 4),
three_total_actions as
(SELECT COUNT(*) as total_users FROM users_any_actions WHERE total_action = 3),
two_total_actions as
(SELECT COUNT(*) as total_users FROM users_any_actions WHERE total_action = 2),
one_total_action as
(SELECT COUNT(*) as total_users FROM users_any_actions WHERE total_action = 1),
totals_by_status as (
SELECT
count(CASE WHEN status='BASIC' OR status='TRIAL' then 1 END) as total_current_user,
count(CASE WHEN (status='BASIC' OR status='TRIAL') AND TOTAL_ACTION > 5 then 1 END ) as current_super_user,
count(CASE WHEN status='TRIAL_ENDED' AND TOTAL_ACTION > 5 then 1 END ) as fallen_super_user,
count(CASE WHEN (status='BASIC' OR status='TRIAL') AND TOTAL_ACTION = 5 then 1 END ) as current_five_user,
count(CASE WHEN status='TRIAL_ENDED' AND TOTAL_ACTION = 5 then 1 END ) as fallen_five_user,
count(CASE WHEN (status='BASIC' OR status='TRIAL') AND TOTAL_ACTION =4  then 1 END ) as current_four_user,
count(CASE WHEN status='TRIAL_ENDED' AND TOTAL_ACTION = 4 then 1 END ) as fallen_four_user,
count(CASE WHEN (status='BASIC' OR status='TRIAL') AND TOTAL_ACTION =3  then 1 END ) as current_three_user,
count(CASE WHEN status='TRIAL_ENDED' AND TOTAL_ACTION = 3 then 1 END ) as fallen_three_user,
count(CASE WHEN (status='BASIC' OR status='TRIAL') AND TOTAL_ACTION =2  then 1 END ) as current_two_user,
count(CASE WHEN status='TRIAL_ENDED' AND TOTAL_ACTION = 2 then 1 END ) as fallen_two_user,
count(CASE WHEN (status='BASIC' OR status='TRIAL') AND TOTAL_ACTION =1  then 1 END ) as current_one_user,
count(CASE WHEN status='TRIAL_ENDED' AND TOTAL_ACTION = 1 then 1 END ) as fallen_one_user
FROM users_any_actions), 
prepavg as
(SELECT AVG(total_action) as Avg_total_action FROM users_any_actions WHERE TOTAL_ACTION <33)

SELECT * FROM totals_by_status
;