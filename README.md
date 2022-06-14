# hypedocssnowflake
SQL code for queries in Snowflake

## general queries for getting all data from tables and count of users
SELECT * FROM HYPEDOCS_CO.PUBLIC.GOALS;
SELECT * FROM HYPEDOCS_CO.PUBLIC.HYPE_EVENTS;
SELECT COUNT(*) FROM HYPEDOCS_CO.PUBLIC.USERS;
SELECT * FROM HYPEDOCS_CO.PUBLIC.USERS;

## Count of users in membership statuses
SELECT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS='BASIC'; 
SELECT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS in ('TRIAL_ENDED', 'CANCELLED'); 
SELECT  COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS = 'CANCELLED'; 
SELECT  COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS = 'UNKOWN';


## Counts of users with missions by membership status
SELECT DISTINCT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE MISSION_TEXT is not NULL; --509
SELECT DISTINCT COUNT(UID) as BASIC_USER FROM HYPEDOCS_CO.PUBLIC.USERS WHERE MISSION_TEXT is not NULL AND STATUS='BASIC'; --47
SELECT DISTINCT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE MISSION_TEXT is not NULL AND STATUS = 'TRIAL_ENDED'; --453
SELECT DISTINCT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE MISSION_TEXT is not NULL AND STATUS = 'CANCELLED'; --0

## Users with goals 
SELECT * FROM HYPEDOCS_CO.PUBLIC.GOALS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.GOALS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid ;

## Users with total goal counts
SELECT DISTINCT user_id, COUNT(ID) OVER (partition by USER_ID ORDER BY USER_ID) AS total_goals FROM HYPEDOCS_CO.PUBLIC.GOALS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.GOALS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid
ORDER BY TOTAL_GOALS DESC;

## Segmentation of users by total number of goals - alter last query after from with name of expression
### zero_goal (no goals), one goal, goals over 5 (over five goals), goals over 10 (over ten goals)
WITH user_goals as (SELECT uid, COUNT(ID) OVER (partition by USER_ID ORDER BY USER_ID) AS total_goals 
                    FROM HYPEDOCS_CO.PUBLIC.USERS 
LEFT JOIN HYPEDOCS_CO.PUBLIC.GOALS on HYPEDOCS_CO.PUBLIC.USERS.uid=HYPEDOCS_CO.PUBLIC.GOALS.user_id), 
zero_goal as
(SELECT * from user_goals WHERE total_goals=0), 
number_zero_goal as
(SELECT DISTINCT COUNT(UID) as users_zero_goals FROM zero_goal),
one_goal as(
SELECT * FROM user_goals WHERE total_goals = 1
), 
count_one_goal as (
    SELECT DISTINCT COUNT(UID) as onegoalcount from one_goal
),
goals_over_5 as(
SELECT * FROM user_goals WHERE total_goals > 5
), 
number_goals_over_five as (
    SELECT DISTINCT COUNT(UID) as count_goals_over_5 from goals_over_5
),
goals_over_10 as(
SELECT * FROM user_goals WHERE total_goals > 10
), 
number_goals_over_10 as (
    SELECT DISTINCT COUNT(UID) as count_goals_over_10 from goals_over_10
)
SELECT * FROM number_goals_over_five; 

## Counts of hypes by category
SELECT 
count(CASE WHEN CATEGORY='personal' then 1 end) as personal,
count(CASE WHEN CATEGORY='family' then 1 end) as family,
count(CASE WHEN CATEGORY='work' then 1 end) as work,
count(CASE WHEN CATEGORY='fitness' then 1 end) as fitness,
count (CASE WHEN CATEGORY in ('awardsAchievements', 'award or recognition', 'awardsachievements') then 1 end) as awardsAchievements
FROM HYPEDOCS_CO.PUBLIC.HYPE_EVENTS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.HYPE_EVENTS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid;

# Counts of hypes by users - alter final expression where clause to filter based on hypecount
WITH userhypes as (
SELECT DISTINCT USER_ID FROM HYPEDOCS_CO.PUBLIC.HYPE_EVENTS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.HYPE_EVENTS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid
), total_users_with_hypes as
(SELECT COUNT(USER_ID) FROM userhypes),
user_hype_breakdown as
(
SELECT 
USER_ID, COUNT(*) OVER (PARTITION BY USER_ID) as hypecount
FROM HYPEDOCS_CO.PUBLIC.HYPE_EVENTS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.HYPE_EVENTS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid
), hypescounts as
(SELECT * FROM user_hype_breakdown GROUP BY USER_ID, HYPECOUNT ORDER BY HYPECOUNT DESC
)
SELECT COUNT(*) FROM hypescounts WHERE HYPECOUNT> 3
;



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


