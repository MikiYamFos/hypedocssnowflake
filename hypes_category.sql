--Counts of hypes by category
SELECT 
count(CASE WHEN CATEGORY='personal' then 1 end) as personal,
count(CASE WHEN CATEGORY='family' then 1 end) as family,
count(CASE WHEN CATEGORY='work' then 1 end) as work,
count(CASE WHEN CATEGORY='fitness' then 1 end) as fitness,
count (CASE WHEN CATEGORY in ('awardsAchievements', 'award or recognition', 'awardsachievements') then 1 end) as awardsAchievements
FROM HYPEDOCS_CO.PUBLIC.HYPE_EVENTS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.HYPE_EVENTS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid;