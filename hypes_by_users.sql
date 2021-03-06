--Counts of hypes by users - alter final expression where clause to filter based on hypecount
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