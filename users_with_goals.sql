--Users with goals 
SELECT * FROM HYPEDOCS_CO.PUBLIC.GOALS
LEFT JOIN HYPEDOCS_CO.PUBLIC.USERS on HYPEDOCS_CO.PUBLIC.GOALS.USER_ID=HYPEDOCS_CO.PUBLIC.USERS.uid ;