--Count of users in membership statuses
SELECT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS='BASIC'; 
SELECT COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS in ('TRIAL_ENDED', 'CANCELLED'); 
SELECT  COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS = 'CANCELLED'; 
SELECT  COUNT(UID) FROM HYPEDOCS_CO.PUBLIC.USERS WHERE STATUS = 'UNKOWN';