--  Context
USE DATABASE HRMS; 
USE SCHEMA HR; 

-- look at the stage
desc stage AWS_ET_CSV_STAGE ;
-------------------------
-- Create external table
-------------------------
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_REFRESH 
( 
EMPLOYEE_ID    VARCHAR   AS (VALUE:c1::VARCHAR), 
FIRST_NAME     VARCHAR   AS (VALUE:c2::VARCHAR), 
LAST_NAME      VARCHAR   AS (VALUE:c3::VARCHAR), 
EMAIL          
VARCHAR   AS (VALUE:c4::VARCHAR), 
PHONE_NUMBER   VARCHAR   AS (VALUE:c5::VARCHAR), 
HIRE_DATE      VARCHAR   AS (VALUE:c6::VARCHAR), 
JOB_ID         
VARCHAR   AS (VALUE:c7::VARCHAR), 
SALARY         
VARCHAR   AS (VALUE:c8::VARCHAR), 
COMMISSION_PCT VARCHAR   AS (VALUE:c9::VARCHAR), 
MANAGER_ID     VARCHAR   AS (VALUE:c10::VARCHAR), 
DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
) 
LOCATION=@AWS_ET_CSV_STAGE 
AUTO_REFRESH= TRUE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = CSV_ET_FILEFORMAT; 
desc  table  EXT_EMPLOYEES_REFRESH;
--------------------
--  Get info about an External Table and the notification process (listening for sqs messages -- like a  "pipe") 
--------------------
DESC EXTERNAL TABLE EXT_EMPLOYEES_REFRESH; -- shows a row for the "value" variant and a row per table column
SHOW EXTERNAL TABLES LIKE 'EXT_EMPLOYEES_REFRESH'; -- 1 row that includes NOTIFICATION_CHANNEL
SELECT SYSTEM$EXTERNAL_TABLE_PIPE_STATUS('EXT_EMPLOYEES_REFRESH');  -- Tells if the listner is ACTIVE in polling for SQS messages and if there are messages to be consumed

--------------------
-- File Registration
--------------------
-- Manual: This is a full true-up of metadata to s3
ALTER EXTERNAL TABLE EXT_EMPLOYEES_REFRESH SET AUTO_REFRESH=TRUE;  -- triggers a metadata true-up to s3
SELECT COUNT(*) FROM EXT_EMPLOYEES_REFRESH; --60   
SELECT * FROM EXT_EMPLOYEES_REFRESH; 
SELECT * FROM HRMS.HR.EXT_EMPLOYEES_REFRESH; 
-- Automatic Registration (similar to a pipe)
SHOW EXTERNAL TABLES LIKE 'EXT_EMPLOYEES_REFRESH';  -- presence of NOTIFICATION_CHANNEL indicates AUTO_REFRESH=TRUE
-- aws: Configure the prefix/suffix/Destination (=NOTIFICATION_CHANNEL) on the bucket


-------------------
-- What files are in s3 RIGHT NOW? Historically?  (remember, Snowflake likes to keep fuull history)
-------------------
-- What files are in s3 RIGHT NOW?
SELECT  DISTINCT  METADATA$FILENAME, max(METADATA$FILE_ROW_NUMBER) rowcount FROM EXT_EMPLOYEES_REFRESH 
group by METADATA$FILENAME;
-- What files are in s3 right now (does not look at registration)
LIST @AWS_ET_CSV_STAGE;  -- instantly updated (does not need a refresh of metadata)

-- What files have ever been seen (since last "true-up" at time of create or ALTER REFRESH)
SELECT *  -- Returns the full historical metadata Snowflake has recorded for every file ever discovered in the external table’s location, ordered from most recently modified to oldest. Some of this metadata may be stale.
FROM TABLE(
  INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(
    TABLE_NAME => 'EXT_EMPLOYEES_REFRESH'
  )
)
ORDER BY LAST_MODIFIED DESC;
-- Get more detail on the registration status 
SELECT * FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILE_REGISTRATION_HISTORY('EXT_EMPLOYEES')); -- Full history with details 

-------------------
--  Query the external table
-------------------
SELECT COUNT(*) FROM EXT_EMPLOYEES_REFRESH; -- 105

-- CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_REFRESH 
-- ( 
-- EMPLOYEE_ID    VARCHAR   AS (VALUE:c1::VARCHAR), 
-- FIRST_NAME     VARCHAR   AS (VALUE:c2::VARCHAR), 
-- LAST_NAME      VARCHAR   AS (VALUE:c3::VARCHAR), 
-- EMAIL          
-- VARCHAR   AS (VALUE:c4::VARCHAR), 
-- PHONE_NUMBER   VARCHAR   AS (VALUE:c5::VARCHAR), 
-- HIRE_DATE      VARCHAR   AS (VALUE:c6::VARCHAR), 
-- JOB_ID         
-- VARCHAR   AS (VALUE:c7::VARCHAR), 
-- SALARY         
-- VARCHAR   AS (VALUE:c8::VARCHAR), 
-- COMMISSION_PCT VARCHAR   AS (VALUE:c9::VARCHAR), 
-- MANAGER_ID     VARCHAR   AS (VALUE:c10::VARCHAR), 
-- DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
-- ) 
-- LOCATION=@AWS_STAGE 
-- AUTO_REFRESH= TRUE 
-- PATTERN='.*employees.*[.]csv' 
-- FILE_FORMAT = CSV_ET_FILEFORMAT;