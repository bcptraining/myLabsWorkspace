---------------------
-- Create storage integration udemy_mc_a1_si
---------------------
CREATE or replace STORAGE INTEGRATION udemy_mc_a1_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
  's3://vdw-dev-ingest/loadingdatalabs/json/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/');

  desc integration udemy_mc_a1_si;
--#####################################  
--External Tables Section LAB 1 
--#####################################  
USE DATABASE HRMS; 
USE SCHEMA HR; 
----------------------------------
-- Create File Format and Stage
----------------------------------
CREATE OR REPLACE FILE FORMAT HRMS.HR.CSV_ET_FILEFORMAT 
TYPE = CSV 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 0 
NULL_IF = ('NULL','Null','null') 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
EMPTY_FIELD_AS_NULL = TRUE; 

desc file format HRMS.HR.CSV_ET_FILEFORMAT;

CREATE OR REPLACE STAGE HRMS.HR.AWS_ET_CSV_STAGE 
STORAGE_INTEGRATION = udemy_mc_a1_si
-- URL = 's3://learn2cloud-snowflake/external_table/csv/' 
URL = 's3://vdw-dev-ingest/loadingdatalabs/labcsv/'
FILE_FORMAT = HRMS.HR.CSV_ET_FILEFORMAT; 
SHOW STORAGE INTEGRATIONS LIKE 'udemy_mc_a1_si';


LIST @AWS_ET_CSV_STAGE; 
 -- query the data to confirm
show stages in account;
SELECT 
T.$1, 
T.$2, 
T.$3, 
T.$4, 
T.$5, 
T.$6 , 
T.$7, 
T.$8, 
T.$9, 
T.$10, 
T.$11 
FROM @AWS_ET_CSV_STAGE (PATTERN => '.*employee.*\.csv') T;
-----------------------------
-- Create external table
-----------------------------
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES 
( 
EMPLOYEE_ID    VARCHAR   AS (VALUE:c1::VARCHAR), 
FIRST_NAME     VARCHAR   AS (VALUE:c2::VARCHAR), 
LAST_NAME      VARCHAR   AS (VALUE:c3::VARCHAR), 
EMAIL          VARCHAR   AS (VALUE:c4::VARCHAR), 
PHONE_NUMBER   VARCHAR   AS (VALUE:c5::VARCHAR), 
HIRE_DATE      VARCHAR   AS (VALUE:c6::VARCHAR), 
JOB_ID         VARCHAR   AS (VALUE:c7::VARCHAR), 
SALARY         VARCHAR   AS (VALUE:c8::VARCHAR), 
COMMISSION_PCT VARCHAR   AS (VALUE:c9::VARCHAR), 
MANAGER_ID     VARCHAR   AS (VALUE:c10::VARCHAR), 
DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
) 
LOCATION=@AWS_ET_CSV_STAGE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = CSV_ET_FILEFORMAT; 


DESCRIBE EXTERNAL TABLE EXT_EMPLOYEES; 
SHOW COLUMNS IN TABLE EXT_EMPLOYEES;
---------------------------
--  Explore data and metadata
---------------------------

SELECT * FROM EXT_EMPLOYEES;  -- Note: value column is variant (json)

SELECT METADATA$FILENAME,METADATA$FILE_ROW_NUMBER FROM 
EXT_EMPLOYEES; 

SELECT DISTINCT METADATA$FILENAME  FROM EXT_EMPLOYEES; 
-- List row counts by file
SELECT METADATA$FILENAME,MAX(METADATA$FILE_ROW_NUMBER) FROM 
EXT_EMPLOYEES 
GROUP BY METADATA$FILENAME 
ORDER BY 2 DESC; 
-- This table function give you metadata about the files
SELECT * FROM 
TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES('EXT_EMPLOYEES')); 
-- This table function give you metadtaa about the files and their registration status
SELECT * FROM 
TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILE_REGISTRATION_HISTORY('EXT_EMPLOYEES')); 
-----------------------------
-- Re-create external table so HIRE_DATE is formatted differently
-----------------------------
SELECT * FROM EXT_EMPLOYEES; 
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_FORMAT_DATE 
( 
EMPLOYEE_ID    VARCHAR   AS (VALUE:c1::VARCHAR), 
FIRST_NAME     VARCHAR   AS (VALUE:c2::VARCHAR), 
LAST_NAME      VARCHAR   AS (VALUE:c3::VARCHAR), 
EMAIL          VARCHAR   AS (VALUE:c4::VARCHAR), 
PHONE_NUMBER   VARCHAR   AS (VALUE:c5::VARCHAR), 
HIRE_DATE      VARCHAR   AS TO_CHAR(TRY_TO_DATE(VALUE:c6::VARCHAR,'YYYY-MM-DD'),'DD-MON-YYYY'), 
JOB_ID         VARCHAR   AS (VALUE:c7::VARCHAR), 
SALARY         VARCHAR   AS (VALUE:c8::VARCHAR), 
COMMISSION_PCT VARCHAR   AS (VALUE:c9::VARCHAR), 
MANAGER_ID     VARCHAR   AS (VALUE:c10::VARCHAR), 
DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
) 
LOCATION=@AWS_ET_CSV_STAGE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = CSV_ET_FILEFORMAT; 

SELECT * FROM EXT_EMPLOYEES; 
SELECT * FROM EXT_EMPLOYEES_FORMAT_DATE; 
DESC EXTERNAL TABLE EXT_EMPLOYEES_FORMAT_DATE; 

-----------------------------
-- Re-create external table so EMPLOYEE_ID is number and COMMISSION_PCT is  NUMBER(38,2)
-----------------------------
CREATE OR REPLACE EXTERNAL TABLE 
EXT_EMPLOYEES_FORMAT_DATE_EMPLOYEE_ID_COMMISSION_PCT 
( 
EMPLOYEE_ID    NUMBER    AS TRY_TO_NUMBER(VALUE:c1::VARCHAR), 
FIRST_NAME     VARCHAR   AS (VALUE:c2::VARCHAR), 
LAST_NAME      VARCHAR   AS (VALUE:c3::VARCHAR), 
EMAIL          
VARCHAR   AS (VALUE:c4::VARCHAR), 
PHONE_NUMBER   VARCHAR   AS (VALUE:c5::VARCHAR), 
HIRE_DATE      VARCHAR   AS TO_CHAR(TRY_TO_DATE(CAST(GET(VALUE, 'c6') AS 
VARCHAR(16777216)), 'YYYY-MM-DD'), 'DD-MON-YYYY'), 
JOB_ID         
VARCHAR   AS (VALUE:c7::VARCHAR), 
SALARY         
VARCHAR   AS (VALUE:c8::VARCHAR), 
COMMISSION_PCT NUMBER(38,2)   AS TRY_CAST(CAST(GET(VALUE, 'c9') AS VARCHAR(16777216)) AS NUMBER(38,2)), 
MANAGER_ID     VARCHAR   AS (VALUE:c10::VARCHAR), 
DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
) 
LOCATION=@AWS_ET_CSV_STAGE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = CSV_ET_FILEFORMAT; 
SELECT * FROM EXT_EMPLOYEES; 
SELECT * FROM EXT_EMPLOYEES_FORMAT_DATE_EMPLOYEE_ID_COMMISSION_PCT; 

-----------------------------
-- Re-create external table with no columns defined (so you can see VALUE and columns in there)
-----------------------------
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_NO_COLS 
WITH LOCATION=@AWS_ET_CSV_STAGE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = (FORMAT_NAME = CSV_ET_FILEFORMAT); 
SELECT * FROM EXT_EMPLOYEES_NO_COLS; 
SELECT  * FROM @AWS_ET_CSV_STAGE; 
SELECT 
T.$1, 
T.$2, 
T.$3, 
T.$4, 
T.$5, 
T.$6 , 
T.$7, 
T.$8, 
T.$9, 
T.$10, 
T.$11 
FROM @AWS_ET_CSV_STAGE T;