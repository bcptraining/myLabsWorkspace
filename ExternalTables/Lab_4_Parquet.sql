-- External Table LAB 4 

-- Context
USE DATABASE HRMS; 
USE SCHEMA HR; 
----------------------
-- Storage Integration
---------------------

Create storage integration udemy_mc_et_parquet_si
-- CREATE or replace STORAGE INTEGRATION udemy_mc_a1_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
  's3://vdw-dev-ingest/loadingdatalabs/json/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/et_parquet/');

  desc integration  udemy_mc_et_parquet_si;
-----------------
-- File Format
----------------
CREATE OR REPLACE FILE FORMAT HRMS.HR.PARQUET_ET_FILEFORMAT 
TYPE = PARQUET --FIELD_DELIMITER = ',' --SKIP_HEADER = 0 
COMPRESSION=AUTO; --COMPRESSION = AUTO | LZO | SNAPPY | NONE 

----------------
-- Stage
----------------
CREATE OR REPLACE STAGE HRMS.HR.AWS_ET_PARQUET_STAGE 
STORAGE_INTEGRATION = udemy_mc_et_parquet_si
URL =  's3://vdw-dev-ingest/loadingdatalabs/labcsv/parquet/' 
FILE_FORMAT = PARQUET_ET_FILEFORMAT; 

LIST @AWS_ET_PARQUET_STAGE; 


-------------------------
-- Generate partquet files of employee data using the external table from lab2 (uses csv files) 
-------------------------
-- This does not provide the real column names
-- COPY INTO @HRMS.HR.AWS_ET_PARQUET_STAGE 
-- FROM ( 
--     SELECT EMPLOYEE_ID, 
--     FIRST_NAME, 
--     LAST_NAME, 
--     EMAIL, 
--     PHONE_NUMBER, 
--     HIRE_DATE, 
--     JOB_ID, 
--     SALARY, 
--     COMMISSION_PCT, 
--     MANAGER_ID, 
--     DEPARTMENT_ID
--     FROM HRMS.HR.EXT_EMPLOYEES_REFRESH 
-- ) 
-- FILE_FORMAT = (TYPE = PARQUET) OVERWRITE = TRUE;
-- This does not work either... provides one unnamed column containing json string
-- COPY INTO @HRMS.HR.AWS_ET_PARQUET_STAGE
-- FROM (
--     SELECT
--         TO_VARIANT(
--             OBJECT_CONSTRUCT(
--                 'EMPLOYEE_ID', EMPLOYEE_ID,
--                 'FIRST_NAME', FIRST_NAME,
--                 'LAST_NAME', LAST_NAME,
--                 'EMAIL', EMAIL,
--                 'PHONE_NUMBER', PHONE_NUMBER,
--                 'HIRE_DATE', HIRE_DATE,
--                 'JOB_ID', JOB_ID,
--                 'SALARY', SALARY,
--                 'COMMISSION_PCT', COMMISSION_PCT,
--                 'MANAGER_ID', MANAGER_ID,
--                 'DEPARTMENT_ID', DEPARTMENT_ID
--             )
--         ) AS RECORD
--     FROM HRMS.HR.EXT_EMPLOYEES_REFRESH
-- )
-- FILE_FORMAT = (TYPE = PARQUET)
-- OVERWRITE = TRUE;

-- Next attempt is to materialize the SRC table and then umload from there
-- 1. Create a temp table
CREATE OR REPLACE TEMPORARY TABLE HRMS.HR.EMPLOYEES_PARQUET_SRC AS
SELECT
    EMPLOYEE_ID::VARCHAR     AS EMPLOYEE_ID,
    FIRST_NAME::VARCHAR      AS FIRST_NAME,
    LAST_NAME::VARCHAR       AS LAST_NAME,
    EMAIL::VARCHAR           AS EMAIL,
    PHONE_NUMBER::VARCHAR    AS PHONE_NUMBER,
    HIRE_DATE::VARCHAR       AS HIRE_DATE,
    JOB_ID::VARCHAR          AS JOB_ID,
    SALARY::VARCHAR          AS SALARY,
    COMMISSION_PCT::VARCHAR  AS COMMISSION_PCT,
    MANAGER_ID::VARCHAR      AS MANAGER_ID,
    DEPARTMENT_ID::VARCHAR   AS DEPARTMENT_ID
FROM HRMS.HR.EXT_EMPLOYEES_REFRESH;
--  2. Unload the table to parquet (this did not work either. Its a limitation with Snowflake Parquet writer)
COPY INTO @HRMS.HR.AWS_ET_PARQUET_STAGE
FROM HRMS.HR.EMPLOYEES_PARQUET_SRC
FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
OVERWRITE = TRUE;




-------------------------
-- Confirm the external table is querying the parquet files correctly
-------------------------

SELECT $1 from @AWS_ET_PARQUET_STAGE; 
SELECT *  from @AWS_ET_PARQUET_STAGE; 
SELECT 
$1:EMPLOYEE_ID, 
$1:FIRST_NAME, 
$1:LAST_NAME, 
$1:EMAIL, 
$1:PHONE_NUMBER, 
$1:HIRE_DATE, 
$1:JOB_ID, 
$1:SALARY, 
$1:COMMISSION_PCT, 
$1:MANAGER_ID, 
$1:DEPARTMENT_ID 
FROM @AWS_ET_PARQUET_STAGE; 
SELECT 
$1:EMPLOYEE_ID::NUMBER, 
$1:FIRST_NAME::VARCHAR, 
$1:LAST_NAME::VARCHAR, 
$1:EMAIL::VARCHAR, 
$1:PHONE_NUMBER::VARCHAR, 
$1:HIRE_DATE::VARCHAR, 
$1:JOB_ID::VARCHAR, 
$1:SALARY::VARCHAR, 
$1:COMMISSION_PCT::VARCHAR, 
$1:MANAGER_ID::VARCHAR, 
$1:DEPARTMENT_ID::VARCHAR 
FROM @AWS_ET_PARQUET_STAGE; 
-- If the parquet had the actual column names then create the external table like this:
-- CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_PARQUET 
-- ( 
-- EMPLOYEE_ID    VARCHAR   AS ($1:EMPLOYEE_ID::VARCHAR), 
-- FIRST_NAME     VARCHAR   AS ($1:FIRST_NAME::VARCHAR), 
-- LAST_NAME      VARCHAR   AS ($1:LAST_NAME::VARCHAR), 
-- EMAIL          
-- VARCHAR   AS ($1:EMAIL::VARCHAR), 
-- PHONE_NUMBER   VARCHAR   AS ($1:PHONE_NUMBER::VARCHAR), 
-- HIRE_DATE      VARCHAR   AS ($1:HIRE_DATE::VARCHAR), 
-- JOB_ID         
-- VARCHAR   AS ($1:JOB_ID::VARCHAR), 
-- SALARY         
-- VARCHAR   AS ($1:SALARY::VARCHAR), 
-- COMMISSION_PCT VARCHAR   AS ($1:COMMISSION_PCT::VARCHAR), 
-- MANAGER_ID     VARCHAR   AS ($1:MANAGER_ID::VARCHAR), 
-- DEPARTMENT_ID  VARCHAR   AS ($1:DEPARTMENT_ID::VARCHAR) 
-- ) 
-- LOCATION=@AWS_ET_PARQUET_STAGE 
-- PATTERN='.*employees.*[.]parquet' 
-- FILE_FORMAT = PARQUET_ET_FILEFORMAT; 

-- Since the Snowflake parquet writer had a limitaiton regarding column names, I used this workaround: 
CREATE OR REPLACE EXTERNAL TABLE HRMS.HR.EXT_EMPLOYEES_PARQUET
(
  EMPLOYEE_ID    VARCHAR   AS ($1:_COL_0::VARCHAR),
  FIRST_NAME     VARCHAR   AS ($1:_COL_1::VARCHAR),
  LAST_NAME      VARCHAR   AS ($1:_COL_2::VARCHAR),
  EMAIL          VARCHAR   AS ($1:_COL_3::VARCHAR),
  PHONE_NUMBER   VARCHAR   AS ($1:_COL_4::VARCHAR),
  HIRE_DATE      VARCHAR   AS ($1:_COL_5::VARCHAR),
  JOB_ID         VARCHAR   AS ($1:_COL_6::VARCHAR),
  SALARY         VARCHAR   AS ($1:_COL_7::VARCHAR),
  COMMISSION_PCT VARCHAR   AS ($1:_COL_8::VARCHAR),
  MANAGER_ID     VARCHAR   AS ($1:_COL_9::VARCHAR),
  DEPARTMENT_ID  VARCHAR   AS ($1:_COL_10::VARCHAR)
)
LOCATION=@HRMS.HR.AWS_ET_PARQUET_STAGE
FILE_FORMAT=(TYPE=PARQUET);


SELECT METADATA$FILENAME,METADATA$FILE_ROW_NUMBER, * FROM EXT_EMPLOYEES_PARQUET; 
SELECT  DISTINCT  METADATA$FILENAME FROM EXT_EMPLOYEES_PARQUET; 
