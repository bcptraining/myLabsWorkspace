-----------------------------
-- External Table LAB 3 
-----------------------------
--   Context
USE DATABASE HRMS; 
USE SCHEMA HR; 

---------------------
-- File Format
---------------------
-- CREATE OR REPLACE FILE FORMAT HRMS.HR.CSV_ET_FILEFORMAT 
-- TYPE = CSV 
-- FIELD_DELIMITER = ',' 
-- SKIP_HEADER = 0 
-- NULL_IF = ('NULL','Null','null') 
-- FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
-- EMPTY_FIELD_AS_NULL = TRUE; 

----------------------
-- Storage Integration
---------------------

Create storage integration udemy_mc_et_partition_si
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
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/et_partition/');


ALTER STORAGE INTEGRATION udemy_mc_et_partition_si
SET STORAGE_ALLOWED_LOCATIONS = (
    's3://vdw-dev-ingest/loadingdatalabs/',
    's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
    's3://vdw-dev-ingest/loadingdatalabs/json/',
    's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
    's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/',
    's3://vdw-dev-ingest/loadingdatalabs/labcsv/et_partition/'
);

 
DESC INTEGRATION udemy_mc_et_partition_si;


---------------------
-- Stage
---------------------
CREATE OR REPLACE STAGE HRMS.HR.AWS_ET_PARTITIONED_CSV_STAGE  
STORAGE_INTEGRATION = udemy_mc_et_partition_si
-- URL = 's3://learn2cloud-snowflake/external_table/csv/' 
URL = 's3://vdw-dev-ingest/loadingdatalabs/labcsv/et_partition/'
FILE_FORMAT = CSV_ET_FILEFORMAT; 
LIST @AWS_ET_PARTITIONED_CSV_STAGE ; 

-- CREATE OR REPLACE STAGE HRMS.HR.AWS_ET_PARTITIONED_CSV_STAGE 
-- STORAGE_INTEGRATION = AWS_S3_INT 
-- URL = 's3://learn2cloud-snowflake/external_table/et_partition/' 
-- FILE_FORMAT = CSV_ET_FILEFORMAT ;
LIST @AWS_ET_PARTITIONED_CSV_STAGE; 

----------------------
-- External Table
----------------------
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_PARTITIONED 
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
--DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
DEPARTMENT_ID  varchar AS  split_part(metadata$filename, '/', 4)  -- Get the DEPARTMENT_ID from the s3 partition (folder)
) 
LOCATION=@AWS_ET_PARTITIONED_CSV_STAGE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = CSV_ET_FILEFORMAT; 


SELECT * FROM EXT_EMPLOYEES_PARTITIONED; 
SELECT  metadata$filename FROM EXT_EMPLOYEES_PARTITIONED; 
SELECT metadata$filename,split_part(metadata$filename, '/', 4) FROM 
EXT_EMPLOYEES_PARTITIONED; 
SELECT * FROM EXT_EMPLOYEES_PARTITIONED 
-- WHERE DEPARTMENT_ID=20 