-- -– External Table Lab 6 
USE DATABASE HRMS; 
USE SCHEMA HR; 

----------------------
-- File format
----------------------
CREATE OR REPLACE FILE FORMAT HRMS.HR.CSV_ET_FILEFORMAT
TYPE = CSV 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 0 
NULL_IF = ('NULL','Null','null') 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
EMPTY_FIELD_AS_NULL = TRUE; 

describe file format HRMS.HR.CSV_ET_FILEFORMAT ;

----------------------
-- Storage Integration
----------------------
CREATE or replace STORAGE INTEGRATION udemy_mc_a1_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/',  -- already covers /labcsv/streams/
  's3://vdw-dev-ingest/loadingdatalabs/json/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/');

  desc integration udemy_mc_a1_si; -- Original: YJB79755_SFCRole=3_ppKckAbuWSWBkwPlylG3vKykvSs=
 
  SHOW STORAGE INTEGRATIONS LIKE 'UDEMY_MC_ET_PARQUET_SI';
    SHOW STORAGE INTEGRATIONS ;



----------------------
-- Stage
----------------------
CREATE OR REPLACE STAGE HRMS.HR.AWS_ET_CSV_STREAM_STAGE 
-- STORAGE_INTEGRATION = AWS_S3_INT 
STORAGE_INTEGRATION = udemy_mc_a1_si
-- URL = 's3://learn2cloud-snowflake/external_table/streams/' 
URL = 's3://vdw-dev-ingest/loadingdatalabs/labcsv/streams'
DIRECTORY = (ENABLE = TRUE)
FILE_FORMAT = CSV_ET_FILEFORMAT
; 
ALTER STAGE AWS_ET_CSV_STREAM_STAGE REFRESH;

DESC STAGE AWS_ET_CSV_STREAM_STAGE;

--------------------------
-- Queries to check stage and staged data
--------------------------
-- What files are in the stage?
LIST @AWS_ET_CSV_STREAM_STAGE; 
-- Select metadata and data from the stage
SELECT metadata$filename, $1, $2, $3, $4
FROM @AWS_ET_CSV_STREAM_STAGE;

 -- Note: This stage query uses $1, $2, $3 = CSV column positions
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
FROM @AWS_ET_CSV_STREAM_STAGE (PATTERN => '.*employee.*\.csv') T;

SELECT $1, $2, $3 FROM @AWS_ET_CSV_STREAM_STAGE (FILE_FORMAT => CSV_ET_FILEFORMAT) WHERE METADATA$FILENAME LIKE '%employees_Lab2_308.csv';

----------------------
-- External Table
----------------------

--  This external table definition works by using VALUE:c1, VALUE:c2 = JSON/VARIANT field extraction (rather than csv columns)
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_STREAMS
(
    EMPLOYEE_ID    VARCHAR   AS (VALUE:c1::VARCHAR),  -- CAST(GET(VALUE, 'c1') AS VARCHAR)
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
LOCATION=@AWS_ET_CSV_STREAM_STAGE
FILE_FORMAT = HRMS.HR.CSV_ET_FILEFORMAT
AUTO_REFRESH = true
PATTERN='.*employees_.*[.]csv';
-- This parses as CSV and it did not work
-- CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_STREAMS2 
-- ( 
--   EMPLOYEE_ID      VARCHAR   AS ($1::VARCHAR),   
--   FIRST_NAME       VARCHAR   AS ($2::VARCHAR),
--   LAST_NAME        VARCHAR   AS ($3::VARCHAR),
--   EMAIL            VARCHAR   AS ($4::VARCHAR),
--   PHONE_NUMBER     VARCHAR   AS ($5::VARCHAR),
--   HIRE_DATE        VARCHAR   AS ($6::VARCHAR),
--   JOB_ID           VARCHAR   AS ($7::VARCHAR),
--   SALARY           VARCHAR   AS ($8::VARCHAR),
--   COMMISSION_PCT   VARCHAR   AS ($9::VARCHAR),
--   MANAGER_ID       VARCHAR   AS ($10::VARCHAR),
--   DEPARTMENT_ID    VARCHAR   AS ($11::VARCHAR)
-- )
-- LOCATION=@AWS_ET_CSV_STREAM_STAGE
-- PATTERN='.*employees.*[.]csv'
-- FILE_FORMAT = (FORMAT_NAME = CSV_ET_FILEFORMAT2);

DROP EXTERNAL TABLE EXT_EMPLOYEES_STREAMS;
-- DROP EXTERNAL TABLE EXT_EMPLOYEES_STREAMS2;

ALTER EXTERNAL TABLE EXT_EMPLOYEES_STREAMS REFRESH;
SHOW COLUMNS IN TABLE EXT_EMPLOYEES_STREAMS;

--------------------------
-- Query External Table
--------------------------
SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME FROM EXT_EMPLOYEES_STREAMS; 
-- where EMPLOYEE_ID > 997
-- LIMIT 6;
-- What files are available vie the external table?
SELECT *
FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(
    TABLE_NAME => 'EXT_EMPLOYEES_STREAMS'
));


SELECT 
  METADATA$FILENAME,
  METADATA$FILE_ROW_NUMBER,
  VALUE,
  TYPEOF(VALUE), -- OBJECT
  $2,
  -- TYPEOF($2),
  ARRAY_SIZE(SPLIT($1::STRING, ',')) AS field_count
FROM EXT_EMPLOYEES_STREAMS
LIMIT 5;


--------------------------
-- Query the external table
--------------------------
-- ALTER EXTERNAL TABLE EXT_EMPLOYEES_STREAMS REFRESH;
ALTER EXTERNAL TABLE EXT_EMPLOYEES_STREAMS_CLEAN REFRESH;
SELECT * FROM EXT_EMPLOYEES_STREAMS;
-- SELECT * FROM EXT_EMPLOYEES_STREAMS;

SELECT VALUE FROM EXT_EMPLOYEES_STREAMS2 LIMIT 1;
-------------------------
-- Create stream
-------------------------

CREATE OR REPLACE STREAM STREAM_EXT_EMPLOYEES ON EXTERNAL TABLE 
EXT_EMPLOYEES_STREAMS 
INSERT_ONLY = TRUE;  -- <-- mandatory for external tables
-------------------------
-- Query Stream
-------------------------
SELECT * FROM STREAM_EXT_EMPLOYEES; 
-- Manual file registration (if needed) 
ALTER EXTERNAL TABLE EXT_EMPLOYEES_STREAMS REFRESH; 
