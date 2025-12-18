-- External Table LAB 5 

-- Set context
USE DATABASE HRMS; 
USE SCHEMA HR; 


-----------------------------
--  Storage Integration (re-used this from an earlier lab) -- 's3://vdw-dev-ingest/loadingdatalabs/json/'
-----------------------------
-- CREATE or replace STORAGE INTEGRATION udemy_mc_a1_si
-- TYPE = EXTERNAL_STAGE
-- STORAGE_PROVIDER = S3
-- ENABLED = TRUE
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
--   's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
--   's3://vdw-dev-ingest/loadingdatalabs/json/',
--   's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
--   's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/');

  desc integration udemy_mc_a1_si;
----------------------------
--  File Format
----------------------------
CREATE OR REPLACE FILE FORMAT HRMS.HR.JSON_ET_FILEFORMAT 
TYPE = JSON; 

----------------------------
--  Stage
----------------------------
CREATE OR REPLACE STAGE HRMS.HR.AWS_ET_JSON_STAGE 
STORAGE_INTEGRATION = udemy_mc_a1_si
-- URL = 's3://learn2cloud-snowflake/external_table/JSON/' 
URL = 's3://vdw-dev-ingest/loadingdatalabs/json/'
FILE_FORMAT = JSON_ET_FILEFORMAT; 
LIST @AWS_ET_JSON_STAGE; 
select * from @AWS_ET_JSON_STAGE;
select $1 from @AWS_ET_JSON_STAGE; -- For json this $1 is column 1 is same as select *

----------------------------
--  Unload some data as json into the s3 location (since no provided vor the lab)
----------------------------
-- COPY INTO @HRMS.HR.AWS_ET_JSON_STAGE
-- FROM (
--     SELECT OBJECT_CONSTRUCT(
--         'EMPLOYEE_ID', EMPLOYEE_ID,
--         'FIRST_NAME', FIRST_NAME,
--         'LAST_NAME', LAST_NAME,
--         'EMAIL', EMAIL,
--         'PHONE_NUMBER', PHONE_NUMBER,
--         'HIRE_DATE', HIRE_DATE,
--         'JOB_ID', JOB_ID,
--         'SALARY', SALARY,
--         'COMMISSION_PCT', COMMISSION_PCT,
--         'MANAGER_ID', MANAGER_ID,
--         'DEPARTMENT_ID', DEPARTMENT_ID
--     )
--     FROM HRMS.HR.EXT_EMPLOYEES_REFRESH
-- )
-- FILE_FORMAT = (TYPE = JSON)
-- OVERWRITE = TRUE;




SELECT * FROM  @AWS_ET_JSON_STAGE; 
SELECT $1 FROM  @AWS_ET_JSON_STAGE; 

-------------------------
-- Create external table over json files
------------------------
CREATE OR REPLACE EXTERNAL TABLE EXT_EMP_JSON  -- 1 row with both VALUE and EMP_JSON_DATA columns that have the json array of employees
( 
EMP_JSON_DATA VARIANT AS (VALUE::VARIANT) 
) 
WITH LOCATION = @AWS_ET_JSON_STAGE 
FILE_FORMAT = JSON_ET_FILEFORMAT; 

SELECT * FROM EXT_EMP_JSON;  -- 1 row with VALUE and EMP_JSON_DATA having same json with all employees
SELECT * FROM EXT_EMP_JSON EDJ, -- 1 row per employee plus PATH and INDEX and a new (additional) VALUE column that has the 1 employee json
lateral flatten ( input =>EMP_JSON_DATA) Y;



-- External table to pull the first employee only (using  $1[0])
CREATE OR REPLACE EXTERNAL TABLE EXT_EMP_JSON_1 
( 
EMP_JSON_DATA VARIANT AS (VALUE::VARIANT), 
EMP_ID VARCHAR AS ($1[0].employee_id::VARCHAR) -- $1[0] Specifies to pull only the first employee record
) 
WITH LOCATION = @AWS_ET_JSON_STAGE 
FILE_FORMAT = JSON_ET_FILEFORMAT; 

SELECT  * FROM EXT_EMP_JSON_1; 
-- V2 external table pulls out the first 2 EMP_IDs (stupid approach... to demonstrate need for lateral flatten)
CREATE OR REPLACE EXTERNAL TABLE EXT_EMP_JSON_2 
( 
EMP_JSON_DATA VARIANT AS (VALUE::VARIANT), 
EMP_ID VARCHAR AS ($1[0].employee_id::VARCHAR), 
EMP_ID2 VARCHAR AS ($1[1].employee_id::VARCHAR) 
) 
WITH LOCATION = @AWS_ET_JSON_STAGE 
FILE_FORMAT = JSON_ET_FILEFORMAT; 

SELECT  * FROM EXT_EMP_JSON_2; 

SELECT  DISTINCT  METADATA$FILENAME FROM EXT_EMP_JSON;  -- My example only had 1 file and it differed from the lab schema

-- lateral flatten query get you a 1 row per employee with PATH [n] and INDEX n and VALUE with json for the 1 employee 
SELECT * FROM EXT_EMP_JSON EDJ, 
lateral flatten( input =>EMP_JSON_DATA) Y; 
-- This flattens and provides some top-level elements
SELECT  -- This flattens and provides top-level elements... but it has the double-quotes everywhere
Y.VALUE:employee_id, 
Y.VALUE:employee_name, 
Y.VALUE:phone_numbers,
Y.VALUE: address,
Y.VALUE: position,
Y.VALUE: skills
FROM EXT_EMP_JSON EDJ, 
lateral flatten( input =>EMP_JSON_DATA) Y; 

-- Cast the data to get rid of the double quotes and provides column names
SELECT  
Y.VALUE:employee_id:: INTEGER AS EMP_ID, 
Y.VALUE:employee_name: VARCHAR AS EMP_NAME, 
Y.VALUE:phone_numbers AS PHONE,
Y.VALUE: address AS ADDRESS,
Y.VALUE: position:: VARCHAR AS POSITION,
Y.VALUE: skills AS SKILLS,
FROM EXT_EMP_JSON EDJ, 
lateral flatten( input =>EMP_JSON_DATA) Y; 

-- Can query directly from the stage
SELECT  *  FROM @AWS_ET_JSON_STAGE; -- 1 row with all employees in the file
SELECT  *  FROM @AWS_ET_JSON_STAGE,  --  1 row per employee
lateral flatten( input => parse_json($1)) ;

-- QUERY_AGG examples
SELECT  -- 2 rows (Seattle, and SOUTHLAKE) each having the employees from the respective city
    Y.VALUE:address.city::STRING AS city,
    ARRAY_AGG(Y.VALUE) AS employees
FROM EXT_EMP_JSON E,
     LATERAL FLATTEN(input => EMP_JSON_DATA) Y
GROUP BY city;



----------------------
-- Other stuff I didnt use
----------------------

-- The queries below are from the lab but do not match my data so did not use them (adapted above) 
SELECT 
Y.VALUE:EMPLOYEE_ID:: INTEGER, 
Y.VALUE:FIRST_NAME :: VARCHAR, 
Y.VALUE:LAST_NAME :: VARCHAR, 
Y.VALUE:EMAIL:: VARCHAR, 
Y.VALUE:PHONE_NUMBER:: VARCHAR , 
Y.VALUE:HIRE_DATE:: VARCHAR, 
Y.VALUE:JOB_ID:: VARCHAR, 
Y.VALUE:SALARY:: VARCHAR, 
Y.VALUE:COMMISSION_PCT:: VARCHAR, 
Y.VALUE:MANAGER_ID:: VARCHAR, 
Y.VALUE:DEPARTMENT_ID:: VARCHAR 
FROM EXT_EMP_JSON EDJ, 
lateral flatten( input =>EMP_JSON_DATA) Y; 

CREATE OR REPLACE VIEW VIEW_EXT_EMP_JSON 
AS 
SELECT 
Y.VALUE:EMPLOYEE_ID:: INTEGER       
Y.VALUE:FIRST_NAME :: VARCHAR       
Y.VALUE:LAST_NAME :: VARCHAR        
Y.VALUE:EMAIL:: VARCHAR             
AS EMPLOYEE_ID, 
AS FIRST_NAME , 
AS LAST_NAME, 
AS EMAIL, 
Y.VALUE:PHONE_NUMBER:: VARCHAR      AS PHONE_NUMBER, 
Y.VALUE:HIRE_DATE:: VARCHAR         
AS HIRE_DATE, 
Y.VALUE:JOB_ID:: VARCHAR            
Y.VALUE:SALARY:: VARCHAR            
AS JOB_ID, 
AS SALARY, 
Y.VALUE:COMMISSION_PCT:: VARCHAR    AS COMMISSION_PCT, 
Y.VALUE:MANAGER_ID:: VARCHAR        
AS MANAGER_ID, 
Y.VALUE:DEPARTMENT_ID:: VARCHAR     AS DEPARTMENT_ID 
FROM EXT_EMP_JSON EDJ, 
lateral flatten( input =>EMP_JSON_DATA) Y; 

SELECT * FROM VIEW_EXT_EMP_JSON; 
SELECT * FROM VIEW_EXT_EMP_DEPT_JSON; 
SELECT  *  FROM @AWS_ET_JSON_STAGE, 
lateral flatten( input => parse_json($1)) 
SELECT  $1 FROM @AWS_ET_JSON_STAGE;

SELECT  $1 FROM @AWS_ET_JSON_STAGE; 
SELECT  DISTINCT  METADATA$FILENAME FROM EXT_EMPLOYEES; 
SELECT * FROM EXT_EMPLOYEES_JSON; 
SELECT 
ARRAY_AGG(OBJECT_CONSTRUCT('DEPARTMENT_ID',D.DEPARTMENT_ID,'EMPLOYEE_ 
ID',E.EMPLOYEE_ID,'EMPLOYEE_NAME',E.FIRST_NAME)) 
FROM DEPARTMENTS D,EMPLOYEES E 
WHERE D.DEPARTMENT_ID=E.DEPARTMENT_ID 
SELECT ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(*)) 
FROM EMPLOYEES 
group by DEPARTMENT_ID; 
SELECT COUNT(*),DEPARTMENT_ID FROM EMPLOYEES 
GROUP BY DEPARTMENT_ID; 
SELECT  *  FROM @AWS_ET_JSON_STAGE, 
lateral flatten( input => parse_json($1)); 
{ 
"COMMISSION_PCT": null, 
"DEPARTMENT_ID": 100, 
"EMAIL": "NGREENBE", 
"EMPLOYEE_ID": 108, 
"FIRST_NAME": "NANCY", 
"HIRE_DATE": "2002-08-17", 
"JOB_ID": "FI_MGR", 
"LAST_NAME": "GREENBERG", 
"MANAGER_ID": 101, 
"PHONE_NUMBER": "515.124.4569", 
"SALARY": 12008 
}, 
SELECT $1[0].EMPLOYEE_ID FROM  @AWS_ET_JSON_STAGE 
SELECT $1[0].EMPLOYEE_ID FROM  @AWS_ET_JSON_STAGE 
CREATE OR REPLACE EXTERNAL TABLE EXT_EMP_DEPT_JSON 
( 
EMP_DEPT_JSON_DATA VARIANT AS (VALUE::VARIANT), 
EMP_ID VARCHAR AS ($1[0].EMPLOYEE_ID::VARCHAR) 
) 
WITH LOCATION = @AWS_ET_JSON_STAGE 
FILE_FORMAT = JSON_ET_FILEFORMAT; 