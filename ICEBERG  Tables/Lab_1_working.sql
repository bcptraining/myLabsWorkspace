------------------------------------------
-- Context
------------------------------------------

CREATE OR REPLACE DATABASE ICEBERG;
CREATE OR REPLACE SCHEMA HR;

------------------------------------------
-- Storage Integration (and stage to test SI)
------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION iceberg_unmanaged_si
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-iceberg/employees_iceberg/','s3://vdw-dev-iceberg/employees/');

desc integration iceberg_unmanaged_si;

ALTER STORAGE INTEGRATION iceberg_unmanaged_si
SET STORAGE_ALLOWED_LOCATIONS = (
    's3://vdw-dev-iceberg/employees_iceberg/',
    's3://vdw-dev-iceberg/snowflake_employees_iceberg/',
    's3://vdw-dev-iceberg/employees/'
);

-- Create a stage to confirm the si is working
CREATE OR REPLACE STAGE ICEBERG.HR.employees_parquet_stage
  URL = 's3://vdw-dev-iceberg/employees/'
  STORAGE_INTEGRATION = iceberg_unmanaged_si
  FILE_FORMAT = (TYPE = PARQUET);
  -- DIRECTORY = (ENABLE = TRUE);
ALTER STAGE ICEBERG.HR.employees_parquet_stage SET DIRECTORY = (ENABLE = TRUE);
ALTER STAGE ICEBERG.HR.employees_parquet_stage refresh;
list @ICEBERG.HR.employees_parquet_stage;


------------------------------------------
-- 1. ICEBERG External Volume (url to the folder containing the data and metadata folders for the unmanaged iceberg table, and role to assume when accessing it)
------------------------------------------

CREATE EXTERNAL VOLUME ICEBERG_VOLUME_EMPLOYEES_2
  STORAGE_LOCATIONS = (
    (
      NAME = 'HR DATA 2',
      STORAGE_PROVIDER = 's3',
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si',
      STORAGE_BASE_URL = 's3://vdw-dev-iceberg/employees_iceberg/'  -- <-- This is the folder that contains the data and metadata folders
    )
  );
GRANT USAGE ON EXTERNAL VOLUME ICEBERG_VOLUME_EMPLOYEES_2 TO ROLE ACCOUNTADMIN;
-- Add the External Volume's ExternalID to the trust policy (keep the ExternalId of the storage integration also)
DESC EXTERNAL VOLUME ICEBERG_VOLUME_EMPLOYEES_2;
-- {"NAME":"HR DATA 2","STORAGE_PROVIDER":"S3","STORAGE_BASE_URL":"s3://vdw-dev-iceberg/employees_iceberg/","STORAGE_ALLOWED_LOCATIONS":["s3://vdw-dev-iceberg/employees_iceberg/*"],"STORAGE_REGION":"us-west-2","PRIVILEGES_VERIFIED":true,"STORAGE_AWS_ROLE_ARN":"arn:aws:iam::904233092605:role/iceberg_unmanaged_si","STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::935542360084:user/6nrd1000-s","STORAGE_AWS_EXTERNAL_ID":"EYB38761_SFCRole=3_TLJAHOSUZvTEhPq1FAyz9lyk6D0=","ENCRYPTION_TYPE":"NONE","ENCRYPTION_KMS_KEY_ID":""}

-- "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": [
--                         "EYB38761_SFCRole=3_Eorb/6bhHX6xhHAORaKJ95d6wXI=", <-- Storage Integration
--                         "EYB38761_SFCRole=3_TLJAHOSUZvTEhPq1FAyz9lyk6D0=", <-- External Volume (ICEBERG_VOLUME_EMPLOYEES_2)
--                         "EYB38761_SFCRole=3_hRhPLXzyO9eFQUk5tR5J6ozTvi4="  <-- CATALOG INTEGRATION (CAT_INT_GLUE)
--                     ]
--                 }
--             }

use role accountadmin;


SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ICEBERG_VOLUME_EMPLOYEES_2'); -- This works
-- {"success":true,"storageLocationSelectionResult":"PASSED","storageLocationName":"HR DATA 2","servicePrincipalProperties":"STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::935542360084:user/6nrd1000-s; STORAGE_AWS_EXTERNAL_ID: EYB38761_SFCRole=3_TLJAHOSUZvTEhPq1FAyz9lyk6D0=","location":"s3://vdw-dev-iceberg/employees_iceberg/","storageAccount":null,"region":"us-west-2","writeResult":"PASSED","readResult":"PASSED","listResult":"PASSED","deleteResult":"PASSED","awsRoleArnValidationResult":"PASSED","azureGetUserDelegationKeyResult":"SKIPPED"}


------------------------------------------
-- 2. ICEBERG Catalog Integration (plumbing)
------------------------------------------

CREATE OR REPLACE CATALOG INTEGRATION CAT_INT_GLUE
CATALOG_SOURCE = GLUE
CATALOG_NAMESPACE ='hrms' -- This refers to glue database name
TABLE_FORMAT = ICEBERG
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si'
GLUE_CATALOG_ID='904233092605'  -- USE THE 12 DIGIT AWS ACCOUNT NUMBER OF THE GLUE CATALOG YOU WANT 
GLUE_REGION='us-west-2'
ENABLED=TRUE;

SHOW CATALOG INTEGRATIONS;  -- Note: "SHOW CATALOG INTEGRATIONS" different than "SHOW INTEGRATIONS" 
DESCRIBE INTEGRATION CAT_INT_GLUE; -- Note: And yet we can use "DESCRIBE INTEGRATION"
-- GLUE_AWS_EXTERNAL_ID  = EYB38761_SFCRole=3_hRhPLXzyO9eFQUk5tR5J6ozTvi4=    -- <-- Add this to the trust policy                      
-- GLUE_AWS_IAM_USER_ARN = arn:aws:iam::935542360084:user/6nrd1000-s

------------------------------
-- Create an iceberg external table  (uses GLUE Catalog)
------------------------------

CREATE OR REPLACE ICEBERG TABLE hr.aws_athena_employees_iceberg_ext -- successfully created
  CATALOG = CAT_INT_GLUE
  EXTERNAL_VOLUME = ICEBERG_VOLUME_EMPLOYEES_2
  CATALOG_TABLE_NAME = 'aws_athena_employees_iceberg';

------------------------------
-- Cannot update rows in external (GLUE) catalog as they are read-only. Ypou can update from AWS only.
------------------------------

select * from hr.aws_athena_employees_iceberg_ext
WHERE EMPLOYEE_ID =120; 

UPDATE hr.aws_athena_employees_iceberg_ext -- SQL Compilation error: Iceberg table AWS_ATHENA_EMPLOYEES_ICEBERG_EXT with an external catalog integration is a read-only table and cannot be modifie
SET FIRST_NAME ='TEST'
WHERE EMPLOYEE_ID =120;

------------------------------
-- If ICEBERG table is updated externally... say 2 rows were added in AWS  
------------------------------
SELECT COUNT(*) FROM hr.aws_athena_employees_iceberg_ext; -- 61 ROWS RETURNED
-- 2 new rows added to the ICEBERG table in AWS
SELECT COUNT(*) FROM hr.aws_athena_employees_iceberg_ext; -- 61 ROWS RETURNED
ALTER ICEBERG TABLE  hr.aws_athena_employees_iceberg_ext REFRESH;
SELECT COUNT(*) FROM hr.aws_athena_employees_iceberg_ext; -- 63 ROWS RETURNED


------------------------------
-- Pivot to managed ICEBERG (This is Lab 3)
------------------------------
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_VOLUME_MANAGED -- ICEBERG_VOLUME_MANAGED successfully created.
STORAGE_LOCATIONS =
(
    (
        NAME = 'HR Data SF',
        STORAGE_PROVIDER = 's3',
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si',
        STORAGE_BASE_URL = 's3://vdw-dev-iceberg/snowflake_employees_iceberg/'
    )
);

desc external volume ICEBERG_VOLUME_MANAGED; -- You must add this new ExternalID "EYB38761_SFCRole=3_MstaeF1orYlht0NpBC2QBajJ034=" to the trust policy in AWS
-- {"NAME":"HR Data SF","STORAGE_PROVIDER":"S3","STORAGE_BASE_URL":"s3://vdw-dev-iceberg/snowflake_employees_iceberg/","STORAGE_ALLOWED_LOCATIONS":["s3://vdw-dev-iceberg/snowflake_employees_iceberg/*"],"STORAGE_REGION":"us-west-2","STORAGE_AWS_ROLE_ARN":"arn:aws:iam::904233092605:role/iceberg_unmanaged_si","STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::935542360084:user/6nrd1000-s","STORAGE_AWS_EXTERNAL_ID":"EYB38761_SFCRole=3_MstaeF1orYlht0NpBC2QBajJ034=","ENCRYPTION_TYPE":"NONE","ENCRYPTION_KMS_KEY_ID":""}
-- ------------------------------------------------------------

-- ALTER ICEBERG TABLE ICEBERG.HR.IB_EMPLOYEES CONVERT TO MANAGED
-- BASE_LOCATION ='ib_employees-managed-table';


-- CREATE OR REPLACE CATALOG INTEGRATION CAT_INT_SNOWFLAKE -- Integration CAT_INT_SNOWFLAKE successfully created.
-- CATALOG_SOURCE = OBJECT_STORE
-- TABLE_FORMAT = ICEBERG
-- ENABLED=TRUE;


-- SHOW TABLES;


-- SELECT MIN(FIRST_NAME),MAX(FIRST_NAME),MIN(LAST_NAME),MAX(LAST_NAME), MIN(EMAIL), MAX(EMAIL),
-- MIN(PHONE_NUMBER), MAX(PHONE_NUMBER),
-- MIN(HIRE_DATE), MAX(HIRE_DATE)
-- FROM EMPLOYEES
-- WHERE EMPLOYEE_ID IN (118,107,119,113)

CREATE OR REPLACE ICEBERG TABLE snowflake_employees_iceberg ( -- Table SNOWFLAKE_EMPLOYEES_ICEBERG successfully created.
  EMPLOYEE_ID    STRING,
  FIRST_NAME     STRING,
  LAST_NAME      STRING ,
  EMAIL          STRING,
  PHONE_NUMBER   STRING,
  HIRE_DATE      STRING ,
  JOB_ID         STRING ,
  SALARY         STRING,
  COMMISSION_PCT STRING,
  MANAGER_ID     STRING,
  DEPARTMENT_ID  STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME ='ICEBERG_VOLUME_MANAGED'
BASE_LOCATION = 'snowflake_employees_iceberg';
-- Identify mis-alignment in the source and target DDL (FIRSTNAME vs FIRST_NAME)
DESC TABLE snowflake_employees_iceberg;
DESC TABLE hr.aws_athena_employees_iceberg_ext;
-- Insert externally managed table data into managed table (Transform FIRSTNAME --> FIRST_NAME)   
INSERT INTO snowflake_employees_iceberg (
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE_NUMBER,
    HIRE_DATE,
    SALARY,
    COMMISSION_PCT,
    MANAGER_ID,
    DEPARTMENT_ID,
    JOB_ID
)
SELECT
    EMPLOYEE_ID,
    FIRSTNAME AS FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE_NUMBER,
    HIRE_DATE,
    SALARY,
    COMMISSION_PCT,
    MANAGER_ID,
    DEPARTMENT_ID,
    NULL AS JOB_ID
FROM hr.aws_athena_employees_iceberg_ext;

-- Update the JOB_ID for one employee 
UPDATE snowflake_employees_iceberg 
SET JOB_ID ='AD_PRES'
WHERE EMPLOYEE_ID =120;
-- Confirm JOB_ID is populated for the employee
select top 10 * from snowflake_employees_iceberg;


-- IRREVERSIBLE OPERATION:  You can alter an unmanaged ICEBERG table to be a managed ICEBERG table  
ALTER ICEBERG TABLE ICEBERG.HR.aws_athena_employees_iceberg_ext
CONVERT TO MANAGED 
BASE_LOCATION = '<your location>';