------------------------------------------
-- Context
------------------------------------------

CREATE OR REPLACE DATABASE ICEBERG;
CREATE OR REPLACE SCHEMA HR;

------------------------------------------
-- Storage Integration
------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION iceberg_unmanaged_si
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-iceberg/employees_iceberg/','s3://vdw-dev-iceberg/employees/');

ALTER STORAGE INTEGRATION iceberg_unmanaged_si
SET STORAGE_ALLOWED_LOCATIONS = (
    's3://vdw-dev-iceberg/employees_iceberg/',
    's3://vdw-dev-iceberg/snowflake_employees_iceberg/',
    's3://vdw-dev-iceberg/employees/'
);
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ICEBERG_VOLUME_UNMANAGED'); -- This works
SELECT SYSTEM$GET_EXTERNAL_VOLUME_INTEGRATION_INFO('ICEBERG_VOLUME_UNMANAGED'); -- SQL compilation error: Unknown function SYSTEM$GET_EXTERNAL_VOLUME_INTEGRATION_INFO
desc integration iceberg_unmanaged_si;
-- STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::935542360084:user/6nrd1000-s
-- STORAGE_AWS_EXTERNAL_ID = EYB38761_SFCRole=3_Eorb/6bhHX6xhHAORaKJ95d6wXI=

-- Create a stage to confirm the si is working
CREATE OR REPLACE STAGE ICEBERG.HR.employees_parquet_stage
  URL = 's3://vdw-dev-iceberg/employees/'
  STORAGE_INTEGRATION = iceberg_unmanaged_si
  FILE_FORMAT = (TYPE = PARQUET);
ALTER STAGE ICEBERG.HR.employees_parquet_stage SET DIRECTORY = (ENABLE = TRUE);
ALTER STAGE ICEBERG.HR.employees_parquet_stage refresh;



------------------------------------------
-- ICEBERG objects
------------------------------------------
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_VOLUME_UNMANAGED
STORAGE_LOCATIONS =
(
    (
       NAME = 'HR DATA',
       STORAGE_PROVIDER = 's3',
       STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si',
       STORAGE_BASE_URL = 's3://vdw-dev-iceberg/snowflake_employees_iceberg/'
    )
);

DESC EXTERNAL VOLUME ICEBERG_VOLUME_UNMANAGED;
-- STORAGE_AWS_IAM_USER_ARN = "arn:aws:iam::935542360084:user/6nrd1000-s"
-- STORAGE_AWS_EXTERNAL_ID = "EYB38761_SFCRole=3_mBDml75hW2sGTXwC0B5gY5sVBXc="

use role accountadmin;

-------------------------------
-- Create Catalog Integration (plumbing)
-------------------------------

CREATE OR REPLACE CATALOG INTEGRATION CAT_INT_GLUE
CATALOG_SOURCE = GLUE
CATALOG_NAMESPACE ='hrms'
TABLE_FORMAT = ICEBERG
-- GLUE_AWS_ROLE_ARN = 'arn:aws:iam::133202620729:role/s3_glue_Role' 
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si'
GLUE_CATALOG_ID='904233092605'  -- USE THE 12 DIGIT AWS ACCOUNT NUMBER OF THE GLUE CATALOG YOU WANT 
GLUE_REGION='us-west-2'
ENABLED=TRUE;

SHOW INTEGRATIONS;  -- Note: It seems this does not include the external volume even though it has its own external id and needs to be on the s3 trust policy

DESCRIBE INTEGRATION CAT_INT_GLUE;
-- GLUE_AWS_EXTERNAL_ID = EYB38761_SFCRole=3_hRhPLXzyO9eFQUk5tR5J6ozTvi4=

-----------------------------
-- Create a Snowflake Catalog
-----------------------------
CREATE OR REPLACE CATALOG hrms_glue -- This fails. Do I not have this enabled in my snowflake account?
  USING INTEGRATION CAT_INT_GLUE;

show catalogs;   -- SQL compilation error: Object type or Class 'CATALOGS' does not exist or not authorized.

------------------------------
-- Create an iceberg external table
------------------------------

CREATE OR REPLACE ICEBERG TABLE hrms.aws_athena_employees_iceberg_ext
  CATALOG = GLUE
  CATALOG_INTEGRATION = CAT_INT_GLUE
  EXTERNAL_VOLUME = your_external_volume_name
  BASE_LOCATION = 's3://vdw-dev-iceberg/employees_iceberg/';



---------

CREATE OR REPLACE ICEBERG TABLE ICEBERG.HR.IB_EMPLOYEES
CATALOG_TABLE_NAME ='aws_iceberg_employees'
CATALOG =' CAT_INT_GLUE'
EXTERNAL_VOLUME = 'iceberg_volume';
--METADATA_FILE_PATH =


SELECT * FROM ICEBERG.HR.IB_EMPLOYEES;


UPDATE ICEBERG.HR.IB_EMPLOYEES
SET FIRST_NAME ='TEST'
WHERE EMPLOYEE_ID =108;


ALTER ICEBERG TABLE ICEBERG.HR.IB_EMPLOYEES CONVERT TO MANAGED
BASE_LOCATION ='ib_employees-managed-table'


CREATE OR REPLACE CATALOG INTEGRATION CAT_INT_SNOWFLAKE
CATALOG_SOURCE = OBJECT_STORE
TABLE_FORMAT = ICEBERG
ENABLED=TRUE;


SHOW TABLES


SELECT MIN(FIRST_NAME),MAX(FIRST_NAME),MIN(LAST_NAME),MAX(LAST_NAME), MIN(EMAIL), MAX(EMAIL),
MIN(PHONE_NUMBER), MAX(PHONE_NUMBER),
MIN(HIRE_DATE), MAX(HIRE_DATE)
FROM EMPLOYEES
WHERE EMPLOYEE_ID IN (118,107,119,113)