-- Account level objects include: 
-- Warehouse, SI

/* ----------------------------------------
Set Context
---------------------------------------- */
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;
use role sysadmin;
create or alter database de_sandbox;
describe database de_sandbox;
use database de_sandbox;
create schema quick_refresher;
use schema quick_refresher;
show grants on database de_sandbox;

/* ----------------------------------------
warehouse
---------------------------------------- */
-- drop warehouse coco_wh;
CREATE or alter  WAREHOUSE coco_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_RESUME = TRUE
  AUTO_SUSPEND = 60;

show warehouses;

/* ----------------------------------------
Storage Integration
---------------------------------------- */
-- s3 trust policy

Create Storage Integration
-- s3 trust POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::357486165516:user/lelw1000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "UNB14794_SFCRole=5_VP1xhZQ9iiTeSvWh1YC6ZhcCRHM="
                }
            }
        }
    ]
}


use role sysadmin;
CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
  's3://vdw-dev-ingest/loadingdatalabs/json/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/');
-- Update s3 trust POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::357486165516:user/lelw1000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "UNB14794_SFCRole=5_VP1xhZQ9iiTeSvWh1YC6ZhcCRHM="
                }
            }
        }
    ]
}
--  Test the stage
 desc integration AWS_S3_INT;
 CREATE or alter  STAGE test_stage
  STORAGE_INTEGRATION = AWS_S3_INT
  URL = 's3://vdw-dev-ingest/loadingdatalabs/JSON/';
list @loadingdatalabs_json_stage;
list @loadingdatalabs_stage;
list @test_stage;

CREATE or alter  STAGE loadingdatalabs_json_stage
  STORAGE_INTEGRATION = AWS_S3_INT
  URL = 's3://vdw-dev-ingest/loadingdatalabs/json/';


CREATE or alter  STAGE loadingdatalabs_stage
  STORAGE_INTEGRATION = AWS_S3_INT
  URL = 's3://vdw-dev-ingest/loadingdatalabs/';

 s3://vdw-dev-ingest/loadingdatalabs/JSON/A4/laptop_multi_json_array.json

-- STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::357486165516:user/lelw1000-s
-- STORAGE_AWS_EXTERNAL_ID = UNB14794_SFCRole=3_UMVBvRyXkdWxPSTsRyVRS5Z1Tt0=
desc storage integration AWS_S3_INT;
show integrations;
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
SHOW GRANTS ON INTEGRATION AWS_S3_INT;


-- CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = S3
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://intro-to-snowflake-snowpipe/')
--   COMMENT = 'Create Storage integration to connect Snowflake with AWS';



-- CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_INT
-- TYPE = EXTERNAL_STAGE
-- STORAGE_PROVIDER = S3
-- ENABLED = TRUE 
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::133202620729:role/SNOWFLAKE-S3-FULL-ACCESS'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://learn2cloud-snowflake/')
-- -- STORAGE_BLOCKED_LOCATIONS = ('s3://learn2cloud-snowflake/secret_folder/')
-- COMMENT = 'Create Storage integration to connect Snowflake with AWS';
Desc storage integration AWS_S3_INT;

/* ----------------------------------------
Create database-level objects
---------------------------------------- */
use role sysadmin;
-- establish context
create or alter database de_sandbox;
use database de_sandbox;
Create or alter schema quick_referesher;
USE SCHEMA DE_SANDBOX.quick_referesher;
SELECT CURRENT_DATABASE() || '.' || CURRENT_SCHEMA() || ' with role ' || current_role();


/* ----------------------------------------
Cleanup AWS_S3_INT;
---------------------------------------- */
-- drop storage integration AWS_S3_INT;
-- drop file format CSV_ETL_FILEFORMAT;

/* ----------------------------------------
 File FORMAT
---------------------------------------- */
--CREATE OR ALTER FILE FORMAT DE_SANDBOX.QUICK_REFRESHER.CSV_ETL_FILEFORMAT <-- strange that FQN does not work
CREATE OR ALTER FILE FORMAT CSV_ETL_FILEFORMAT
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 0
NULL_IF = ('Null','NULL')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'        
TRIM_SPACE=TRUE
COMPRESSION=AUTO;

show file formats like 'CSV_ETL_FILEFORMAT';

/*---------------------------------------
Stages
---------------------------------------*/
SHOW STAGES;
DESC INTEGRATION AWS_S3_INT;

CREATE OR ALTER STAGE myoldfree_labcsv
  STORAGE_INTEGRATION = AWS_S3_INT
  URL = 's3://vdw-dev-ingest/loadingdatalabs/labcsv'
  FILE_FORMAT = CSV_ETL_FILEFORMAT;

desc storage integration AWS_S3_INT;

LIST @myoldfree_labcsv;

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
        T.$11,
        T.METADATA$FILENAME
FROM @AWS_ETL_CSV_STAGE T;

SELECT * FROM @AWS_ETL_CSV_STAGE;