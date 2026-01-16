---------------------------------------
-- Context
---------------------------------------
use database ICEBERG;
USE SCHEMA HR;

---------------------------------------
-- Summary of file functions
---------------------------------------
SELECT GET_STAGE_LOCATION(@ICEBERG.HR.EMPLOYEES_PARQUET_STAGE); -- RETURNS THE URL FROM THE STAGE DEFINITION: s3://vdw-dev-iceberg/employees/
SELECT GET_ABSOLUTE_PATH( @ICEBERG.HR.EMPLOYEES_PARQUET_STAGE ,  'employees_part_001.snappy.parquet'); -- s3://vdw-dev-iceberg/employees/employees_part_001.snappy.parquet
SELECT GET_RELATIVE_PATH( @ICEBERG.HR.EMPLOYEES_PARQUET_STAGE ,  's3://vdw-dev-iceberg/employees/employees_part_001.snappy.parquet'); -- employees_part_001.snappy.parquet
SELECT GET_PRESIGNED_URL( @ICEBERG.HR.EMPLOYEES_PARQUET_STAGE  , 'employees_part_001.snappy.parquet' , [ <expiration_time> ] ); -- gives you an aws-managed s3 url for specified seconds 
SELECT GET_PRESIGNED_URL( @ICEBERG.HR.EMPLOYEES_PARQUET_STAGE  , 'employees_part_001.snappy.parquet' );                         -- Gives you an aws-managed url for 1 hr (default) 
SELECT GET_PRESIGNED_URL( @ICEBERG.HR.EMPLOYEES_PARQUET_STAGE  , 'employees_part_001.snappy.parquet');        -- Gives you an aws-managed url for 1 hr (default)
SELECT BUILD_SCOPED_FILE_URL(  @<stage_name> ,  '<relative_file_path>' ,   <use_privatelink_host_for_business_critical>) ; -- Snowflake-managed url for 1 hr and by default uses url of privatelink.snowflakecomputing.com
SELECT BUILD_SCOPED_FILE_URL(@ICEBERG.HR.employees_parquet_stage, 'employees_part_001.snappy.parquet'); -- gives you a Snowflake-managed url FOR 1 HOUR

---------------------------------------
-- COMPOSITE QUERY example TO GIVE FULL S3 AND SF FILE NAMES AND A SCOPED FILE URL (SF ENDPOINT) and PRESIGNED FILE URL (s3 endpoint)
-- information_schema.stages provides s3 url for the stage
-- directory table provides RELATIVE_PATH (file name) and FILE_URL (of the file in the sf stage)
---------------------------------------

WITH stage_info AS ( -- information_schema.stages
    SELECT 
        STAGE_CATALOG AS DATABASE,
        STAGE_SCHEMA  AS SCHEMA,
        STAGE_NAME, 
        STAGE_URL,
        REGEXP_REPLACE(STAGE_URL, '^s3://[^/]+/', '') AS folder_prefix   -- extract "employees/"
    FROM information_schema.stages
    WHERE stage_catalog = 'ICEBERG'
      AND stage_schema  = 'HR'
      AND stage_name    = 'EMPLOYEES_PARQUET_STAGE'
)
SELECT              -- files in stage
    stg.database, stg.schema, stg.stage_name, stg.stage_url as stg_url_s3,      -- stage info
    files.relative_path,                                                        -- file name
    CONCAT(stg.stage_url, files.relative_path) AS file_url_s3,                  -- full file url in s3
    files.file_url AS file_url_sf,                                              -- file url within Snowflake
    -- 'INTERNAL_STAGE_URL' AS file_url_sf_type,
    BUILD_SCOPED_FILE_URL(                                                      -- Scoped URL to download the file from Snowflake  
        @ICEBERG.HR.employees_parquet_stage,                                    -- No way to avoid this hardcoding
        -- CONCAT('@',stg.stage_name),  -- SQL compilation error: argument 1 to function BUILD_SCOPED_FILE_URL needs to be constant, found 'CONCAT('@', STG.STAGE_NAME)'
        files.relative_path          
    ) AS scoped_url,
    GET_PRESIGNED_URL(@ICEBERG.HR.employees_parquet_stage, files.relative_path  -- Resigned url to access files in s3       
    ) AS PRESIGNED_URL
    -- 'SCOPED_DOWNLOAD_URL' AS scoped_url_type
    -- GET_RELATIVE_PATH(@ICEBERG.HR.employees_parquet_stage,relative_path) AS GET_RELATIVE_PATH
    -- GET_RELATIVE_PATH( @ICEBERG.HR.employees_parquet_stage, CONCAT(stg.folder_prefix, files.relative_path)) -- FIXED ) AS get_relative_path

    -- ,GET_RELATIVE_PATH( @ICEBERG.HR.employees_parquet_stage, files.relative_path ) AS get_relative_path
    ,GET_RELATIVE_PATH(  @ICEBERG.HR.employees_parquet_stage,  CONCAT(stg.stage_url, files.relative_path))

FROM stage_info stg
JOIN (
    SELECT RELATIVE_PATH, FILE_URL
    -- from directory(CONCAT('@',stg.stage_name)  -- No way to avoid hardcoding
    FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage)
) AS files
ON TRUE;

DESCRIBE STAGE ICEBERG.HR.EMPLOYEES_PARQUET_STAGE;
ALTER STAGE ICEBERG.HR.employees_parquet_stage REFRESH;
SELECT * FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage);
SELECT * FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage) WHERE RELATIVE_PATH LIKE '%employees%';
DROP STAGE IF EXISTS ICEBERG.HR.employees_parquet_stage;
CREATE STAGE ICEBERG.HR.employees_parquet_stage
  URL='s3://vdw-dev-iceberg/employees/'
  STORAGE_INTEGRATION = iceberg_unmanaged_si
  DIRECTORY = (ENABLE = TRUE);


SELECT *
FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage)
WHERE RELATIVE_PATH = 'employees_part_001.snappy.parquet';
