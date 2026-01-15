---------------------------------------
-- DIRECTORY TABLE QUERY GIVES FILE NAME AND URL WITHIN STAGE
---------------------------------------
SELECT
    CONCAT('s3://', RELATIVE_PATH) AS full_path, -- < FULL PATH IS WRONG
    RELATIVE_PATH, FILE_URL
FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage);

---------------------------------------
-- INFORMATION_SCHEMA.STAGES GIVES YOU THE S3 URL OF THE STAGE
---------------------------------------
 SELECT stage_url  FROM information_schema.stages
 WHERE stage_catalog = 'ICEBERG'
      AND stage_schema  = 'HR'
      AND stage_name    = 'EMPLOYEES_PARQUET_STAGE';
      
---------------------------------------
-- COMPOSITE QUERY TO GIVE FULL S3 AND SF FILE NAMES AND A SCOPED FILE URL (SF ENDPOINT) and PRESIGNED FILE URL (s3 endpoint)
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
    ) AS PRESIGNED_URL,
    -- 'SCOPED_DOWNLOAD_URL' AS scoped_url_type
    -- GET_RELATIVE_PATH(@ICEBERG.HR.employees_parquet_stage,relative_path) AS GET_RELATIVE_PATH
    -- GET_RELATIVE_PATH( @ICEBERG.HR.employees_parquet_stage, CONCAT(stg.folder_prefix, files.relative_path)) -- FIXED ) AS get_relative_path
    GET_RELATIVE_PATH( @ICEBERG.HR.employees_parquet_stage, files.relative_path ) AS get_relative_path
FROM stage_info stg
JOIN (
    SELECT RELATIVE_PATH, FILE_URL
    -- from directory(CONCAT('@',stg.stage_name)  -- No way to avoid hardcoding
    FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage)
) AS files
ON TRUE;




-- EXAMPLE
-- FILE_URL_SF = https://eyb38761.snowflakecomputing.com/api/files/ICEBERG/HR/EMPLOYEES_PARQUET_STAGE/employees_part_001%2esnappy%2eparquet
-- SCOPED_URL = https://eyb38761.snowflakecomputing.com/api/files/01c1bc3a-0207-7f06-001b-667f000481ca/117683728445/9DB9M3gs7fck5wJGxmMzdekqhIkwTih90JP5IiLkavl6fDKM8RoDNSBigfdjpsMYiF4YOgLoy359Tas8xPcpHM0pgEvRWlExwGYxUNg7O%2bCS%2bAwqx8u9SorACvuUifr0RgTnpdUkdPrlQtbWC3fX2R0bWb25yofeGQS1wxZx%2biT715R2JSJV9C87N3%2baoKSelIlXy1rhQuzdgHHlzXeAuIlrbu%2froPgPXjV2ermyw8XcGZosS4l9kN8g6vR9Mg72Pg%2fAp9j8bwpJSfkyaIzCVF%2b8xvwq9BGh62b0I%2bKwb%2fERONhB3v5cCoHhJWQdJ1rUkaabo4Ju


WITH stage_info AS (
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
SELECT
    stg.database,
    stg.schema,
    stg.stage_name,
    stg.stage_url AS stg_url_s3,
    files.relative_path,
    CONCAT(stg.stage_url, files.relative_path) AS file_url_s3,
    files.file_url AS file_url_sf,

    BUILD_SCOPED_FILE_URL(
        @ICEBERG.HR.employees_parquet_stage,
        files.relative_path
    ) AS scoped_url,

    GET_PRESIGNED_URL(
        @ICEBERG.HR.employees_parquet_stage,
        files.relative_path
    ) AS presigned_url,

    GET_RELATIVE_PATH(
        @ICEBERG.HR.employees_parquet_stage,
        CONCAT(stg.folder_prefix, files.relative_path)   -- FIXED
    ) AS get_relative_path

FROM stage_info stg
JOIN (
    SELECT RELATIVE_PATH, FILE_URL
    FROM DIRECTORY(@ICEBERG.HR.employees_parquet_stage)
) AS files
ON TRUE;

);
select get_stage_location(@ICEBERG.HR.employees_parquet_stage); -- returns url from the stage definition

list @employees_parquet_stage;
list @ICEBERG.HR.employees_parquet_stage;

desc integration iceberg_unmanaged_si;
-- STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::935542360084:user/6nrd1000-s
-- STORAGE_AWS_EXTERNAL_ID = EYB38761_SFCRole=3_Eorb/6bhHX6xhHAORaKJ95d6wXI=  <-- this is correct