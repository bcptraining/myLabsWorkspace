--  Account-level objects used
-- si: AWS_S3_INT

-- Set context
use role sysadmin;
use database DE_SANDBOX;
use schema  quick_refresher;


/* ----------------------------------------
Create stages
---------------------------------------- */

--@test_stage is at the root level
CREATE or alter  STAGE test_stage
  STORAGE_INTEGRATION = AWS_S3_INT
  URL = 's3://vdw-dev-ingest/loadingdatalabs/';
list @test_stage;

CREATE or alter  STAGE loadingdatalabs_json_stage
  STORAGE_INTEGRATION = AWS_S3_INT
  URL = 's3://vdw-dev-ingest/loadingdatalabs/json/';

list @loadingdatalabs_json_stage;

/* ----------------------------------------
Explore Stage
---------------------------------------- */
-- CREATE OR REPLACE FILE FORMAT json_ff TYPE = JSON;
CREATE OR REPLACE FILE FORMAT json_strip_array_ff
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

select $1 from @loadingdatalabs_json_stage;  -- 1 row (entire file)

SELECT $1
FROM @loadingdatalabs_json_stage
(FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.JSON_STRIP_ARRAY_FF');

SELECT
    f.value AS employee
FROM @loadingdatalabs_json_stage
     (FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.JSON_FF') T
     , LATERAL FLATTEN(input => T.$1) f;


/* ----------------------------------------
Options for loading into raw layer
---------------------------------------- */
--1. Load into a raw TABLE with a variant column
CREATE OR REPLACE TABLE employees_raw (
    v VARIANT,
    load_dts TIMESTAMP_NTZ,
    file_name TEXT,
    row_number int
);


COPY INTO employees_raw
(
    v,
    load_dts,
    file_name,
    row_number
)
FROM (
    SELECT
        $1,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @loadingdatalabs_json_stage
         (FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.JSON_STRIP_ARRAY_FF')
);

select * from employees_raw;

--2 Load into Employee table  with variant data for skills 
CREATE OR REPLACE TABLE employees_raw2 (
    employee_id     TEXT,
    employee_name   TEXT,
    position        TEXT,
    street          TEXT,
    city            TEXT,
    state           TEXT,
    zip_code        TEXT,
    phone_numbers   VARIANT,
    skills          VARIANT,
    load_dts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name       TEXT,
    row_number      INT
);


COPY INTO employees_raw2
FROM (
  SELECT
    $1:employee_id::TEXT,
    $1:employee_name::TEXT,
    $1:position::TEXT,
    $1:address.street::TEXT,
    $1:address.city::TEXT,
    $1:address.state::TEXT,
    $1:address.zip_code::TEXT,
    $1:phone_numbers,
    $1:skills,
    current_timestamp(),
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER
  FROM @loadingdatalabs_json_stage
  (FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.JSON_STRIP_ARRAY_FF')
);

select * from employees_raw2 ;
-- CREATE OR REPLACE PIPE employees_raw2_pipe
-- -- INTEGRATION = <notification_integration>
-- AS
-- COPY INTO employees_raw2
-- (
--     employee_id,
--     employee_name,
--     position,
--     street,
--     city,
--     state,
--     zip_code,
--     phone_numbers,
--     skills,
--     file_name
-- )
-- FROM (
--     SELECT
--         $1:employee_id::TEXT,
--         $1:employee_name::TEXT,
--         $1:position::TEXT,
--         $1:address.street::TEXT,
--         $1:address.city::TEXT,
--         $1:address.state::TEXT,
--         $1:address.zip_code::TEXT,
--         $1:phone_numbers,
--         $1:skills,
--         METADATA$FILENAME
--     FROM @LOADINGDATALABS_JSON_STAGE
--          (FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.JSON_STRIP_ARRAY_FF')
-- );
-- desc pipe employees_raw2_pipe;
SHOW PIPES IN SCHEMA DE_SANDBOX.QUICK_REFRESHER;

select * from employees_raw2;