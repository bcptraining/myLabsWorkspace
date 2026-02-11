-- Prerequisite create HRMS.HR and complete external_tables/lab4_parquet.sql so we have some parquet data 





-------------------------
-- Create external table
-------------------------
CREATE OR REPLACE EXTERNAL TABLE EXT_EMPLOYEES_REFRESH 
( 
EMPLOYEE_ID    VARCHAR   AS (VALUE:c1::VARCHAR), 
FIRST_NAME     VARCHAR   AS (VALUE:c2::VARCHAR), 
LAST_NAME      VARCHAR   AS (VALUE:c3::VARCHAR), 
EMAIL          
VARCHAR   AS (VALUE:c4::VARCHAR), 
PHONE_NUMBER   VARCHAR   AS (VALUE:c5::VARCHAR), 
HIRE_DATE VARCHAR   AS (VALUE:c6::VARCHAR), 
JOB_ID    VARCHAR   AS (VALUE:c7::VARCHAR), 
SALARY    VARCHAR   AS (VALUE:c8::VARCHAR), 
COMMISSION_PCT VARCHAR   AS (VALUE:c9::VARCHAR), 
MANAGER_ID     VARCHAR   AS (VALUE:c10::VARCHAR), 
DEPARTMENT_ID  VARCHAR   AS (VALUE:c11::VARCHAR) 
) 
LOCATION=HRMS.HR.@AWS_ET_CSV_STAGE 
AUTO_REFRESH= TRUE 
PATTERN='.*employees.*[.]csv' 
FILE_FORMAT = CSV_ET_FILEFORMAT; 
desc  table  EXT_EMPLOYEES_REFRESH;
---------------------------------
-- Hands‑On Lab: JSON vs Parquet (Using Your Real Parquet Dataset)
---------------------------------

-- Lab Overview
-- You will:
-- 1. Load Parquet data from your S3 stage
-- 2. Store it as VARIANT in a Parquet-backed table
-- 3. Convert that same data into JSON text
-- 4. Load the JSON into a JSON-backed table
-- 5. Compare storage size
-- 6. Compare query performance
-- 7. Explore schema evolution & missing fields

---------------------------------
-- 1. Context Setup
---------------------------------

CREATE DATABASE IF NOT EXISTS variant_formats;
CREATE SCHEMA IF NOT EXISTS json_vs_parquet_lab;
USE DATABASE variant_formats;
USE SCHEMA json_vs_parquet_lab;

---------------------------------
-- 2. Create Tables
---------------------------------

-- Parquet-backed table (raw VARIANT)
CREATE OR REPLACE TABLE parquet_employees (raw VARIANT);

-- JSON-backed table (raw VARIANT)
CREATE OR REPLACE TABLE json_employees (raw VARIANT);

---------------------------------
-- 3. Load Parquet From Your S3 Stage
---------------------------------

-- Your stage:
-- HRMS.HR.AWS_ET_PARQUET_STAGE
-- URL = 's3://vdw-dev-ingest/loadingdatalabs/labcsv/parquet/'

LIST @HRMS.HR.AWS_ET_PARQUET_STAGE;

COPY INTO parquet_employees
FROM @HRMS.HR.AWS_ET_PARQUET_STAGE
FILE_FORMAT = (TYPE = PARQUET);

SELECT * FROM parquet_employees LIMIT 20;

---------------------------------
-- 4. Convert Parquet VARIANT Rows Into JSON
---------------------------------

-- Explanation:
--   TO_JSON() converts VARIANT → JSON text
--   PARSE_JSON() converts JSON text → VARIANT
--   This ensures the JSON table contains the *exact same logical data*

INSERT INTO json_employees
SELECT PARSE_JSON(TO_JSON(raw))
FROM parquet_employees;

SELECT * FROM json_employees LIMIT 20;

---------------------------------
-- 5. Validate That Both Tables Contain Identical Logical Data
---------------------------------

-- Compare row counts
SELECT
    (SELECT COUNT(*) FROM parquet_employees) AS parquet_rows,
    (SELECT COUNT(*) FROM json_employees) AS json_rows;

-- Spot-check equality
SELECT
    COUNT(*) AS matching_rows
FROM parquet_employees p
JOIN json_employees j
    ON TO_JSON(p.raw) = TO_JSON(j.raw);

---------------------------------
-- 6. Inspect JSON vs Parquet Internals
---------------------------------

-- JSON
SELECT
    typeof(raw) AS type,
    raw
FROM json_employees
LIMIT 5;

-- Parquet
SELECT
    typeof(raw) AS type,
    raw
FROM parquet_employees
LIMIT 5;

-- Both show OBJECT because Snowflake normalizes both into VARIANT.

---------------------------------
-- 7. Compare Storage Size
---------------------------------

SELECT table_name, bytes
FROM information_schema.tables
WHERE table_name IN ('JSON_EMPLOYEES', 'PARQUET_EMPLOYEES');

-- Expected:
--   Parquet uses fewer bytes due to:
--     • Columnar encoding
--     • Compression (Snappy/GZIP)
--     • Typed binary representation

---------------------------------
-- 8. Compare Query Performance
---------------------------------

-- Example: filter on a nested field (adjust path to match your data)
-- Replace raw:employee:id with whatever fields exist in your Parquet

EXPLAIN
SELECT raw
FROM json_employees
WHERE raw:employee:id::NUMBER > 1000;

EXPLAIN
SELECT raw
FROM parquet_employees
WHERE raw:employee:id::NUMBER > 1000;

-- Expected:
--   Parquet shows:
--     • Fewer CPU operations
--     • Better pruning
--     • Faster execution

---------------------------------
-- 9. Schema Evolution Differences
---------------------------------

-- JSON evolves naturally (schema-less)
INSERT INTO json_employees
SELECT PARSE_JSON('{
  "employee": { "id": 9999, "name": "New Hire", "title": "VP" },
  "compensation": { "salary": 200000 }
}');

-- Parquet requires producer schema updates:
--   The Parquet file must include the new column.
--   Snowflake loads it fine, but the file must be valid.

---------------------------------
-- 10. Query Behavior With Missing Fields
---------------------------------

SELECT raw:employee:title::STRING
FROM json_employees
LIMIT 10;

SELECT raw:employee:title::STRING
FROM parquet_employees
LIMIT 10;

-- JSON: missing fields → NULL
-- Parquet: depends on file schema evolution rules

---------------------------------
-- END OF LAB
---------------------------------
