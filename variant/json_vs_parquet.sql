---------------------------------
-- Hands‑On Lab: JSON vs Parquet in Snowflake
-- Using Real LOCATIONS Parquet Data
---------------------------------

-- Lab Overview
-- You will:
-- 1. Materialize your Parquet external table into a typed table
-- 2. Expand the dataset (so Parquet advantages become visible)
-- 3. Create a JSON VARIANT table with identical logical data
-- 4. Compare storage size
-- 5. Compare query performance
-- 6. Explore schema evolution & missing fields

---------------------------------
-- Context Setup
---------------------------------

CREATE DATABASE IF NOT EXISTS variant_formats;
CREATE SCHEMA IF NOT EXISTS json_vs_parquet_lab;
USE DATABASE variant_formats;
USE SCHEMA json_vs_parquet_lab;

----------------------------------
-- Explore the parquet data that we aree starting with
----------------------------------
select * 
FROM EXTERNALDB.ET.EXT_LOCATIONS_PARTITIONED_PARQUET;

---------------------------------
-- 2. Materialize Parquet External Table Into a Typed Table
---------------------------------

CREATE OR REPLACE TABLE locations_parquet_src AS
SELECT
    LOCATION_ID::NUMBER LOCATION_ID,
    STREET_ADDRESS::STRING   AS STREET_ADDRESS,
    POSTAL_CODE::STRING      AS POSTAL_CODE,
    CITY::STRING             AS CITY,
    COUNTRY_ID::STRING       AS COUNTRY_ID
FROM EXTERNALDB.ET.EXT_LOCATIONS_PARTITIONED_PARQUET;
-- cREATED THE TABLE THIS WAY INSTEAD SO LOCATION_ID COULD BE TYPES AS A NUMBER

DESCRIBE TABLE locations_parquet_src;
CREATE OR REPLACE TABLE locations_parquet_src AS
SELECT
    /* LOCATION_ID should be numeric */
    CASE 
        WHEN TRY_TO_NUMBER(LOCATION_ID) IS NULL 
            THEN  2300   -- the correct LOCATION_ID for the Mexico row
        ELSE LOCATION_ID::NUMBER(38,0)
    END AS LOCATION_ID,

    /* STREET_ADDRESS should be the real street */
    CASE 
        WHEN TRY_TO_NUMBER(LOCATION_ID) IS NULL 
            THEN 'MARIANO ESCOBEDO 9991'
        ELSE STREET_ADDRESS
    END AS STREET_ADDRESS,

    /* POSTAL_CODE should be numeric or string postal code */
    CASE 
        WHEN TRY_TO_NUMBER(LOCATION_ID) IS NULL 
            THEN '11932'
        ELSE POSTAL_CODE
    END AS POSTAL_CODE,

    /* CITY should be the real city */
    CASE 
        WHEN TRY_TO_NUMBER(LOCATION_ID) IS NULL 
            THEN 'MEXICO CITY'
        ELSE RTRIM(CITY, '\\')   -- also removes the trailing slash
    END AS CITY,

    COUNTRY_ID::STRING AS COUNTRY_ID
FROM EXTERNALDB.ET.EXT_LOCATIONS_PARTITIONED_PARQUET;


SELECT * FROM locations_parquet_src LIMIT 20;

---------------------------------
-- 3. Expand Dataset (So JSON vs Parquet Differences Are Visible)
---------------------------------

-- Expand to ~600k rows (12 rows × 50,000)
-- Adjust ROWCOUNT lower if you want to minimize cost.

CREATE OR REPLACE TABLE locations_parquet_big AS
SELECT *
FROM locations_parquet_src
CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 50000));

SELECT COUNT(*) FROM locations_parquet_big; -- 1M rows
DESC TABLE locations_parquet_big;;
-- SELECT * FROM locations_parquet_big LIMIT 10;
---------------------------------
-- 4. Create JSON Table With Identical Logical Data
---------------------------------

CREATE OR REPLACE TABLE locations_json_src (raw VARIANT);

SELECT * FROM locations_parquet_big limit 10;

INSERT INTO locations_json_src
SELECT OBJECT_CONSTRUCT(
    'LOCATION_ID', LOCATION_ID,
    'STREET_ADDRESS', STREET_ADDRESS,
    'POSTAL_CODE', POSTAL_CODE,
    'CITY', CITY,
    'COUNTRY_ID', COUNTRY_ID
)
FROM locations_parquet_big;
-- DESC TABLE locations_json_src;
SELECT COUNT(*) FROM locations_json_src; -- 1M ROWS
SELECT COUNT(*) FROM locations_parquet_big; -- 1M ROWS

---------------------------------
-- 5. Validate Row Counts Match
---------------------------------

SELECT
    (SELECT COUNT(*) FROM locations_parquet_big) AS parquet_rows, -- 1M rows; 4120576 BYTES
    (SELECT COUNT(*) FROM locations_json_src) AS json_rows;       -- 1M rows; 3793408 BYTES

---------------------------------
-- 6. Compare Storage Size
---------------------------------
-- While this step is valid, the results were invalid due the way I synthetically generated the data
SELECT table_name, bytes
FROM information_schema.tables
WHERE table_name ILIKE '%LOCATIONS%';

-- Expected:
--   locations_parquet_big → smaller (columnar, compressed)
--   locations_json_src    → larger (text-based JSON in VARIANT)

---------------------------------
-- 7. Compare Query Performance
---------------------------------

-- Parquet (typed, columnar, pruning)
EXPLAIN
SELECT *
FROM locations_parquet_big
WHERE COUNTRY_ID = 'US';

-- JSON (semi-structured, no pruning)
EXPLAIN
SELECT *
FROM locations_json_src
WHERE raw:COUNTRY_ID::STRING = 'US';

---------------------------------
-- 8. Schema Evolution Differences
---------------------------------

-- JSON evolves naturally
INSERT INTO locations_json_src
SELECT PARSE_JSON('{
  "LOCATION_ID": 9999,
  "CITY": "New City",
  "COUNTRY_ID": "US",
  "NEW_FIELD": "extra metadata"
}');

-- Parquet requires producer schema updates.

---------------------------------
-- 9. Query Behavior With Missing Fields
---------------------------------

SELECT raw:NEW_FIELD::STRING
FROM locations_json_src
LIMIT 10;

SELECT raw:NEW_FIELD::STRING
FROM locations_parquet_big
LIMIT 10;

-- JSON: missing fields → NULL
-- Parquet: depends on file schema evolution rules

---------------------------------
-- END OF LAB
---------------------------------
