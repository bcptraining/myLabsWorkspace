-------------------
-- Purpose: Load a LOCATION table from parquet files and quarantine bad data into LOCATION_ERRORS
-------------------

CREATE OR REPLACE DATABASE EXTERNALDB; 
CREATE or replace SCHEMA ET; 

-- Create a locations table (since it was missing)
CREATE OR REPLACE TABLE LOCATION ( LOCATION_ID INTEGER PRIMARY KEY, STREET_ADDRESS VARCHAR(100), POSTAL_CODE VARCHAR(10), CITY VARCHAR(50), STATE_PROVINCE VARCHAR(50), COUNTRY_ID CHAR(2) );

CREATE OR REPLACE FILE FORMAT EXTERNALDB.ET.CSV_ET_LAB_PARQUETFORMAT 
TYPE = PARQUET;

CREATE OR REPLACE STAGE EXTERNALDB.ET.PARQUET_STAGE
  URL = 's3://vdw-dev-ingest/external_table_lab/parquet/'
  STORAGE_INTEGRATION = udemy_mc_et_si
  FILE_FORMAT = EXTERNALDB.ET.CSV_ET_LAB_PARQUETFORMAT;


LIST @PARQUET_STAGE;
LIST @EXTERNALDB.ET.PARQUET_STAGE;


SELECT *
FROM @EXTERNALDB.ET.PARQUET_STAGE
-- (FILE_FORMAT = (TYPE = PARQUET))
LIMIT 5;





SELECT * FROM  @EXTERNALDB.ET.PARQUET_STAGE
 ;

COPY INTO EXTERNALDB.ET.LOCATION 
FROM @EXTERNALDB.ET.PARQUET_STAGE FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
---------------------
-- Inspect the parquet file schema
---------------------
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@EXTERNALDB.ET.PARQUET_STAGE',
        FILE_FORMAT => 'EXTERNALDB.ET.CSV_ET_LAB_PARQUETFORMAT'
    )
);

-------------------------
-- Load into a temp table for inspection
-------------------------
CREATE OR REPLACE TEMP TABLE PARQUET_RAW (v VARIANT);

COPY INTO PARQUET_RAW
FROM @EXTERNALDB.ET.PARQUET_STAGE
FILE_FORMAT = (TYPE = PARQUET);

select * from PARQUET_RAW;
---------------------
-- Can I load the file into my table?
---------------------
COPY INTO EXTERNALDB.ET.LOCATION
FROM @EXTERNALDB.ET.PARQUET_STAGE
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE; -- Error: considered a transform
-- VALIDATION_MODE = RETURN_ERRORS;

--There is a row with a data error: "LOCATION_ID": "MARIANO ESCOBEDO 9991", 
---------------------------------
-- Approach: Load into a staging VARIANT table, then clean manually
-----------------------
-- Step 1: Create a staging table
CREATE OR REPLACE TABLE LOCATION_STAGE (v VARIANT);
-- Step 2: Load Parquet into the staging table
COPY INTO LOCATION_STAGE
FROM @EXTERNALDB.ET.PARQUET_STAGE
FILE_FORMAT = (TYPE = PARQUET);
-- Step 3: Determine the type of LOCATION_ID column
-- SELECT v FROM LOCATION_STAGE 
-- --WHERE TRY_TO_NUMBER(v:LOCATION_ID) IS NULL;
-- WHERE TRY_TO_NUMBER(v:LOCATION_ID) IS NULL;
--- Determine the type for LOCATION_ID for the bad row (its VARCHAR)
SELECT 
    v:LOCATION_ID,
    TYPEOF(v:LOCATION_ID)
FROM LOCATION_STAGE
WHERE v:LOCATION_ID::STRING LIKE '%MARIANO%'; -- VARCHAR
-- Now we know the bad row has LOCATION_ID as a varchar and TRY_TO_NUMBER will not implicitly cast as VARCHAR so must explicitly cast it
-- Now we get the malformed row
SELECT v FROM LOCATION_STAGE 
WHERE TRY_TO_NUMBER(v:LOCATION_ID::STRING) is NULL;

-- So now we can load the good rows and isolate the bad... but instead we get another data issue in POSTAL_CODE = <some location name>
-- Lets take a look at the bad rows: 
SELECT v
FROM LOCATION_STAGE
WHERE LENGTH(v:POSTAL_CODE::STRING) > 10;

-- so we have to widen the filter to exclude and isolote both errors
-- Step 4: Load the good data into the location table

select * from LOCATION_STAGE;
INSERT INTO EXTERNALDB.ET.LOCATION
(
    LOCATION_ID,
    STREET_ADDRESS,
    POSTAL_CODE,
    CITY,
    STATE_PROVINCE,
    COUNTRY_ID
)
SELECT
    TRY_TO_NUMBER(v:LOCATION_ID::STRING) AS LOCATION_ID,
    v:STREET_ADDRESS::STRING,
    v:POSTAL_CODE::STRING,
    v:CITY::STRING,
    v:STATE_PROVINCE::STRING,
    v:COUNTRY_ID::STRING
FROM LOCATION_STAGE
WHERE TRY_TO_NUMBER(v:LOCATION_ID::STRING) IS NOT NULL
 and LENGTH(v:POSTAL_CODE::STRING) <= 10;

-- review the loaded data: 
SELECT * FROM LOCATION;

-- Step 5: Quarantine the bad rows: 
CREATE OR REPLACE TABLE EXTERNALDB.ET.LOCATION_ERRORS (
    v VARIANT
);

INSERT INTO EXTERNALDB.ET.LOCATION_ERRORS
SELECT v
FROM EXTERNALDB.ET.LOCATION_STAGE
WHERE TRY_TO_NUMBER(v:LOCATION_ID::STRING) IS NULL
   OR LENGTH(v:POSTAL_CODE::STRING) > 10;





