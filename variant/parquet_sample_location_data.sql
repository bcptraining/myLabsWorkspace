--- Context
create database  externaldb;
create schema et;

use database externaldb;
use schema et;

------------------------------
--  Alter the SI to add parquet location (if needed)
------------------------------
-- desc integration UDEMY_MC_ET_PARQUET_SI; -- Add this allowed locattion:  external_table_lab/parquet/
ALTER STORAGE INTEGRATION UDEMY_MC_ET_PARQUET_SI
SET STORAGE_ALLOWED_LOCATIONS = (
    's3://vdw-dev-ingest/loadingdatalabs/',
    's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
    's3://vdw-dev-ingest/loadingdatalabs/json/',
    's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
    's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/',
    's3://vdw-dev-ingest/loadingdatalabs/labcsv/et_parquet/',
    's3://vdw-dev-ingest/external_table_lab/parquet/'
);

------------------------------
-- Create parquet format
------------------------------ 

CREATE OR REPLACE FILE FORMAT EXTERNALDB.ET.PARQUET_ET_LAB_FILEFORMAT 
TYPE = PARQUET 
COMPRESSION=NONE; 

------------------------------
-- Create External Stage (test it!)
------------------------------ 
CREATE OR REPLACE STAGE EXTERNALDB.ET.AWS_PARQUET_SAMPLE_STAGE 
  STORAGE_INTEGRATION = UDEMY_MC_ET_PARQUET_SI  -- arn:aws:iam::969799720206:user/erqg1000-s    COB74867_SFCRole=4_1WE6w4I9rnqEsp8wmmUmpC247fY=     
  URL = 's3://vdw-dev-ingest/external_table_lab/parquet/'
  FILE_FORMAT = (TYPE = PARQUET);

LIST @AWS_PARQUET_SAMPLE_STAGE;

 -- List the files and the country id
SELECT metadata$filename,split_part(metadata$filename, '/', 3),
split_part(split_part(split_part(metadata$filename, '/', -1), '_', 2), '.', 1) AS country_code
FROM 
@AWS_PARQUET_SAMPLE_STAGE; 

------------------------------
-- Create External Table PARTITIONED BY VOUNTRY_ID
------------------------------

CREATE OR REPLACE EXTERNAL TABLE externaldb.ET.EXT_LOCATIONS_PARTITIONED_PARQUET 
( 
LOCATION_ID       VARCHAR   AS ($1:LOCATION_ID::VARCHAR), 
STREET_ADDRESS    VARCHAR   AS ($1:STREET_ADDRESS::VARCHAR), 
POSTAL_CODE       VARCHAR   AS ($1:POSTAL_CODE::VARCHAR), 
CITY              VARCHAR   AS ($1:CITY::VARCHAR), 
-- STATE_PROVINC     VARCHAR   AS ($1:STATE_PROVINCE::VARCHAR),   COUNTRY_ID        VARCHAR   AS ($1:COUNTRY_ID::VARCHAR), 
COUNTRY_ID        varchar AS  split_part(split_part(split_part(metadata$filename, '/', -1), '_', 2), '.', 1)
) 
PARTITION BY (COUNTRY_ID) 
LOCATION=@AWS_PARQUET_SAMPLE_STAGE 
PATTERN='.*locations.*[.]parquet' 
FILE_FORMAT = PARQUET_ET_LAB_FILEFORMAT;

-- Confirm the external table is working
SELECT * FROM EXTERNALDB.ET.EXT_LOCATIONS_PARTITIONED_PARQUET
WHERE COUNTRY_ID='US'; 