--- Context
create database  externaldb;
create schema et;

------------------------------
--Assignment 3 -Creating Partitioned Parquet External Tables 
------------------------------ 
--Q1 --Create partitioned ET with Parquet files 
--Create a folder in AWS  to hold partition data files. 
--Create sub partition inside the AWS folder based on Country 
--Upload files to the folder 
--Create a file format for the parquet files --Create a stage in Snowflake  which points to the partition folder --List the files from the s3 folder in AWS --Use split_part on the file name and exact the CITY column partition data --Create the ET and when creating it ensure that the CITY column is coming from the External  table path 
-- Create folder in the path s3://<student_folder_name>/external_table_lab/et_partition_lab/ 
-- Create partition folder in s3 in the above partition one for each country in the locations table 
-- COUNTRY_ID 
CREATE OR REPLACE FILE FORMAT EXTERNALDB.ET.PARQUET_ET_LAB_FILEFORMAT 
TYPE = PARQUET 
COMPRESSION=NONE; 

-- CREATE OR REPLACE STAGE 
-- EXTERNALDB.ET.AWS_ET_PARTITIONED_LAB_PARQUET_STAGE 
-- STORAGE_INTEGRATION = AWS_S3_INT 
-- URL = 's3://learn2cloud-snowflake/external_table_lab/et_partition_lab/' 
-- FILE_FORMAT = PARQUET_ET_LAB_FILEFORMAT; 
show integrations;
desc integration UDEMY_MC_ET_PARQUET_SI;
-- CREATE OR REPLACE STAGE 
-- EXTERNALDB.ET.AWS_ET_PARTITIONED_LAB_PARQUET_STAGE 
-- URL = 's3://vdw-dev-ingest/external_table_lab/et_partition_lab/'
-- STORAGE_INTEGRATION = UDEMY_MC_ET_PARQUET_SI
-- FILE_FORMAT = EXTERNALDB.ET.PARQUET_ET_LAB_FILEFORMAT ;
-- List the files in the stage
CREATE OR REPLACE STAGE EXTERNALDB.ET.AWS_ET_PARQUET_STAGE 
  STORAGE_INTEGRATION = UDEMY_MC_ET_PARQUET_SI  -- arn:aws:iam::969799720206:user/erqg1000-s    COB74867_SFCRole=4_1WE6w4I9rnqEsp8wmmUmpC247fY=
  URL = 's3://vdw-dev-ingest/loadingdatalabs/labcsv/et_parquet/'
  FILE_FORMAT = (TYPE = PARQUET);
LIST @AWS_ET_PARQUET_STAGE;
--Failure using stage area. Cause: [User: arn:aws:sts::904233092605:assumed-role/vdw_dev_data_ingest_si_role/snowflake is not authorized to perform: s3:ListBucket on resource: "arn:aws:s3:::vdw-dev-ingest" because no identity-based policy allows the s3:ListBucket action (Status Code: 403; Error Code: AccessDenied)]

LIST @AWS_ET_PARTITIONED_LAB_PARQUET_STAGE;
 -- List the files and the country id
SELECT metadata$filename,split_part(metadata$filename, '/', 3) FROM 
@AWS_ET_PARTITIONED_LAB_PARQUET_STAGE; 
-- Create partitioned external table
CREATE OR REPLACE EXTERNAL TABLE EXT_LOCATIONS_PARTITIONED 
( 
LOCATION_ID       VARCHAR   AS ($1:LOCATION_ID::VARCHAR), 
STREET_ADDRESS    VARCHAR   AS ($1:STREET_ADDRESS::VARCHAR), 
POSTAL_CODE       VARCHAR   AS ($1:POSTAL_CODE::VARCHAR), 
CITY              VARCHAR   AS ($1:CITY::VARCHAR), 
-- STATE_PROVINC     VARCHAR   AS ($1:STATE_PROVINCE::VARCHAR),   COUNTRY_ID        VARCHAR   AS ($1:COUNTRY_ID::VARCHAR), 
COUNTRY_ID        varchar AS  split_part(metadata$filename, '/', 3) 
) 
LOCATION=@AWS_ET_PARTITIONED_LAB_PARQUET_STAGE 
PATTERN='.*locations.*[.]parquet' 
FILE_FORMAT = PARQUET_ET_LAB_FILEFORMAT; 
-- Confirm the external table is working
SELECT * FROM EXT_LOCATIONS_PARTITIONED
WHERE COUNTRY_ID='US'; 