--Assignment 01 Create External Table in CSV format. ------------------------------------------------------------------------ --Q1 --Create a database and a schema to store  External Tables. 
CREATE OR REPLACE DATABASE EXTERNALDB; 
CREATE or replace SCHEMA ET; 
use schema ET;




-- select current_schema();
--Q2 --Create a CSV file format with the following properties --Delimiter is comma --Skip header is 1 --Empty Field as NULL 
CREATE OR REPLACE FILE FORMAT EXTERNALDB.ET.CSV_ET_LAB_FILEFORMAT 
TYPE = CSV 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 
EMPTY_FIELD_AS_NULL = TRUE; 
--Q3 --Create a folder in AWS to hold Locations data --Upload Locations data to the folder(upload 3 files, SG,UK and US) 
-- Create a folder on s3 called external_table_lab inside your s3 bucket 
-- Create a folder on inside external_table_lab inside called CSV which will hold our CSV files 
-- s3://<student_folder_name>/external_table_lab/csv/ 
-- s3://vdw-dev-ingest/external_table_lab/csv/
-- Upload the files locations_SG.csv,locations_UK.csv,locations_US.csv 
--Q4 --Create Stage object to be able to read  the uploaded files from Snowflake 
-- si 
---------------------
-- File format
----------------------
CREATE OR REPLACE FILE FORMAT HRMS.HR.CSV_ET_FILEFORMAT_ASSIGNMENT
TYPE = CSV 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 
NULL_IF = ('NULL','Null','null') 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
EMPTY_FIELD_AS_NULL = TRUE; 

describe file format HRMS.HR.CSV_ET_FILEFORMAT ;

----------------------
-- Storage Integration
----------------------
-- CREATE or replace STORAGE INTEGRATION udemy_mc_et_si
-- TYPE = EXTERNAL_STAGE
-- STORAGE_PROVIDER = S3
-- ENABLED = TRUE
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
--   's3://vdw-dev-ingest/loadingdatalabs/labcsv/',  -- already covers /labcsv/streams/
--   's3://vdw-dev-ingest/loadingdatalabs/json/',
--   's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
--   's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/',
--   's3://vdw-dev-ingest/external_table_lab/'
--   );
-- SHOW PARAMETERS LIKE 'ENABLE_EXTERNAL_STAGE_AUTO_REFRESH';

CREATE OR REPLACE STORAGE INTEGRATION udemy_mc_et_si
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092065:role/vdw_dev_data_ingest_si_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://vdw-dev-ingest/loadingdatalabs/',
    's3://vdw-dev-ingest/loadingdatalabs/abcsv/',
    's3://vdw-dev-ingest/external_table_lab/'
  )
  NOTIFICATION_CHANNEL = 'arn:aws:sns:us-west-2:904233092065:udemy_et_assignment';

select current_role();

desc integration udemy_mc_et_si; -- STORAGE_AWS_EXTERNAL_ID= YJB79755_SFCRole=3_/92t2mLledON+dsTiFrMCRMQ670=
SHOW INTEGRATIONS LIKE 'UDEMY_MC_ET_SI';

----------------------
-- Stage
----------------------

--Verify the successful creation of the stage by listing the files --The file has 6 columns , select data from the stage 
CREATE OR REPLACE STAGE EXTERNALDB.ET.AWS_ET_LAB_CSV_STAGE 
STORAGE_INTEGRATION = udemy_mc_et_si 
URL = 's3://vdw-dev-ingest/external_table_lab/csv/' 
FILE_FORMAT = HRMS.HR.CSV_ET_FILEFORMAT_ASSIGNMENT; 

desc  stage AWS_ET_LAB_CSV_STAGE;

LIST @EXTERNALDB.ET.AWS_ET_LAB_CSV_STAGE; 
select * FROM EXTERNALDB.ET.LOCATION WHERE COUNTRY_ID = 'AU';
use schema hr;
show tables;
-- UNLOAD THE 1 ROW (Sydney Austrailia) 
COPY INTO @EXTERNALDB.ET.AWS_ET_LAB_CSV_STAGE FROM(SELECT * FROM 
EXTERNALDB.ET.LOCATION WHERE COUNTRY_ID ='AU') 
HEADER =TRUE 
OVERWRITE=TRUE ;

-- How many locations in each country?
-- SELECT COUNT(*),COUNTRY_ID FROM HRMS.HR.LOCATIONS 
SELECT COUNT(*),COUNTRY_ID FROM EXTERNALDB.ET.LOCATION
GROUP BY COUNTRY_ID  
order by 1 desc ;
-- Select LOCATION data from the stage via CSV parser
SELECT 
METADATA$FILENAME,
T.$1, 
-- T.$2, 
-- T.$3, 
T.$4, 
-- T.$5, 
T.$6 
FROM @EXTERNALDB.ET.AWS_ET_LAB_CSV_STAGE T; --Q5 --Create External table on the Locations data. --Provide optional parameter pattern. --Try to create the table without the file format and check if it succeeds.if it does not then go  ahead and create the ET with the file format 
--Describe the External Table and Check what is the first column in the ET --Check what is in the expression for Location_Id. 
CREATE OR REPLACE EXTERNAL TABLE EXT_LOCATIONS 
( 
LOCATION_ID    VARCHAR   AS (VALUE:c1::VARCHAR), 
STREET_ADDRESS VARCHAR   AS (VALUE:c2::VARCHAR), 
POSTAL_CODE    VARCHAR   AS (VALUE:c3::VARCHAR), 
CITY           VARCHAR   AS (VALUE:c4::VARCHAR), 
STATE_PROVINCE VARCHAR   AS (VALUE:c5::VARCHAR), 
COUNTRY_ID     VARCHAR   AS (VALUE:c6::VARCHAR) 
) 
LOCATION=@AWS_ET_LAB_CSV_STAGE 
PATTERN='.*locations.*[.]csv' 
AUTO_REFRESH= TRUE 
FILE_FORMAT = HRMS.HR.CSV_ET_FILEFORMAT_ASSIGNMENT;
 
-- select current_schema();
DESCRIBE TABLE EXTERNALDB.ET.EXT_LOCATIONS ;
SELECT * FROM EXTERNALDB.ET.EXT_LOCATIONS; 
-- DESCRIBE TABLE HRMS.HR.LOCATIONS; 
--Q6 --Select distinct file name and Max row number from the ET --Locate the MD5 values of all  the file locations_US.csv associated with the ET by selecting data from Information Schema view --Which Information Schema view will you  go to see registration history 
SELECT METADATA$FILENAME,MAX(METADATA$FILE_ROW_NUMBER) FROM 
EXT_LOCATIONS 
GROUP BY METADATA$FILENAME 
ORDER BY 2 DESC; 

SELECT * FROM 
TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES('EXT_LOCATIONS')); 
SELECT * FROM 
TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILE_REGISTRATION_HISTORY('EXT_LOCATIONS')); 
------------------------------------------
--Assignment 02 -Enable Auto-Refresh 
------------------------------------ 
--Q1 --Add 1 new file (locations_JP.csv) to the path of the External table, wait 5 mins  and check to see if the ET is able to register the file. Upload the file locations_JP.csv manually to the s3 folder,files available in resources section. s3://<student_folder_name>/external_table_lab/csv/ 
-- Do I have auto refresh on this external table?
SELECT SYSTEM$EXTERNAL_TABLE_PIPE_STATUS('EXTERNALDB.ET.EXT_LOCATIONS');
-- Observation: Yes the auto refresh is runnining despite the ET DDL not specifying AUTO_REFRESH. It is because it is already associated to the stage so when the ET is created it sets it up anyway. 
ALTER EXTERNAL TABLE EXT_LOCATIONS REFRESH;
SELECT * FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES('EXT_LOCATIONS'));


--Q2 --Manually run a refresh and check if the ET is able to register the file. What message do you  see in the status field 
ALTER EXTERNAL TABLE EXT_LOCATIONS REFRESH;  -- caused locations_JP.csv to register
SHOW EXTERNAL TABLES;
--Q3 --Enable Event Notification on s3.Add Prefix and suffix --Alter the table to make set AUTO_REFRESH to TRUE 
--Check Pipe status is running in Snowflake 
--Select the distinct METADATA$FILENAME from the External Table 
Click your bucket name -> Go to properties ->Event notifications->Create event notification-> 
Provide an Event Name -> provide Prefix(external_table_lab/csv/) -> Provide Suffix(.csv) -> select All object create 
events -> select All object removal events -> choose SQS Queue -> Enter SQS queue ARN(This can be found by describing the 
Snowflake External Table)->Save changes 
ALTER EXTERNAL TABLE EXT_EMPLOYEES_REFRESH SET AUTO_REFRESH=TRUE; 
SELECT SYSTEM$EXTERNAL_TABLE_PIPE_STATUS('EXT_LOCATIONS'); 
SELECT DISTINCT METADATA$FILENAME  FROM EXT_LOCATIONS; --Q4 --Add 2  new files(locations_IN.csv and locations_MX.csv)  to the ET folder path and remove 1  file(locations_UK.csv) and after 5 mins 
--Check to see if the files that have been added are registered and files that are removed are de-registered. --Which table will show you history of registration and de-registration 
SELECT DISTINCT METADATA$FILENAME  FROM EXT_LOCATIONS ;
SELECT DISTINCT country_id  FROM EXT_LOCATIONS ;

SELECT * FROM 
TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILE_REGISTRATION_HISTORY('EXT_LOCATIONS')); 
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

CREATE OR REPLACE STAGE 
EXTERNALDB.ET.AWS_ET_PARTITIONED_LAB_PARQUET_STAGE 
URL = 's3://vdw-dev-ingest/external_table_lab/et_partition_lab/'
STORAGE_INTEGRATION = udemy_mc_et_si
FILE_FORMAT = EXTERNALDB.ET.CSV_ET_LAB_PARQUETFORMAT;
-- List the files in the stage
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
----------------------------------
--Assignment 4 - JSON External Table and view
---------------------------------- 
-- Q1 --Create a folder in AWS  to hold JSON data files. --Upload files to the folder 
--Create a JSON stage in Snowflake  which points to the partition folder 
--List the files from the s3 folder in AWS 
--SELECT data  for all columns and examine the data 
--Create JSON External Table. 
--Use Lateral Flatten to break the JSON into multiple rows --Create a view  based on the select with the lateral flatten,ensure that double quotes are removed. 
--s3://<student_folder_name>/external_table_lab/et_partition_lab/JSON/ 
-- create json format
CREATE OR REPLACE FILE FORMAT EXTERNALDB.ET.JSON_ET_LAB_FILEFORMAT 
TYPE = JSON; 
-- Create stage for json
CREATE OR REPLACE STAGE EXTERNALDB.ET.AWS_ET_LAB_JSON_STAGE 
STORAGE_INTEGRATION = udemy_mc_et_si -- AWS_S3_INT 
URL = 's3://vdw-dev-ingest/external_table_lab/json/' 
FILE_FORMAT = JSON_ET_LAB_FILEFORMAT; 

SHOW STORAGE INTEGRATIONS LIKE 'UDEMY_MC_ET_SI';

-- Check that stage is working
LIST @AWS_ET_LAB_JSON_STAGE; 
SELECT * FROM  @AWS_ET_LAB_JSON_STAGE; 
SELECT $1 FROM  @AWS_ET_LAB_JSON_STAGE; 
-- Create external table on json files
CREATE OR REPLACE EXTERNAL TABLE EXT_LOC_JSON 
( 
LOC_JSON_DATA VARIANT AS (VALUE::VARIANT) 
) 
WITH LOCATION = @AWS_ET_LAB_JSON_STAGE 
FILE_FORMAT = JSON_ET_LAB_FILEFORMAT; 
-- Check that external table is working
SELECT * FROM EXT_LOC_JSON; 
-- CREATE VIEW ON FLATTENED LOCATION DATA 
CREATE OR REPLACE VIEW VIEW_EXT_LOC_JSON 
AS 
SELECT 
Y.VALUE:LOCATION_ID:: INTEGER       AS LOCATION_ID, 
Y.VALUE:STREET_ADDRESS :: VARCHAR   AS STREET_ADDRESS , 
Y.VALUE:POSTAL_CODE :: VARCHAR      AS POSTAL_CODE, 
Y.VALUE:CITY:: VARCHAR              AS CITY, 
Y.VALUE:STATE_PROVINCE:: VARCHAR    AS STATE_PROV, Y.VALUE:COUNTRY_ID:: VARCHAR        AS COUNTRY_ID
FROM EXT_LOC_JSON ELJ, 
lateral flatten( input =>LOC_JSON_DATA) Y; 
SELECT * FROM VIEW_EXT_LOC_JSON; 
------------------------------------------
--Assignment 05 -Streams on External Table 
------------------------------------------ 
--Create a folder in AWS  to hold parquet Streams files. 
--Upload files to the folder 
--Create a CSV stage in Snowflake  which points to the parquet Streams folder 
--List the files from the s3 folder in AWS 
--SELECT data  for all columns and examine the data 
--Create an External Table based on the files added. 
--SELECT data  from the External Table. 
--Create folder s3://<student_folder_name>/external_table_lab/et_partition_lab/streams/ 
-- File format (parquet)
CREATE OR REPLACE FILE FORMAT EXTERNALDB.ET.PARQUET_ET_LAB_FILEFORMAT 
TYPE = PARQUET 
COMPRESSION=NONE; 
-- Stage
CREATE OR REPLACE STAGE EXTERNALDB.ET.AWS_ET_STREAMS_LAB_STAGE 
STORAGE_INTEGRATION = udemy_mc_et_si
URL = 's3://vdw-dev-ingest/external_table_lab/streams/' 
DIRECTORY = (ENABLE = true AUTO_REFRESH = true)
FILE_FORMAT = PARQUET_ET_LAB_FILEFORMAT; 

SHOW STORAGE INTEGRATIONS LIKE 'UDEMY_MC_ET_SI';
SHOW STAGES LIKE 'AWS_ET_STREAMS_LAB_STAGE';

---------------
-- Debug storage integration
---------------
-- CREATE OR REPLACE STORAGE INTEGRATION udemy_mc_et_si2
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = S3
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
--   STORAGE_ALLOWED_LOCATIONS = (
--     's3://vdw-dev-ingest/external_table_lab/',
--     's3://vdw-dev-ingest/loadingdatalabs/',
--     's3://vdw-dev-ingest/loadingdatalabs/abcsv/'
--   )
--   NOTIFICATION_CHANNEL = 'arn:aws:sns:us-west-2:904233092605:udemy_et_assignment';  -- This fails so may not be available in my trial account


-- Query the stage to make sure its working
LIST @EXTERNALDB.ET.AWS_ET_STREAMS_LAB_STAGE; 
SELECT metadata$filename, s.* FROM @AWS_ET_STREAMS_LAB_STAGE s; 
-- Create external table
CREATE OR REPLACE EXTERNAL TABLE EXT_LOCATIONS_STREAMS 
( 
LOCATION_ID        VARCHAR   AS ($1:LOCATION_ID::VARCHAR), 
STREET_ADDRESS     VARCHAR   AS ($1:STREET_ADDRESS::VARCHAR), 
POSTAL_CODE        VARCHAR   AS ($1:POSTAL_CODE::VARCHAR), 
CITY               VARCHAR   AS ($1:CITY::VARCHAR), 
STATE_PROVINCE     VARCHAR   AS ($1:STATE_PROVINCE::VARCHAR), 
COUNTRY_ID         VARCHAR   AS ($1:COUNTRY_ID::VARCHAR) 
) 
LOCATION=@AWS_ET_STREAMS_LAB_STAGE 
PATTERN='.*locations.*[.]parquet' 
AUTO_REFRESH = TRUE
FILE_FORMAT = PARQUET_ET_LAB_FILEFORMAT; 

select * from EXT_LOCATIONS_STREAMS; -- <-- looking for country IN and JP and they are not coming through

show external tables like 'EXT_LOCATIONS_STREAMS';
-- Is auto refresh working? 
SELECT SYSTEM$EXTERNAL_TABLE_PIPE_STATUS('EXT_LOCATIONS_STREAMS');


-- Query external table to make sure it works
SELECT * FROM EXT_LOCATIONS_STREAMS ;
-- Create stream on external table
CREATE OR REPLACE STREAM STREAM_EXT_LOCATIONS ON EXTERNAL TABLE 
EXT_LOCATIONS_STREAMS 
INSERT_ONLY = TRUE; 
-- Query the stream
SELECT * FROM STREAM_EXT_LOCATIONS; 
-- Manually refresh data registration
ALTER EXTERNAL TABLE EXT_LOCATIONS_STREAMS REFRESH; 
SELECT * FROM STREAM_EXT_LOCATIONS;