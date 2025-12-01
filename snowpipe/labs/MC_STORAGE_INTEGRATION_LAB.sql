
CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::133202620729:role/SNOWFLAKE-S3-FULL-ACCESS'
STORAGE_ALLOWED_LOCATIONS = ('s3://learn2cloud-snowflake/')
STORAGE_BLOCKED_LOCATIONS = ('s3://learn2cloud-snowflake/secret_folder/')
COMMENT = 'Create Storage integration to connect Snowflake with AWS' 




;


ALTER STORAGE INTEGRATION AWS_S3_INT SET ENABLED = TRUE;

ALTER STORAGE INTEGRATION AWS_S3_INT 
SET STORAGE_ALLOWED_LOCATIONS = ('S3://LOCATION1', 
                                 'S3://LOCATION2',                                
                                 'S3://LOCATION4',
                                 'S3://LOCATION5',
                                 'S3://LOCATION6',
                                 'S3://LOCATION7',
                                 'S3://LOCATION8',
                                 'S3://LOCATION9',
                                 'S3://LOCATION10');


ALTER STORAGE INTEGRATION AWS_S3_INT SET STORAGE_ALLOWED_LOCATIONS = ('s3://learn2cloud-snowflake/');

SHOW INTEGRATIONS;

DESCRIBE INTEGRATION AWS_S3_INT;


USE DATABASE HRMS;

CREATE OR REPLACE SCHEMA ETL;
USE SCHEMA ETL;
CREATE OR REPLACE FILE FORMAT HRMS.ETL.CSV_ETL_FILEFORMAT
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 0
NULL_IF = ('Null','NULL')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'        
TRIM_SPACE=TRUE
COMPRESSION=AUTO;



DESCRIBE FILE FORMAT HRMS.ETL.CSV_ETL_FILEFORMAT;


SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();

CREATE OR REPLACE STAGE HRMS.ETL.AWS_ETL_CSV_STAGE
STORAGE_INTEGRATION = udemy_mc_a1_si
URL = 'S3://vdw-dev-ingest/loadingdatalabs/labcsv/'
FILE_FORMAT = CSV_ETL_FILEFORMAT;


SHOW STAGES;


DESCRIBE STAGE HRMS.ETL.AWS_ETL_CSV_STAGE;


LIST @HRMS.ETL.AWS_ETL_CSV_STAGE;

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