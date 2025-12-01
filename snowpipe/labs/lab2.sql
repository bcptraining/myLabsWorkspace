--  Set Context
USE DATABASE HRMS;
create schema pipe;
USE SCHEMA PIPE;


-- Create storage integration
CREATE or replace STORAGE INTEGRATION udemy_mc_a1_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/',
  's3://vdw-dev-ingest/loadingdatalabs/json/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/',
  's3://vdw-dev-ingest/loadingdatalabs/snowpipe/json/');

  DESC INTEGRATION udemy_mc_a1_si;

  -- Create File Format
CREATE OR REPLACE FILE FORMAT HRMS.PIPE.CSV_PIPE_FILEFORMAT
TYPE = CSV
FIELD_DELIMITER = ","
SKIP_HEADER = 0
NULL_IF = ('NULL', 'Null', 'null')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
EMPTY_FIELD_AS_NULL = TRUE;

-- Create Stage
CREATE OR REPLACE STAGE HRMS.PIPE.AWS_PIPE_CSV_STAGE
STORAGE_INTEGRATION = udemy_mc_a1_si
URL = 's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/'
FILE_FORMAT = "CSV_PIPE_FILEFORMAT";

LIST @HRMS.PIPE.AWS_PIPE_CSV_STAGE;
list @AWS_PIPE_CSV_STAGE;

--  Target Table
CREATE OR REPLACE TABLE HRMS.PIPE.EMPLOYEES_SNOWPIPE_CSV_ERROR
(
EMPLOYEE_ID NUMBER(6),
FIRST_NAME VARCHAR(20),
LAST_NAME VARCHAR(25),
EMAIL VARCHAR(25),
PHONE_NUMBER VARCHAR(20),
HIRE_DATE DATE,
JOB_ID VARCHAR(10),
SALARY NUMBER(8,2),
COMMISSION_PCT NUMBER(2,2),
MANAGER_ID NUMBER(6),
DEPARTMENT_ID NUMBER(4)
);

-- Create and explore pipes and metadata
CREATE OR REPLACE PIPE PIPE_EMPLOYEES_CSV_ERROR
AUTO_INGEST=TRUE
AS
COPY INTO EMPLOYEES_SNOWPIPE_CSV_ERROR 
FROM @AWS_PIPE_CSV_STAGE
pattern='.*_ERROR\.csv';

SHOW PIPES;
desc pipe PIPE_EMPLOYEES_CSV_ERROR;
SHOW PIPES LIKE 'PIPE_EMPLOYEES_CSV_ERROR';
-- Debug Pipe: Step 1.  Get Pipe Status (can ess name of last file ingested); 
SELECT SYSTEM$PIPE_STATUS ('PIPE_EMPLOYEES_CSV_ERROR');
-- {"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2025-11-20T20:56:43.334Z","lastIngestedFilePath":"employees_part_006_ERROR.csv","notificationChannelName":"arn:aws:sqs:us-west-2:560088568248:sf-snowpipe-AIDAYEZ7BKG4MS357RHPH-jPU43ssLSTUMQ6YPSwKP2g","numOutstandingMessagesOnChannel":2,"lastReceivedMessageTimestamp":"2025-11-20T20:56:42.983Z","lastForwardedMessageTimestamp":"2025-11-20T20:56:43.828Z","lastPulledFromChannelTimestamp":"2025-11-20T20:59:18.106Z","lastForwardedFilePath":"vdw-dev-ingest/loadingdatalabs/snowpipe/csv/employees_part_006_ERROR.csv","pendingHistoryRefreshJobsCount":0}

-- Debug Pipe Step 2 LIST FAILED LOAD FILES (only shows first error)
SELECT *
FROM TABLE(
    HRMS.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'EMPLOYEES_SNOWPIPE_CSV_ERROR',
        START_TIME => DATEADD(HOUR, -4, CURRENT_TIMESTAMP),
        END_TIME   => CURRENT_TIMESTAMP
    )
);
-- Debug Pipe Step 3:  Validate Pipe Load Errors (see all the errors)

SELECT *
FROM TABLE(
  VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'PIPE_EMPLOYEES_CSV_ERROR',
    START_TIME => DATEADD(HOUR, -4, CURRENT_TIMESTAMP)
  )
);

-- Another confirmation (takes longer)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE PIPE_NAME = 'HRMS.PIPE.PIPE_EMPLOYEES_CSV_ERROR'
  AND LAST_LOAD_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP);


-- Verify row counts: 
-- Count rows in the target table
SELECT COUNT(*) FROM HRMS.PIPE.EMPLOYEES_SNOWPIPE_CSV_ERROR;

-- Compare with COPY_HISTORY
SELECT FILE_NAME, ROW_COUNT
FROM TABLE(
    HRMS.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'EMPLOYEES_SNOWPIPE_CSV_ERROR',
        START_TIME => DATEADD(HOUR, -4, CURRENT_TIMESTAMP),
        END_TIME   => CURRENT_TIMESTAMP
    )
);



-------------------
-- Snowpipe Costs and Monitoring
------------------- 
-- INFORMATION_SCHEMA.PIPE_USAGE_HISTORY (max 14 days of data)
-- ACCOUNT_USAGE.PIPE_USAGE_HISTORY stored data for 365 days, however there is a lag of up to 3 hours before data shows up.
-- Credits per date range (14 days max)
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
DATE_RANGE_START=>TO_TIMESTAMP_TZ('2025-11-15 00:00:00.000 -0700'),
DATE_RANGE_END=>TO_TIMESTAMP_TZ('2025-11-21 00:00:00.000 -0700')));
-- Credits last 14d full days (not including current day)
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
DATE_RANGE_START=>DATEADD('DAY',-14,CURRENT_DATE()),
DATE_RANGE_END=>CURRENT_DATE()));

-- Credits last 12 hours UTC
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
DATE_RANGE_START=>DATEADD('HOUR',-12,CURRENT_DATE())));

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
oRDER BY 3 DESC;

