use database hrms;
use schema pipe;
-- Create Sales Table
CREATE OR REPLACE TABLE sales 
(
    OrderDate     DATE,
    Category      VARCHAR(20),
    City          VARCHAR(50),
    Country       VARCHAR(50),
    CustomerName  VARCHAR(100),
    Discount      NUMBER(3,2),
    OrderID       VARCHAR(20),
    PostalCode    VARCHAR(10),
    Product       VARCHAR(100),
    Profit        NUMBER(10,2),
    Quantity      NUMBER(10),
    Region        VARCHAR(50),
    Sales         NUMBER(10,2),
    Segment       VARCHAR(50),
    ShipDate      DATE,
    ShipMode      VARCHAR(50),
    State         VARCHAR(50),
    SOURCE_FILE_NAME       VARCHAR(500),
    SOURCE_FILE_ROW_NUMBER NUMBER(38,0),
    LOAD_TIMESTAMP         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);



--File Format
create or replace file format sales_file_format
type = csv
field_delimiter = ','   
skip_header = 1
;

show file formats;
-- Stage
create OR REPLACE stage sales_stage
    file_format = (format_name = sales_file_format)
    URL = 's3://vdw-dev-ingest/loadingdatalabs/snowpipe/csv/'
    storage_integration = udemy_mc_a1_si;
    PATTERN = '.*sales.*\\.csv';  -- <== Not allowed in the stage

  LIST @sales_stage;

-- Pipe
--  COPY INTO sales
CREATE OR REPLACE PIPE sales_pipe
  AUTO_INGEST = TRUE        
AS
COPY INTO sales
(
  OrderDate, Category, City, Country, CustomerName, Discount, OrderID,
  PostalCode, Product, Profit, Quantity, Region, Sales, Segment,
  ShipDate, ShipMode, State,
  SOURCE_FILE_NAME, SOURCE_FILE_ROW_NUMBER
)
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7,
         t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14,
         t.$15, t.$16, t.$17,
         METADATA$FILENAME AS SOURCE_FILE_NAME,
         METADATA$FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
  FROM @sales_stage t
)
FILE_FORMAT = (format_name = sales_file_format)
PATTERN = '.*sale.*\\.csv$';


--  Use the copy statement to reload after setting up a new Snowflake Trial  account 
COPY INTO sales
(
  OrderDate, Category, City, Country, CustomerName, Discount, OrderID,
  PostalCode, Product, Profit, Quantity, Region, Sales, Segment,
  ShipDate, ShipMode, State,
  SOURCE_FILE_NAME, SOURCE_FILE_ROW_NUMBER
)
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7,
         t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14,
         t.$15, t.$16, t.$17,
         METADATA$FILENAME AS SOURCE_FILE_NAME,
         METADATA$FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
  FROM @sales_stage t
)
FILE_FORMAT = (FORMAT_NAME = sales_file_format)
PATTERN = '.*sale.*\\.csv$';



  desc pipe sales_pipe;


  -- Debug Pipe: Step 1.  Get Pipe Status (can see name of last file ingested);
  select system$pipe_status('sales_pipe');

-- Debug: Step 2.  File-level reconciliation -- Check COPY history for your target table (3 hr lag)
-- Example: I can see that sales01.csv and sales02.cs4 had ROW_COUNT= 0  and ERROR_COUNT of 10 and 20 respectively, and sales1_reload.csv and sales2_reload.csv had ROW_COUNT of 10 and 20 respectively with ERROR_COUNT of 0
SELECT FILE_NAME, LAST_LOAD_TIME, ROW_COUNT, ERROR_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_NAME = 'SALES'
ORDER BY LAST_LOAD_TIME DESC;

-- Debug: Step 2 Alternative.  File-level reconciliation -- Check COPY history for your target table (no lag)
SELECT *
FROM TABLE(
    HRMS.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'sales',
        START_TIME => DATEADD(HOUR, -4, CURRENT_TIMESTAMP),
        END_TIME   => CURRENT_TIMESTAMP
    )
);

  
-- Row-level Reconciliation
SELECT $1 AS OrderDate,
       $2 AS Category,
       $3 AS City,
       $4 AS Country,
       $5 AS CustomerName,
       $6 AS Discount,
       $7 AS OrderID,
       $8 AS PostalCode,
       $9 AS Product,
       $10 AS Profit,
       $11 AS Quantity,
       $12 AS Region,
       $13 AS Sales,
       $14 AS Segment,
       $15 AS ShipDate,
       $16 AS ShipMode,
       $17 AS State
FROM @sales_stage
(FILE_FORMAT => sales_file_format, PATTERN => '.*sales03\\.csv');


  -- Debug: Step 3.  Validate files ingested by the pipe in last 4 hours

SELECT *
FROM TABLE(
  VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'sales_pipe',
    START_TIME => DATEADD(HOUR, -80, CURRENT_TIMESTAMP)
  )
);

select * from hrms.pipe.sales order by orderid desc;
SELECT COUNT(*)
FROM hrms.pipe.sales;



  -- Check ingestion jobs for your pipe (cost by pipe by time window)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE PIPE_NAME = 'SALES_PIPE'
ORDER BY START_TIME DESC;

-- Check which files were copied into the target table
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_NAME = 'SALES'
ORDER BY LAST_LOAD_TIME ;


-- Audit view
SELECT 
    CASE 
      WHEN FILE_NAME LIKE '%reload%' THEN 'Reloaded'
      ELSE 'Original'
    END AS Load_Type,
    FILE_NAME,
    LAST_LOAD_TIME,
    ROW_COUNT,
    ERROR_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_NAME = 'SALES'
  AND FILE_NAME IN ('sales1.csv','sales2.csv','sales1_reload.csv','sales2_reload.csv')
ORDER BY LAST_LOAD_TIME;
