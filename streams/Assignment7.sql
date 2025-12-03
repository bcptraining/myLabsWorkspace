-- Assignment 7: Streams in Snowflake
-- This file demonstrates working with Snowflake Streams
-----------------
--  Question 1: Create Stream on a Empty Table, populate the table with 10 rows and check the stream for data.
-----------------
-- Step 1: Create an Empty CUSTOMER Table from the Snowflake Sample Data.
CREATE OR REPLACE TABLE CUSTOMER (
    CUSTOMER_ID INT,
    FULL_NAME VARCHAR(100),
    PHONE_NUM VARCHAR(15),
    EMAIL VARCHAR(50)
);
-- Step 2: Create a Stream object on the table CUSTOMER..
drop stream if exists customer_stream;
CREATE or replace STREAM customer_stream ON TABLE CUSTOMER;
-- Step 3: Insert 10 rows from the Sample Snowflake Customer table to the CUSTOMER_STREAM table.
INSERT INTO CUSTOMER (CUSTOMER_ID, FULL_NAME, PHONE_NUM, EMAIL)
VALUES
  (1, 'Joe Piscapo', '(999)-999-9999', 'jpiscapo@gmail.com'),
  (2, 'Linda Marquez', '(212)-555-1234', 'lmarquez@example.com'),
  (3, 'Rajiv Patel', '(646)-888-4567', 'rajiv.patel@demo.org'),
  (4, 'Emily Zhang', '(310)-777-7890', 'emily.zhang@fauxmail.com'),
  (5, 'Carlos Rivera', '(415)-222-3456', 'crivera@sample.net'),
  (6, 'Fatima Noor', '(718)-333-6789', 'fatima.noor@mockmail.com'),
  (7, 'James O’Connor', '(617)-444-9876', 'joconnor@placeholder.io'),
  (8, 'Aisha Thompson', '(202)-555-2345', 'aisha.thompson@fakemail.com'),
  (9, 'Kenji Nakamura', '(503)-666-4321', 'kenji.nakamura@demo.co'),
  (10, 'Sophie Dubois', '(438)-777-1111', 'sophie.dubois@sample.ca');



-- Step 4: Select data from the stream and count the number of rows that are displayed.
select * from customer_stream; -- 10 ROWS (INSERTS)

--Step 5:Create a new Empty table called  CUSTOMER_DIM of the same structure as the CUSTOMER table.
CREATE OR REPLACE TABLE CUSTOMER_DIM (
    CUSTOMER_ID INT,
    FULL_NAME VARCHAR(100),
    PHONE_NUM VARCHAR(15),
    EMAIL VARCHAR(50)
);
-- Step 6: Write a MERGE statement which will MERGE data from CUSTOMER_STREAM to CUSTOMER_DIM, count rows inserted
MERGE INTO CUSTOMER_DIM AS target
USING customer_stream AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID

WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' 
               AND source.METADATA$ISUPDATE = TRUE
  THEN UPDATE SET target.FULL_NAME  = source.FULL_NAME,
                  target.PHONE_NUM  = source.PHONE_NUM,
                  target.EMAIL      = source.EMAIL

WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' 
               AND source.METADATA$ISUPDATE = FALSE
  THEN DELETE

WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT'
  THEN INSERT (CUSTOMER_ID, FULL_NAME, PHONE_NUM, EMAIL)
       VALUES (source.CUSTOMER_ID, source.FULL_NAME, source.PHONE_NUM, source.EMAIL);


-- Step 7:Update 1 row in the CUSTOMER table and see the impact of the same in the stream ,What is the value of METADATA$ACTION  and METADATA$ISUPDATE  in the stream.
select * from CUSTOMER_DIM;
 UPDATE CUSTOMER SET PHONE_NUM = '(888)-888-8886' WHERE CUSTOMER_ID = 2;    
 select * from customer_stream;
 select count(*) as ROWS_IN_STREAM_AFTER_UPDATE from customer_stream; -- 2 ROWs in stream now 

--  Step 8: Re-run the MERGE, what happens    
-- The row is updated in the CUSTOMER_DIM table and the stream is cleared.
-- Step 9:Verify that the stream is Empty without selecting data from the Stream
SELECT SYSTEM$STREAM_HAS_DATA('customer_stream') AS has_data;

-----------------
--  Question 2:  Create a Stream on a view
-----------------
-- Step 1: Create a view which has data from Customer and Nation
create or replace view customer_location_view as
select c.CUSTOMER_ID, c.FULL_NAME, c.PHONE_NUM, c.EMAIL, 1100 as LOCATION_ID, l.country_id as NATION
from customer c
join locations l on 1100 = l.location_id;
select * from customer_location_view;
-- Step 2:Create a Stream on the view.
create OR REPLACE stream customer_location_stream on view customer_location_view;
-- Step 3: Update the  customer table and see if there is data in the Stream
update customer set PHONE_NUM='(777)-777-6666' where CUSTOMER_ID=1; 
select * from customer_location_stream; -- 2 ROWS (1 UPDATE on CUSTOMER table reflected as DELETE + INSERT in the stream)
-- Step 4: Write a Merge statement to consume data from the Stream  and merge it into a new Table called CUSTOMER_NATION_DIM.
CREATE OR REPLACE TABLE CUSTOMER_NATION_DIM (
    CUSTOMER_ID INT,
    FULL_NAME VARCHAR(100),
    PHONE_NUM VARCHAR(15),
    EMAIL VARCHAR(50),
    LOCATION_ID INT,
    NATION VARCHAR(10)
);

-- select * from CUSTOMER_NATION_DIM;
MERGE INTO CUSTOMER_NATION_DIM AS target
USING customer_location_stream AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID

WHEN MATCHED AND source.METADATA$ACTION = 'INSERT'
               AND source.METADATA$ISUPDATE = TRUE
  THEN UPDATE SET target.FULL_NAME   = source.FULL_NAME,
                  target.PHONE_NUM   = source.PHONE_NUM,
                  target.EMAIL       = source.EMAIL,
                  target.LOCATION_ID = source.LOCATION_ID,
                  target.NATION      = source.NATION

WHEN MATCHED AND source.METADATA$ACTION = 'DELETE'
               AND source.METADATA$ISUPDATE = FALSE
  THEN DELETE

WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT'
  THEN INSERT (CUSTOMER_ID, FULL_NAME, PHONE_NUM, EMAIL, LOCATION_ID, NATION)
       VALUES (source.CUSTOMER_ID, source.FULL_NAME, source.PHONE_NUM, source.EMAIL,
               source.LOCATION_ID, source.NATION);

-- Step 5: Update the Nation Table and check to see if there is data in the Stream
update locations set country_id='US' where location_id=1100;
select * from customer_location_stream; -- 2 ROWS (1 UPDATE on LOCATION table reflected as DELETE + INSERT in the stream)
-- Step 6: Run Merge to Merge the data from the Stream into the CUSTOMER_NATION_DIM table
select * from CUSTOMER_NATION_DIM;
----------------
--  Question 3:  Create a Task that will run when there is data in the Stream
-----------------
-- Step 1: Insert a row in the Customer table and check the view stream  from Q2 to see if has data in it.
INSERT INTO CUSTOMER (CUSTOMER_ID, FULL_NAME, PHONE_NUM, EMAIL)
VALUES
  (11, 'Joe Piscapo Jr.', '(999)-999-9999', 'jpiscapo@gmail.com');
  select * from customer_location_stream; -- 1 ROW (INSERT)
-- Step 2:Create a Task which polls the Stream to check to see if there is data and creates a table from it.
-- Create a task that runs on a schedule and consumes the stream
CREATE OR REPLACE TASK customer_location_task
  WAREHOUSE = compute_wh
  SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('customer_location_stream')
AS
MERGE INTO CUSTOMER_NATION_DIM AS target
USING customer_location_stream AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID

WHEN MATCHED AND source.METADATA$ACTION = 'INSERT'
               AND source.METADATA$ISUPDATE = TRUE
  THEN UPDATE SET target.FULL_NAME   = source.FULL_NAME,
                  target.PHONE_NUM   = source.PHONE_NUM,
                  target.EMAIL       = source.EMAIL,
                  target.LOCATION_ID = source.LOCATION_ID,
                  target.NATION      = source.NATION

WHEN MATCHED AND source.METADATA$ACTION = 'DELETE'
               AND source.METADATA$ISUPDATE = FALSE
  THEN DELETE

WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT'
  THEN INSERT (CUSTOMER_ID, FULL_NAME, PHONE_NUM, EMAIL, LOCATION_ID, NATION)
       VALUES (source.CUSTOMER_ID, source.FULL_NAME, source.PHONE_NUM, source.EMAIL,
               source.LOCATION_ID, source.NATION);


alter task customer_location_task resume;
execute task customer_location_task;
select * from CUSTOMER_NATION_DIM;
----------------
--  Question 4: Create an Append Only Stream
-----------------
-- Step 1: Create an Append only Stream on the NATION's table
CREATE OR REPLACE STREAM STREAM_LOCATIONS_APPEND_ONLY
    ON TABLE LOCATIONS
    APPEND_ONLY = TRUE;

-- Step 2: Insert a row into the table

INSERT INTO LOCATIONS (
  LOCATION_ID,
  STREET_ADDRESS,
  POSTAL_CODE,
  CITY,
  STATE_PROVINCE,
  COUNTRY_ID
)
VALUES (
  3200,
  '1641 9 St SE',
  'V1E 0G9',
  'Salmon Arm',
  'BC',
  'CA'
);

-- Step 3: Review the Stream to check if data is present
select * from STREAM_LOCATIONS_APPEND_ONLY; -- yes, data is present
-- Step 4: Consume the stream so that it is empty
DROP STREAM customer_location_stream;
CREATE or replace STREAM customer_location_stream_append_only 
ON TABLE locations
append_only = TRUE;
select * from customer_location_stream;

-- Step 5: Update a row in the Nations table and check the stream for data, do you see any data.
UPDATE LOCATIONS
SET COUNTRY_ID = 'CC'
WHERE LOCATION_ID = 3200;

select * from customer_location_stream_append_only ;-- No data is seen in the stream as it is an append only stream

----------------
--  Question 5: Enable Change Tracking on a table and capture Tracked Changes
-----------------
-- Step 1:Create table Region from Snowflake Sample Data.

create or replace table REGION  as 
select * from snowflake_sample_data.tpch_sf1.region;

select * from REGION;
-- Step 2: Enable Change tracking on the newly created table
alter table REGION set CHANGE_TRACKING = true;
-- Step 3: Save Timestamp as TS1
DECLARE TS1 TIMESTAMP;
DECLARE TS2 TIMESTAMP;
DECLARE TS3 TIMESTAMP;

SET TS1 = current_timestamp();


-- Step 4: Insert a row into the REGION table
-- desc table region;
insert into REGION (R_REGIONKEY, R_NAME, R_COMMENT) values (6, 'KRAKEN NATION', 'Region located in the pacific northwest of the United States.');
-- -- Step 5: Save Timestamp as TS2
set TS2 = current_timestamp();
-- -- Step 6:Update the R_REGION_KEY=1 and change AMERICA to 'AMERIKA'
SELECT $TS1 AS TSA, $TS2 AS TS2;;
-- update REGION set R_NAME='AMERIKA' where R_REGIONKEY=1;
-- Step 7:Save Timestamp as TS3
set TS3 = current_timestamp();
-- Step 8: Select from Region such that data is visible without any data in the table
-- Show changes between TS1 and TS2 (the insert)
-- Show changes between two timestamps
SELECT *
FROM REGION CHANGES(INFORMATION => DEFAULT)
AT (TIMESTAMP => $TS1)
END (TIMESTAMP => $TS2);

-- Step 9:  Select from Region such that data is visible after a new row has been inserted
SELECT *
FROM REGION CHANGES(INFORMATION => APPEND_ONLY)
AT (TIMESTAMP => $TS1);

-- Step 10: Create a table from the Select in Step 9 and see if the data is consumed, what do you see. (DATA NOT CONSUMED)
create or replace table REGION_TEST_FOR_CONSUMPTION  as 
SELECT *
FROM REGION CHANGES(INFORMATION => APPEND_ONLY)
AT (TIMESTAMP => $TS1);
SELECT * FROM REGION_TEST_FOR_CONSUMPTION;
