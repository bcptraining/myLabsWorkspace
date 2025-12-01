-- Question 1: Create and Manage a Task in Snowflake
-- Step 1:Create a Task that runs every 2 hours
CREATE OR REPLACE TASK task_runs_every_2hrs
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */2 * * * UTC'
AS
  SELECT 'Task runs every 2 hours';
-- Step 2List all tasks in my account to see the task you created
  show tasks;
-- Step 3:Describe the task created in Step 1 (DESC and SHOW have same outoput)
  desc task task_runs_every_2hrs; 
  show tasks like 'task_runs_every_2hrs';
-- Step 4:Check the task to see if it is active or suspended, what do you see
  show tasks like 'task_runs_every_2hrs'; -- It was suspended initially
  -- Step 5 Change the state of the Task if it is suspended
  alter task task_runs_every_2hrs resume;
  -- Step 6:Check the Task DDL and check to see if it matches the Syntax you used.
   SELECT GET_DDL('TASK', 'task_runs_every_2hrs');


-- Question 2 Create a Task and schedule it to run Mon, Sat and Sunday at 4:00 am in Tokyo Japan Timezone
CREATE OR REPLACE TASK task_runs_mon_sat_sun_4am_tokyo
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 4 * * 0,1,6 Asia/Tokyo'
AS
  SELECT 'Task runs Mon, Sat and Sun at 4:00 am Tokyo Timezone';

desc task task_runs_mon_sat_sun_4am_tokyo;
drop task task_runs_mon_sat_sun_4am_tokyo;

-- Question 3: Create a Task without a schedule
CREATE OR REPLACE TASK task_without_schedule
  WAREHOUSE = COMPUTE_WH
  as 
  SELECT 'Task without a schedule';
  desc task task_without_schedule; -- schedule = null
  execute task task_without_schedule; -- Manually execute the task
  drop task task_without_schedule;
-- Question 4: Create a task without a schedule or warehouse
CREATE OR REPLACE TASK task_without_schedule_warehouse
  as 
  SELECT 'Task without a schedule or warehouse';
  desc task task_without_schedule_warehouse; -- schedule = null, warehouse = null
  -- execute task task_without_schedule_warehouse; -- success (it runs serverless)
  alter task task_without_schedule_warehouse set warehouse = COMPUTE_WH;
  execute task task_without_schedule_warehouse; -- Now it works
  drop task task_without_schedule_warehouse;
  CREATE OR REPLACE TASK task_without_schedule_warehouse 
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = XLARGE
  as 
  SELECT 'Task without a schedule or warehouse';

drop task task_without_schedule_warehouse;

-- Q5:Create a stored Proc and call the Stored Proc inside the Task
CREATE OR REPLACE PROCEDURE my_stored_proc()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var sql_command = `
        SELECT 
            'User: ' || CURRENT_USER() ||
            ' | Role: ' || CURRENT_ROLE() ||
            ' | Database: ' || CURRENT_DATABASE() ||
            ' | Schema: ' || CURRENT_SCHEMA() AS session_info
    `;
    
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var rs = stmt.execute();
    rs.next();
    var session_info = rs.getColumnValue(1);

    // You can return any string you want here
    return "2 tables created | " + session_info;
$$;
-- Q6:Create a Tree or Hierarchy of Tasks, Execute , monitor and display dependencies
-- Step 1: Create root task
create task task_calls_stored_proc
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */3 * * * UTC'
  as
  call my_stored_proc();    
desc task task_calls_stored_proc;
alter task task_calls_stored_proc resume;
execute task task_calls_stored_proc;

alter task task_calls_stored_proc suspend;
-- Step 2: Create 2 child rasks
create  task ct1
  WAREHOUSE = COMPUTE_WH
  after  task_calls_stored_proc
  as
  select 'Task 1 executed';

  create  task ct2
  WAREHOUSE = COMPUTE_WH
  after  task_calls_stored_proc
  as
  select 'Task 2 executed';

-- Step 3: Create 2 grand child tasks
  create  task gct1
  WAREHOUSE = COMPUTE_WH
  after  ct1 
  as
  select 'Grand Child Task 1 executed';

create task gcT2 
after ct2
as 
select "gct2 executed";

-- Step 4 : Resume the root task and any suspended tasks in the dag
SELECT * FROM TABLE(GENERATE_TASK_RESUME_SQL('HRMS.PUBLIC.TASK_CALLS_STORED_PROC'));

-- Step 4:  Resume the suspended tasks for the dag
ALTER TASK HRMS.PUBLIC.GCT2 RESUME;
ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC RESUME;

--Step 5: Execute the Task and Monitor the Task
execute task HRMS.PUBLIC.TASK_CALLS_STORED_PROC;
--  Change schedule to run every 5 min

ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC suspend;
ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC resume;
alter task HRMS.PUBLIC.FINALIZE_TASK_CALLS resume;

ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC
SET SCHEDULE = '5 MINUTE';


-- Scheduled task runs
SHOW TASKS LIKE 'TASK_CALLS_STORED_PROC' IN SCHEMA HRMS.PUBLIC;

desc task HRMS.PUBLIC.TASK_CALLS_STORED_PROC;
use database hrms;
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'HRMS.PUBLIC.TASK_CALLS_STORED_PROC',
    RESULT_LIMIT => 100
  )
)
ORDER BY SCHEDULED_TIME DESC;
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'HRMS.PUBLIC.TASK_CALLS_STORED_PROC',
    RESULT_LIMIT => 100
  )
)
ORDER BY SCHEDULED_TIME DESC;
SHOW TASKS LIKE 'TASK_CALLS_STORED_PROC' IN SCHEMA HRMS.PUBLIC;

-- Step 6: Display dependencies
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'HRMS.PUBLIC.TASK_CALLS_STORED_PROC',
    RECURSIVE => TRUE
  )
)
ORDER BY CREATED_ON;
show warehouses; -- AUTO_RESUME = TRUE
--Q7: Use the FINALIZE TASK 
-- Create the log table
CREATE OR REPLACE TABLE HRMS.PUBLIC.TASK_LOG (
    LOG_ID          NUMBER AUTOINCREMENT,          -- unique identifier
    TASK_NAME       STRING NOT NULL,               -- name of the task that ran
    RUN_TIME        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP, -- when the finalize step logged
    STATUS          STRING,                        -- e.g. 'completed', 'failed'
    QUERY_ID        STRING,                        -- optional: link back to QUERY_HISTORY
    MESSAGE         STRING                         -- optional: custom notes or error text
);


select * from HRMS.PUBLIC.TASK_LOG;

alter task HRMS.PUBLIC.TASK_CALLS_STORED_PROC resume;
CREATE OR REPLACE TASK HRMS.PUBLIC.TASK_FINALIZE_CALLS
WAREHOUSE = compute_wh
AFTER HRMS.PUBLIC.TASK_CALLS_STORED_PROC
AS
INSERT INTO HRMS.PUBLIC.TASK_LOG (TASK_NAME, STATUS, QUERY_ID, MESSAGE)
VALUES (
  'TASK_FINALIZE_CALLS',
  'completed',
  SYSTEM$CURRENT_QUERY(),
  'Finalize step executed successfully'
);



SELECT QUERY_ID, STATE, ERROR_MESSAGE, SCHEDULED_TIME
FROM TABLE(
  INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'HRMS.PUBLIC.TASK_FINALIZE_CALLS',
    RESULT_LIMIT => 10
  )
)
ORDER BY SCHEDULED_TIME DESC;

ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC RESUME;
ALTER TASK HRMS.PUBLIC.TASK_FINALIZE_CALLS RESUME;
ALTER TASK HRMS.PUBLIC.TASK_FINALIZE_CALLS RESUME;
---------------------------------------------------
-- Dag full of needed resume statements 
--  ----------------------------------------------- 
-- Step 1: materialize once
CREATE OR REPLACE TEMP TABLE dag_raw AS
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'HRMS.PUBLIC.TASK_CALLS_STORED_PROC',
    RECURSIVE => TRUE
  )
);

-- Step 2: use dag_raw in your query
WITH raw AS (
  SELECT * FROM dag_raw
)
-- Null/empty predecessors → depth 0
SELECT NAME AS TASK_NAME,
       PREDECESSORS,
       0 AS DEPTH
FROM raw
WHERE PREDECESSORS IS NULL

UNION ALL

-- Array predecessors → count elements
SELECT NAME AS TASK_NAME,
       PREDECESSORS,
       ARRAY_SIZE(PREDECESSORS) AS DEPTH
FROM raw
WHERE TYPEOF(PREDECESSORS) = 'ARRAY'

UNION ALL

-- String predecessors → split and count
SELECT NAME AS TASK_NAME,
       PREDECESSORS,
       ARRAY_SIZE(SPLIT(PREDECESSORS::STRING, ',')) AS DEPTH
FROM raw
WHERE TYPEOF(PREDECESSORS) = 'STRING'
  AND PREDECESSORS::STRING <> ''

ORDER BY DEPTH DESC, TASK_NAME;

-- Step 3 generate resume statements
WITH dag AS (
  -- same union‑all logic as above
  SELECT NAME, DATABASE_NAME, SCHEMA_NAME, STATE,
         CASE
           WHEN PREDECESSORS IS NULL THEN 0
           WHEN TYPEOF(PREDECESSORS) = 'ARRAY'  THEN ARRAY_SIZE(PREDECESSORS)
           WHEN TYPEOF(PREDECESSORS) = 'STRING' THEN ARRAY_SIZE(SPLIT(PREDECESSORS::STRING, ','))
           ELSE 0
         END AS DEPTH
  FROM dag_raw
)
SELECT
  'ALTER TASK ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.' || NAME || ' RESUME;' AS RESUME_SQL,
  STATE,
  DEPTH
FROM dag
WHERE STATE = 'suspended'
ORDER BY DEPTH DESC, NAME;



-- -- Wrap as a udtf for reusability
CREATE OR REPLACE FUNCTION GENERATE_TASK_RESUME_SQL(TASK_NAME STRING)
RETURNS TABLE (RESUME_SQL STRING, STATE STRING, DEPTH NUMBER)
LANGUAGE SQL
AS
$$
  SELECT
    'ALTER TASK ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.' || NAME || ' RESUME;' AS RESUME_SQL,
    STATE,
    CASE
      WHEN PREDECESSORS IS NULL THEN 0
      WHEN TYPEOF(PREDECESSORS) = 'ARRAY'  THEN ARRAY_SIZE(PREDECESSORS)
      WHEN TYPEOF(PREDECESSORS) = 'STRING' THEN ARRAY_SIZE(SPLIT(PREDECESSORS::STRING, ','))
      ELSE 0
    END AS DEPTH
  FROM TABLE(
    INFORMATION_SCHEMA.TASK_DEPENDENTS(
      TASK_NAME => TASK_NAME,
      RECURSIVE => TRUE
    )
  )
  WHERE STATE = 'suspended'
  ORDER BY DEPTH DESC, NAME
$$;
SELECT * FROM TABLE(GENERATE_TASK_RESUME_SQL('HRMS.PUBLIC.TASK_CALLS_STORED_PROC'));
ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC suspend;
ALTER TASK HRMS.PUBLIC.TASK_CALLS_STORED_PROC RESUME;
ALTER TASK HRMS.PUBLIC.TASK_FINALIZE_CALLS RESUME;
----
---- Debug the dag
----
---- Problem:  I had was the copy statement into the log had an so the statement failed silently

-- 1. Query recent runs of all tasks in the DAG:
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE, TO_CHAR(COMPLETED_TIME, 'YYYY-MM-DD HH24:MI') AS RUN_TIME_SHORT
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 100))
WHERE NAME IN ('TASK_CALLS_STORED_PROC','TASK_FINALIZE_CALLS', 'CT1', 'CT2', 'GCT1', 'GCT2' /* add other DAG tasks here */)
ORDER BY SCHEDULED_TIME DESC;

-- 2. Cross‑check with TASK_LOG
SELECT TASK_NAME, STATUS, RUN_TIME, MESSAGE, TO_CHAR(RUN_TIME, 'YYYY-MM-DD HH24:MI') AS RUN_TIME_SHORT
FROM HRMS.PUBLIC.TASK_LOG
ORDER BY RUN_TIME DESC;

--1-2 Combined: Join TASK_HISTORY with TASK_LOG to see discrepancies
SELECT 
    TH.NAME,
    TH.STATE,
    TH.SCHEDULED_TIME,
    TH.COMPLETED_TIME,
    TH.ERROR_MESSAGE,
    TL.RUN_TIME AS LOG_RUN_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 100)) AS TH
LEFT JOIN HRMS.PUBLIC.TASK_LOG AS TL
  ON TH.NAME = TL.TASK_NAME
  AND TO_CHAR(TH.COMPLETED_TIME, 'YYYY-MM-DD HH24:MI') = TO_CHAR(TL.RUN_TIME, 'YYYY-MM-DD HH24:MI')
WHERE TH.NAME IN ('TASK_CALLS_STORED_PROC','TASK_FINALIZE_CALLS','CT1','CT2','GCT1','GCT2')
ORDER BY TH.SCHEDULED_TIME DESC;


-- Step 3: Why is task gct2 failing? THER WAS A PROBLEM WITH THE SQL
DESC TASK HRMS.PUBLIC.gct2; -- Note that the warehouse = null
alter task HRMS.PUBLIC.TASK_CALLS_STORED_PROC suspend;
alter task HRMS.PUBLIC.TASK_CALLS_STORED_PROC resume;
ALTER TASK HRMS.PUBLIC.GCT2
SET WAREHOUSE = compute_wh;
CREATE OR REPLACE TASK HRMS.PUBLIC.GCT2
WAREHOUSE = compute_wh
AFTER HRMS.PUBLIC.task_calls_stored_proc
AS
SELECT 'gct2 executed';

alter task HRMS.PUBLIC.GCT2 suspend;
alter task HRMS.PUBLIC.GCT2 resume;

--Question 8: Create  a Task and have it run the Copy Statement at cyclic schedule of 5 mins
-- Step 1: Write a COPY statement that will load data to a CUSTOMER_COPY table which is a copy of the CUSTOMER table with no data.
Copy into departments_COPY
FROM  LOADINGDATA.LOAD.DEPARTMENTS_COPY
WHERE 1 = 0;

SELECT * FROM LOADINGDATA.LOAD.DEPARTMENTS_COPY;

select * from EMPLOYEES_DIM_MERGE;
-- Step 2: Create a Task and use the COPY statement in that task and set a schedule of 5 mins, resume the task
-- Step 3:Wait for 10 mins and then check to see if the task has executed.
-- Step 4: Verify that the data has been loaded
-- Step 5: Drop the Task
