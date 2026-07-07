/* ----------------------------------------
Set Context
---------------------------------------- */
use database de_sandbox;
use schema quick_refresher;



/* ----------------------------------------
LOCAL OPERATION: Load files into stage

python upload_local_files.py  
python upload_local_files.py --folder "C:\Users\coryp\Downloads\employees_part" --stage EMPLOYEES
---------------------------------------- */


/* ----------------------------------------
Create file format 
---------------------------------------- */
CREATE TEMPORARY FILE FORMAT temp_parquet_format TYPE = PARQUET;

/* ----------------------------------------
Explore internal stage
---------------------------------------- */
show stages;
list @employees;
SELECT * FROM TABLE(INFER_SCHEMA(LOCATION => '@DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES', FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.TEMP_PARQUET_FORMAT', IGNORE_CASE => TRUE));
SELECT * FROM @DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES (FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.TEMP_PARQUET_FORMAT') LIMIT 10;

SELECT COLUMN_NAME, TYPE, NULLABLE FROM TABLE(INFER_SCHEMA(LOCATION => '@DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES', FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.TEMP_PARQUET_FORMAT', IGNORE_CASE => TRUE));


/* ----------------------------------------
Create and load temporary table based on inferred schema
---------------------------------------- */

CREATE OR REPLACE TEMPORARY TABLE employee_pq
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES',
      FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.TEMP_PARQUET_FORMAT',
      IGNORE_CASE => TRUE
    ))
  );

COPY INTO employee_pq FROM @EMPLOYEES FILE_FORMAT = 'temp_parquet_format' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


/* ----------------------------------------
Explore employee_pq 
---------------------------------------- */

select * from employee_pq; --  limit 10;
 
format 