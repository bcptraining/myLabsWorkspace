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
Create and load table based on inferred schema
---------------------------------------- */
drop table DE_SANDBOX.QUICK_REFRESHER.employee_pq;
CREATE TABLE IF NOT EXISTS DE_SANDBOX.QUICK_REFRESHER.employee_pq
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES',
      FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.TEMP_PARQUET_FORMAT',
      IGNORE_CASE => TRUE
    ))
  );

-- -- Optional: quarantine table for rejected ROWS
-- CREATE TABLE employee_pq_quarantine (
--     row_number NUMBER,
--     error VARCHAR,
--     error_line NUMBER,
--     error_column VARCHAR,
--     error_code NUMBER,
--     raw_line VARCHAR
-- );

COPY INTO employee_pq FROM @EMPLOYEES FILE_FORMAT = 'temp_parquet_format' 
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    VALIDATION_MODE = 'RETURN_ERRORS';
  -- have orchestrator manage quarantine loop  



-- See status of the load


/* ----------------------------------------
Explore employee_pq 
---------------------------------------- */

select * from employee_pq; --  limit 10;

/* ----------------------------------------
Create proc to remove processed files from the internal stage
---------------------------------------- */
CREATE OR REPLACE PROCEDURE DE_SANDBOX.QUICK_REFRESHER.remove_loaded_files(p_table_name VARCHAR, p_stage_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    files_removed INT DEFAULT 0;
    query VARCHAR;
BEGIN
    query := 'SELECT FILE_NAME FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY('
             || 'TABLE_NAME => ''' || p_table_name || ''', '
             || 'START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP()))) '
             || 'WHERE STATUS = ''Loaded''';
    LET rs RESULTSET := (EXECUTE IMMEDIATE :query);
    LET cur CURSOR FOR rs;
    FOR rec IN cur DO
        EXECUTE IMMEDIATE
            'REMOVE @' || p_stage_name || '/' || rec.FILE_NAME;
        files_removed := files_removed + 1;
    END FOR;

    RETURN 'Removed ' || files_removed || ' file(s) from @' || p_stage_name;
END
$$;


/* ----------------------------------------
Cleanup the internal stage 
---------------------------------------- */ 

WITH recent_loads AS (
    SELECT *
    FROM TABLE(
        INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'employee_pq',
            START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())
        )
    )
)
SELECT FILE_NAME, status
FROM recent_loads
WHERE STATUS = 'Loaded';

show stages;
call remove_loaded_files('employee_pq','employees');

