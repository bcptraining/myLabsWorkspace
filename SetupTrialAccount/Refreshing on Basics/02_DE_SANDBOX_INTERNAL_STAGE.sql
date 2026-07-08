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
-- drop table DE_SANDBOX.QUICK_REFRESHER.employee_pq;
CREATE TABLE IF NOT EXISTS DE_SANDBOX.QUICK_REFRESHER.employee_pq
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES',
      FILE_FORMAT => 'DE_SANDBOX.QUICK_REFRESHER.TEMP_PARQUET_FORMAT',
      IGNORE_CASE => TRUE
    ))
  );

COPY INTO employee_pq FROM @EMPLOYEES FILE_FORMAT = 'temp_parquet_format' 
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';


/* ----------------------------------------
Explore employee_pq 
---------------------------------------- */

select * from employee_pq; --  limit 10;

/* ----------------------------------------
Create proc to remove processed files from the internal stage
---------------------------------------- */
-- CREATE OR REPLACE PROCEDURE remove_loaded_files(
--     p_database VARCHAR,
--     p_schema VARCHAR,
--     p_table VARCHAR,
--     p_stage VARCHAR
-- )
-- RETURNS VARCHAR
-- LANGUAGE JAVASCRIPT
-- AS
-- $$
--     var db = arguments[0];
--     var schema = arguments[1];
--     var table = arguments[2];
--     var stage = arguments[3];

--     var files_removed = 0;

--     // Query ACCOUNT_USAGE instead of INFORMATION_SCHEMA
--     var history_sql = `
--         SELECT FILE_NAME
--         FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
--         WHERE TABLE_CATALOG = '` + db + `'
--           AND TABLE_SCHEMA  = '` + schema + `'
--           AND TABLE_NAME    = '` + table + `'
--           AND STATUS = 'Loaded'
--           AND LAST_LOAD_TIME > DATEADD(hour, -24, CURRENT_TIMESTAMP())
--     `;

--     var history_stmt = snowflake.createStatement({sqlText: history_sql});
--     var rs = history_stmt.execute();

--     while (rs.next()) {
--         var file_name = rs.getColumnValue(1);

--         var remove_sql = `
--             REMOVE @` + stage + ` (FILE_NAME => '` + file_name + `')
--         `;

--         snowflake.createStatement({sqlText: remove_sql}).execute();
--         files_removed++;
--     }

--     return 'Removed ' + files_removed + ' file(s) from @' + stage;
-- $$;
CREATE OR REPLACE PROCEDURE remove_files_from_stage(
    p_stage VARCHAR,
    p_files ARRAY
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var stage = arguments[0];
    var files = arguments[1];
    var removed = 0;

    for (var i = 0; i < files.length; i++) {
        var file = files[i];

        var remove_sql = `
            REMOVE @` + stage + ` (FILE_NAME => '` + file + `')
        `;

        snowflake.createStatement({sqlText: remove_sql}).execute();
        removed++;
    }

    return 'Removed ' + removed + ' file(s) from @' + stage;
$$;










/* ----------------------------------------
Cleanup the internal stage 
---------------------------------------- */ 

WITH recent_loads AS (
    SELECT *
    FROM TABLE(
        INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'employee_pq',
            START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP())
        )
    )
)
SELECT FILE_NAME, status
FROM recent_loads
WHERE STATUS = 'Loaded';


-- CALL remove_loaded_files('EMPLOYEE_PQ', 'DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES');
CALL remove_loaded_files(
    'DE_SANDBOX',
    'QUICK_REFRESHER',
    'EMPLOYEE_PQ',
    'DE_SANDBOX.QUICK_REFRESHER.EMPLOYEES'
);

