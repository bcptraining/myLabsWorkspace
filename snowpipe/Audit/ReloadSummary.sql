-- ============================================
-- Audit Script: Compare original vs reloaded files (this code is broken and needs fixing)
-- ============================================

-- Step 1: Define the file names you want to audit
SET ORIGINAL_FILES = ('sales1.csv','sales2.csv');
SET RELOADED_FILES = ('sales1_reload.csv','sales2_reload.csv');

-- Step 2: Run the audit query
WITH audit AS (
    SELECT 
        FILE_NAME,
        LAST_LOAD_TIME,
        ROW_COUNT,
        ERROR_COUNT,
        CASE 
            WHEN FILE_NAME IN $RELOADED_FILES THEN 'Reloaded'
            WHEN FILE_NAME IN $ORIGINAL_FILES THEN 'Original'
            ELSE 'Other'
        END AS load_type
    FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
    WHERE TABLE_NAME = 'SALES'
      AND FILE_NAME IN (
          SELECT COLUMN1 FROM TABLE(FLATTEN(INPUT => $ORIGINAL_FILES))
          UNION ALL
          SELECT COLUMN1 FROM TABLE(FLATTEN(INPUT => $RELOADED_FILES))
      )
)
SELECT 
    load_type,
    FILE_NAME,
    LAST_LOAD_TIME,
    ROW_COUNT,
    ERROR_COUNT
FROM audit
ORDER BY LAST_LOAD_TIME;

-- Step 3: Reconciliation summary
SELECT 
    SUM(CASE WHEN FILE_NAME IN $RELOADED_FILES THEN ROW_COUNT ELSE 0 END) AS total_rows_reloaded,
    SUM(CASE WHEN FILE_NAME IN $ORIGINAL_FILES THEN ROW_COUNT ELSE 0 END) AS total_rows_original,
    SUM(CASE WHEN FILE_NAME IN $ORIGINAL_FILES THEN ERROR_COUNT ELSE 0 END) AS total_errors_original
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_NAME = 'SALES'
  AND FILE_NAME IN (
      SELECT COLUMN1 FROM TABLE(FLATTEN(INPUT => $ORIGINAL_FILES))
      UNION ALL
      SELECT COLUMN1 FROM TABLE(FLATTEN(INPUT => $RELOADED_FILES))
  );


-- The above scruipt had issues with the variable substitution in the IN clause.
-- Here is a revised version to avoid the issues: 
SELECT VALUE::STRING AS FILE_NAME
FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('sales1.csv','sales2.csv')));


-- Step 1: Define file lists directly in CTEs
WITH original AS (
    SELECT VALUE::STRING AS FILE_NAME 
    FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('sales1.csv','sales2.csv')))
),
reloaded AS (
    SELECT VALUE::STRING AS FILE_NAME 
    FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('sales1_reload.csv','sales2_reload.csv')))
),
audit AS (
    SELECT 
        FILE_NAME,
        LAST_LOAD_TIME,
        ROW_COUNT,
        ERROR_COUNT,
        CASE 
            WHEN FILE_NAME IN (SELECT FILE_NAME FROM reloaded) THEN 'Reloaded'
            WHEN FILE_NAME IN (SELECT FILE_NAME FROM original) THEN 'Original'
            ELSE 'Other'
        END AS load_type
    FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
    WHERE TABLE_NAME = 'SALES'
      AND FILE_NAME IN (
          SELECT FILE_NAME FROM original
          UNION ALL
          SELECT FILE_NAME FROM reloaded
      )
)

-- Step 2: Detailed audit results
SELECT 
    load_type,
    FILE_NAME,
    LAST_LOAD_TIME,
    ROW_COUNT,
    ERROR_COUNT
FROM audit
ORDER BY LAST_LOAD_TIME;

-- Step 3: Reconciliation summary
WITH original AS (
    SELECT VALUE::STRING AS FILE_NAME 
    FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('sales1.csv','sales2.csv')))
),
reloaded AS (
    SELECT VALUE::STRING AS FILE_NAME 
    FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT('sales1_reload.csv','sales2_reload.csv')))
)
SELECT 
    SUM(CASE WHEN FILE_NAME IN (SELECT FILE_NAME FROM reloaded) THEN ROW_COUNT ELSE 0 END) AS total_rows_reloaded,
    SUM(CASE WHEN FILE_NAME IN (SELECT FILE_NAME FROM original) THEN ROW_COUNT ELSE 0 END) AS total_rows_original,
    SUM(CASE WHEN FILE_NAME IN (SELECT FILE_NAME FROM original) THEN ERROR_COUNT ELSE 0 END) AS total_errors_original
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_NAME = 'SALES'
  AND FILE_NAME IN (
      SELECT FILE_NAME FROM original
      UNION ALL
      SELECT FILE_NAME FROM reloaded
  );
