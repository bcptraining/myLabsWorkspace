---------------------------------
-- Hands‑On Lab: Semi‑Structured Data Handling & Optimization in Snowflake
-------------------------------
-- Lab Overview
-- You will:
-- Load JSON into a VARIANT column
-- Query nested fields using path traversal
-- Extract frequently‑used fields into typed columns
-- Compare performance and query plans
-- Enable Search Optimization and test selective queries
-- Validate that Snowflake does not
--------------------------------
-- Context
--------------------------------
CREATE DATABASE IF NOT EXISTS variant;
CREATE SCHEMA IF NOT EXISTS semi_structured_lab;
use database variant;
USE SCHEMA semi_structured_lab;

-------------------------------
-- 1.0 DDL/DML to load json into a table as variant
-------------------------------
-- Create a Table with a VARIANT Column

CREATE OR REPLACE TABLE raw_events (
    id NUMBER,
    event VARIANT,
    USER_NAME varchar
);

desc table raw_events;

-- Load a few rows 
-- This dataset intentionally includes:
--   Missing fields
--   Extra nested fields
--   Evolving structure

Perfect for demonstrating schema‑less behavior.
-- Option 1 — Use SELECT … UNION ALL (Most Common & Cleanest)
INSERT INTO raw_events
SELECT 1, PARSE_JSON('{
  "user": { "id": 101, "name": "Cory", "location": "CA" },
  "device": { "type": "mobile", "os": "iOS" },
  "metrics": { "clicks": 5, "duration": 12.5 }
}')
UNION ALL
SELECT 2, PARSE_JSON('{
  "user": { "id": 102, "name": "Alex" },
  "device": { "type": "desktop", "os": "Windows" },
  "metrics": { "clicks": 2 }
}')
UNION ALL
SELECT 3, PARSE_JSON('{
  "user": { "id": 103, "name": "Sam", "location": "WA" },
  "device": { "type": "mobile" },
  "metrics": { "clicks": 9, "duration": 33.1, "extra": { "debug": true } }
}');
-- Option 2 — Use SELECT With Inline JSON (Even Cleaner)
INSERT INTO raw_events
SELECT
    column1 AS id,
    PARSE_JSON(column2) AS event  -- Works correctly
FROM VALUES
    (1, '{
      "user": { "id": 101, "name": "Cory", "location": "CA" },
      "device": { "type": "mobile", "os": "iOS" },
      "metrics": { "clicks": 5, "duration": 12.5 }
    }'),
    (2, '{
      "user": { "id": 102, "name": "Alex" },
      "device": { "type": "desktop", "os": "Windows" },
      "metrics": { "clicks": 2 }
    }'),
    (3, '{
      "user": { "id": 103, "name": "Sam", "location": "WA" },
      "device": { "type": "mobile" },
      "metrics": { "clicks": 9, "duration": 33.1, "extra": { "debug": true } }
    }');
-- Option 3 — UNRELIABLE: TO_VARIANT() stores string as variant. 
-- It may work if the string is valid json but depends on context.
-- This example does not work (unless you wrtap it with PARSE_JSON())
INSERT INTO raw_events
SELECT 1, parse_json(TO_VARIANT('{
  "user": { "id": 101, "name": "Cory", "location": "CA" },
  "device": { "type": "mobile", "os": "iOS" },
  "metrics": { "clicks": 5, "duration": 12.5 }
}'));


select id, event from raw_events;
------------------------------
-- 1.0 — Inspect the Raw Semi‑Structured Data (Query Nested Fields Using Path Traversal)
------------------------------
-- Purpose: Confirm the VARIANT is structured JSON (not a string).
SELECT
    id,
    event:user:id::NUMBER AS user_id,
    event:user:name::STRING AS user_name,
    event:metrics:clicks::NUMBER AS clicks, -- Fast, convenient, but strict (suitable for production)
    (TO_NUMBER(GET_PATH(RAW_EVENTS.EVENT, 'metrics.clicks'))) -- GET_PATH is the foundation for dynamic traversal and debugging (avoids error, returns raw value. Its slow,raw, and too permissive for prod)
FROM raw_events;

-- Demonstrate N‑Level Hierarchy Support
SELECT
    id,
    event:metrics:extra:debug AS debug_flag
FROM raw_events
WHERE event:metrics:extra:debug = true;

------------------------------
-- 1.1 — Navigate Nested JSON Fields
------------------------------
-- Purpose: Demonstrate flexible schema + N‑level hierarchy.
SELECT
    id,
    event:user:id::NUMBER        AS user_id,
    event:user:name::STRING      AS user_name,
    event:device:os::STRING      AS os,         -- This is not always present so can be null
    event:metrics:duration::FLOAT AS duration   -- This is not always present so can be null
FROM raw_events;


SELECT
    id,
    typeof(event),
    event:user:id::int            AS user_id,
    event:user:name::string       AS user_name,
    event:device:type::string     AS device_type,
    event:device:os::string       AS device_os,
    event:metrics:clicks::int     AS clicks,
    event:metrics:duration::float AS duration   -- null for this row
FROM raw_events
WHERE event:user:id::int = 102;


----------------------------------------------
-- 1.2 — Extract Frequently‑Accessed Fields into Typed Columns
----------------------------------------------
-- Purpose:
--   Demonstrate the exam concept:
--   “Extract frequently‑accessed nested fields to typed columns enabling better optimization
--    than VARIANT‑only queries.”

-- Create a curated table with typed columns
CREATE OR REPLACE TABLE curated_events (
id NUMBER,
user_id NUMBER,
user_name STRING,
device_type STRING,
device_os STRING,
clicks NUMBER,
duration FLOAT,
raw VARIANT   -- Keep original for flexibility
);

INSERT INTO curated_events
SELECT
id,
event:user:id::NUMBER,
event:user:name::STRING,
event:device:type::STRING,
event:device:os::STRING,
event:metrics:clicks::NUMBER,
event:metrics:duration::FLOAT,
event
FROM raw_events;

SELECT * FROM curated_events;

-- Notice:
--   • Missing fields become NULL
--   • Types are enforced (NUMBER, STRING, FLOAT)
--   • This table can now benefit from clustering, pruning, statistics

-------------------------
-- 1.2.1 — Compare Querying VARIANT vs Typed Columns
-------------------------
-- VARIANT path traversal
EXPLAIN
SELECT id
FROM raw_events
WHERE event:metrics:clicks::NUMBER = 5; -- Snowflake must inspect the JSON inside every row; No pruning, no metadata shortcuts, no micro‑partition elimination. The TableScan reads all bytes of the micro‑partition.

-- Typed column filter
EXPLAIN
SELECT id
FROM curated_events
WHERE clicks = 5; -- Typed columns unlock Snowflake’s full optimization engine.Uses: column‑level metadata, min/max statistics, micro‑partition pruning

-- Discussion:
--   • The typed column version uses column-level metadata & pruning.
--   • The VARIANT version must inspect each row’s JSON structure.
--   • This is the exact optimization principle tested on the exam.

-------------------------
-- 1.3 — Demonstrate Query Performance Differences
-------------------------
-- (Not actual timing — Snowflake hides micro-partition internals)
-- But you can still observe:
--   • Different operators in the plan
--   • Pruning behavior
--   • Search access paths (later)

SELECT *
FROM raw_events
WHERE event:user:name::STRING = 'Cory';

SELECT *
FROM curated_events
WHERE user_name = 'Sam';

-- Observe:
--   • The curated table can prune micro-partitions based on typed metadata.
--   • The raw VARIANT table cannot.

/*=====================================================================
  1.4 — Search Optimization on Semi‑Structured Data
  NOTE: This lab is fully functional except for the SEO commands,
        which will work once your Snowflake account has the
        Search Optimization Service enabled.
=====================================================================*/


/*---------------------------------------------------------------
  1.4.1 — Baseline: Querying Deeply Nested Fields
  This is the type of query Search Optimization accelerates.
----------------------------------------------------------------*/
SELECT *
FROM raw_events
WHERE event:metrics:extra:debug = true;



/*---------------------------------------------------------------
  1.4.2 — Attempting SEO on a VARIANT Path (Expected Failure)
  Demonstrates that SEO cannot index JSON paths.
----------------------------------------------------------------*/
ALTER TABLE raw_events
ADD SEARCH OPTIMIZATION ON (event:user:name);  
-- Expected error:
--   invalid identifier 'GET(GET(EVENT, 'user'), 'name')'



/*---------------------------------------------------------------
  1.4.3 — Correct Pattern: Extract → Materialize → Index
  Step 1: Add a computed column (NOT indexable)
----------------------------------------------------------------*/
ALTER TABLE raw_events
ADD COLUMN user_name_computed STRING
AS (event:user:name::STRING);



/*---------------------------------------------------------------
  Step 2: Add a physical column (indexable)
----------------------------------------------------------------*/
ALTER TABLE raw_events
ADD COLUMN user_name STRING;



/*---------------------------------------------------------------
  Step 3: Populate the physical column
----------------------------------------------------------------*/
UPDATE raw_events
SET user_name = event:user:name::STRING;



/*---------------------------------------------------------------
  Step 4: Inspect the table structure
----------------------------------------------------------------*/
DESC TABLE raw_events;


/*---------------------------------------------------------------
  Step 5: Generate some data so we can test SEO : This did not work. The rows are too small so still it all fits in 1 micropartition,
----------------------------------------------------------------*/
INSERT INTO raw_events (id, event, user_name)
SELECT
    id,
    OBJECT_CONSTRUCT(
        'user', OBJECT_CONSTRUCT(
            'id', id,
            'name', user_name
        ),
        'device', OBJECT_CONSTRUCT(
            'type', 'mobile',
            'os', 'iOS'
        ),
        'metrics', OBJECT_CONSTRUCT(
            'clicks', uniform(1, 10, random()),
            'duration', uniform(1, 100, random())
        )
    ) AS event,
    user_name
FROM (
    SELECT 
        seq4() AS id,
        'User_' || seq4() AS user_name
    FROM TABLE(GENERATOR(ROWCOUNT => 500000))
);


select * from raw_events limit 20;

/*=====================================================================
  1.4.4 — Enable Search Optimization (Run ONLY when SEO is enabled)
  These commands will fail until your account has the feature.
=====================================================================*/

-- Check if SEO is available in your account
-- SHOW PARAMETERS LIKE 'SEARCH%' IN ACCOUNT;   <-- This did not help. Nice try.

-- When SEO is enabled, run:

-- Step 1: Enable SEO on the table
ALTER TABLE raw_events
ADD SEARCH OPTIMIZATION;

-- Step 2: (If supported) Add SEO on specific columns
ALTER TABLE raw_events
ADD SEARCH OPTIMIZATION ON EQUALITY(user_name);


select * from raw_events;

/*---------------------------------------------------------------
  1.4.5 — Compare Query Performance (Before/After SEO)
----------------------------------------------------------------*/

-- JSON path filter (slow, no pruning) (1.4 seconds)
explain
SELECT *
FROM raw_events
WHERE event:user:name::STRING = 'Cory';

-- Typed column filter (fast, prunable, SEO‑accelerated) ( 260ms )
SELECT *
FROM raw_events
WHERE user_name = 'Cory';

-- Compare EXPLAIN plans
EXPLAIN
SELECT *
FROM raw_events
WHERE user_name = 'Cory';



/*=====================================================================
  1.5 — Validate That Snowflake Does NOT Auto‑Normalize VARIANT
=====================================================================*/

-- Show raw storage
SELECT id, typeof(event), event
FROM raw_events;

-- Show that Snowflake does not create hidden relational tables
SHOW TABLES LIKE '%raw_events%';

-- Show that VARIANT is stored as a single column
DESC TABLE raw_events;

-- Discussion:
--   • Snowflake stores JSON as a binary hierarchical structure.
--   • No automatic shredding.
--   • No hidden tables.
--   • All relationalization must be explicit.



/*=====================================================================
  1.6 — Bonus: FLATTEN for Arrays
=====================================================================*/

-- Insert a row with an array
INSERT INTO raw_events
SELECT 
    4,
    parsed,
    parsed:user.name::string
FROM (
    SELECT PARSE_JSON('{
        "user": { "id": 104, "name": "Jordan" },
        "device": { "type": "mobile", "os": "Android" },
        "metrics": { "clicks": 7, "events": ["open", "scroll", "click"] }
    }') AS parsed
);

select * from raw_events;
-- FLATTEN the array
SELECT
    id, 
    f.value::STRING AS event_name
    -- f.value::STRING
FROM raw_events,
LATERAL FLATTEN(input => event:metrics:events) f
WHERE id = 4;

-- Concepts reinforced:
--   • N‑level hierarchy
--   • Arrays inside VARIANT
--   • FLATTEN as the relational bridge
