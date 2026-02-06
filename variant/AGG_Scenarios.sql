-------------------------------
-- HANDS‑ON LAB: Different Forms of Aggregation
-- Purpose: Learn how Snowflake builds semi‑structured data (OBJECTs, ARRAYs)
--          from relational rows. These patterns are the foundation for:
--          • API payload construction
--          • JSON/VARIANT modeling
--          • hierarchical/nested data design
--          • dynamic schemas and metadata-driven pipelines
-------------------------------

-- Context
USE DATABASE VARIANT;
CREATE SCHEMA IF NOT EXISTS OBJECT_CONSTRUCT;
USE SCHEMA OBJECT_CONSTRUCT;


-------------------------------
-- SETUP
-------------------------------
CREATE OR REPLACE TABLE people (
    id INTEGER,
    first_name STRING,
    last_name STRING,
    age INTEGER,
    city STRING,
    profile_ssn STRING,
    tag STRING
);

INSERT INTO people VALUES
(1, 'Alice', 'Jones', 30, 'Seattle', '111-22-3333', 'premium'),
(2, 'Bob',   'Smith', 45, 'Denver',  '222-33-4444', 'trial'),
(3, 'Cara',  'Lee',   28, 'Austin',  NULL,          'premium');

-------------------------------
-- AGG limitations (important mental model)
-------------------------------
-- ✔ OBJECT_AGG keys must be VARCHAR (explicit cast recommended)
-- ✔ OBJECT_AGG values must be VARIANT (explicit cast recommended)
-- ✔ ARRAY_AGG has no type restrictions (values remain typed)
-- ✔ MAP_AGG exists only in certain Snowflake editions → avoid for portability
-- These rules explain why OBJECT_AGG often requires TO_VARCHAR + TO_VARIANT.

-------------------------------
-- Exercise 1 — OBJECT_AGG Basics (Row → Key/Value Object)
-------------------------------
-- Goal: Collapse multiple rows into a single JSON-style lookup object.
-- Why this matters:
--   • Creates compact dictionaries for fast lookup
--   • Useful for metadata maps, config objects, and API payloads
--   • Teaches the “row → object” transformation pattern
-- Expected: {"1":"Alice","2":"Bob","3":"Cara"}
SELECT OBJECT_AGG(TO_VARCHAR(id), TO_VARIANT(first_name)) AS obj
FROM people;

-------------------------------
-- Exercise 2 — OBJECT_AGG With GROUP BY (Group → Object)
-------------------------------
-- Goal: Build one object per group (hierarchical object construction).
-- Why this matters:
--   • Foundation for nested JSON (e.g., tag → {id → name})
--   • Used in API responses, grouped metadata, and dimension rollups
-- Example: premium → {"1":"Alice","3":"Cara"}
SELECT 
    tag,
    OBJECT_AGG(
        TO_VARCHAR(id),
        TO_VARIANT(first_name)
    ) AS obj
FROM people
GROUP BY tag;

-- Variation: Group by city (city → {id → name})
SELECT 
    city,
    OBJECT_AGG(
        TO_VARCHAR(id),
        TO_VARIANT(first_name)
    ) AS obj
FROM people
GROUP BY city;

-------------------------------
-- Exercise 3 — ARRAY_AGG Basics (Row → Array)
-------------------------------
-- Goal: Convert rows into an ordered or unordered array.
-- Why this matters:
--   • Arrays are the backbone of JSON lists, API responses, and event payloads
--   • Teaches the “row → array” pattern
-- Expected: ["Alice","Bob","Cara"]
SELECT ARRAY_AGG(first_name) AS arr
FROM people;

-- Challenge: Sorted array
-- ANSI-safe approach: sort rows BEFORE aggregation.
SELECT ARRAY_AGG(first_name) AS arr
FROM (
    SELECT first_name
    FROM people
    ORDER BY first_name
);

-------------------------------
-- Exercise 4 — ARRAY_AGG of Objects (Row → Object → Array)
-------------------------------
-- Goal: Convert each row into an object, then aggregate into an array.
-- Why this matters:
--   • Produces arrays of JSON objects (common in APIs and VARIANT columns)
--   • Teaches “row → object → array” nesting
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS arr
FROM people;

-- Challenge: Only include selected attributes
SELECT ARRAY_AGG(
           OBJECT_CONSTRUCT(
               'first_name', first_name,
               'last_name',  last_name,
               'city',       city
           )
       ) AS arr
FROM people;

-------------------------------
-- Exercise 5 — ARRAY_AGG With GROUP BY (Group → Array)
-------------------------------
-- Goal: Build arrays per group.
-- Why this matters:
--   • Creates grouped lists (e.g., tag → [names])
--   • Common in hierarchical JSON and reporting structures
SELECT tag, ARRAY_AGG(first_name) AS names
FROM people
GROUP BY tag;

-- Challenge: Return arrays of full objects instead of names.
SELECT tag, ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS names
FROM people
GROUP BY tag;

-------------------------------
-- Exercise 6 — Replacement for MAP_AGG (Build Lookup Objects)
-------------------------------
-- Goal: Build a dictionary-style lookup object (key → value).
-- Why this matters:
--   • Replaces MAP_AGG in environments where it’s unavailable
--   • Produces compact lookup structures for downstream queries
-- Example: {"Jones":30,"Smith":45,"Lee":28}
SELECT OBJECT_AGG(
           TO_VARCHAR(last_name),
           TO_VARIANT(age)
       ) AS age_map
FROM people;

-- Static lookup of the literal key "Jones"
-- obj:"literal" → JSON‑path lookup (works only with hard‑coded keys)
SELECT 
    age_map:"Jones" AS jones_age
FROM (
    SELECT OBJECT_AGG(
        TO_VARCHAR(last_name),
        TO_VARIANT(age)
    ) AS age_map
    FROM people
);

-- Same static lookup, but with an explicit cast
-- Adding ::int only converts the returned value; it does NOT make the lookup dynamic
SELECT 
    age_map:"Jones"::int AS jones_age
FROM (
    SELECT OBJECT_AGG(
        TO_VARCHAR(last_name),
        TO_VARIANT(age)
    ) AS age_map
    FROM people
);

-- Dynamic lookup using a column value
-- GET(obj, key)   → Proper dynamic OBJECT key lookup (supports variables/columns)
WITH lookup AS (
    SELECT OBJECT_AGG(          -- Example: { "Jones": 30, "Lee": 28, "Smith": 45 }
        last_name,
        age
    ) AS age_map
    FROM people
)
SELECT 
    p.last_name,
    GET(l.age_map, p.last_name) AS age_lookup   -- Dynamic lookup (correct)
FROM people p
CROSS JOIN lookup l;


-------------------------------
-- Exercise 7 — Grouped Lookup Objects (Group → Object of Key/Value Pairs)
-------------------------------
-- Goal: Build grouped dictionaries (tag → {id → city}).
-- Why this matters:
--   • Teaches multi-level JSON construction
--   • Useful for building hierarchical VARIANT structures
--   • Mirrors real-world API and event payload design
SELECT 
    tag,
    OBJECT_AGG(
        TO_VARCHAR(id),
        TO_VARIANT(city)
    ) AS id_to_city
FROM people
GROUP BY tag;


--------------------------------
--  OBJECT_AGG Value Rules: How Snowflake Converts Different Types to VARIANT
--------------------------------

select OBJECT_AGG('x', 123); -- { "x": 123}  -- Note: Snowflake auto‑converts numeric literals to VARIANTt
select OBJECT_AGG('y', TO_VARIANT('hello')) FROM (SELECT 1); -- { "y": "hello" } -- Note: String literals are NOT auto‑converted to VARIANT 
select OBJECT_AGG('z', PARSE_XML('<a/>')); -- { "z": { "$": "", "@": "a"}}
select OBJECT_AGG('w', ARRAY_CONSTRUCT(1,2,3)); -- { "w": [ 1, 2, 3 ]}

--------------------------------
-- 🚀 Exercise 8 — ARRAY_AGG as a Window Function (Running + Sliding Windows)
--------------------------------
-- Goal: Understand how ARRAY_AGG behaves when used as a window function instead of a GROUP BY aggregate.
-- Why this matters:
-- Windowed arrays are used for running history, sliding windows, ordered event sequences, and per‑row contextual arrays.
-- Exam questions love this because the rules differ from GROUP BY aggregation.
-- This is where PARTITION BY, ORDER BY, and window frames become essential.

--------------------------------
-- 8.1 — Basic Windowed ARRAY_AGG (Running Array): Each row gets an array of all values up to that row.
--------------------------------
-- Key concept:  
-- OVER (ORDER BY id) produces a running array.
-- Each row’s array grows as the window moves.
-- Expected: ID=3, FIRST_NAME = Cara, RUNNING_NAMES = ["Alice", "Bob", "Cara"]

SELECT
    id,
    first_name,
    ARRAY_AGG(first_name) OVER (
        ORDER BY id
    ) AS running_names
FROM people
ORDER BY id;

--------------------------------
-- 8.2 — Windowed ARRAY_AGG With PARTITION BY (Per‑Group Running Arrays)
--------------------------------
-- Goal: Each group gets its own running array.  
-- Expected (row: tag='premium', id=3, FIRST_NAME = Cara, RUNNING_NAMES_BY_TAG = ["Alice", "Cara"]
-- Why this row? Because it shows the partition reset: Cara only sees Alice (same tag), not Bob.
SELECT
    tag,
    id,
    first_name,
    ARRAY_AGG(first_name) OVER (
        PARTITION BY tag
        ORDER BY id
    ) AS running_names_by_tag
FROM people
ORDER BY tag, id;

--------------------------------
-- 8.3 — Sliding Window (1 PRECEDING → CURRENT ROW)
--------------------------------
-- Expected (row: id=3, FIRST_NAME = Cara, SLIDING_TWO = ["Bob", "Cara"]
-- This row shows the classic “previous + current” behavior.
SELECT
    id,
    first_name,
    ARRAY_AGG(first_name) OVER (
        ORDER BY id
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS sliding_two
FROM people
ORDER BY id;
--------------------------------
-- 8.4 — RANGE Frame (UNBOUNDED PRECEDING → CURRENT ROW)
--------------------------------
-- Expected (row: age=30):
-- RANGE_BY_AGE = ["Cara", "Alice"]
-- (Because ages 28 and 30 fall within the range up to 30)
-- This row is the most interesting because it shows how RANGE groups rows by value, not row position.
SELECT
    id,
    age,
    first_name,
    ARRAY_AGG(first_name) OVER (
        ORDER BY age
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS range_by_age
FROM people
ORDER BY age;

--------------------------------
-- 8.5 — ORDER BY Inside ARRAY_AGG (Value Ordering)
--------------------------------
-- Snowflake rule:
--   ❌ ORDER BY inside ARRAY_AGG is NOT allowed in window functions.
--   ❌ WITHIN GROUP is NOT allowed when OVER contains ORDER BY.
--   ✔ To demonstrate ordering, pre-sort the rows before the window.
--
-- Expected (row: id=3):
-- ORDERED_INSIDE_ARRAY = ["Alice", "Bob", "Cara"]
-- (Alphabetical because the input rows were pre-sorted)

SELECT
    id,
    ARRAY_AGG(first_name) OVER (ORDER BY id) AS ordered_inside_array
FROM (
    SELECT id, first_name
    FROM people
    ORDER BY first_name
)
ORDER BY id;

--------------------------------
-- 8.6 — DISTINCT Not Allowed With ORDER BY (Error Demo)
--------------------------------
-- Expected: ERROR
-- "DISTINCT is not supported with ORDER BY in windowed ARRAY_AGG"
-- SQL compilation error: error line 324 at position 35 distinct cannot be used with a window frame or an order.
SELECT
    ARRAY_AGG(DISTINCT first_name) OVER (ORDER BY id)
FROM people;

--------------------------------
-- 8.7 — NULL Exclusion (ARRAY_AGG Drops NULLs)
--------------------------------
-- Expected (row: id=3):
-- SSN_HISTORY = ["111-22-3333", "222-33-4444"]
-- (Cara's NULL SSN is excluded)
-- This row clearly shows the NULL being skipped.
SELECT
    id,
    profile_ssn,
    ARRAY_AGG(profile_ssn) OVER (ORDER BY id) AS ssn_history
FROM people
ORDER BY id;

--------------------------------
-- 8.8 — Forward-Looking Window (CURRENT ROW → UNBOUNDED FOLLOWING)
--------------------------------
-- Expected (row: id=1):
-- FUTURE_NAMES = ["Alice", "Bob", "Cara"]
-- This row is the most illustrative because it shows the entire future window.
SELECT
    id,
    first_name,
    ARRAY_AGG(first_name) OVER (
        ORDER BY id
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS future_names
FROM people
ORDER BY id;

--------------------------------
-- 8.9 — Comparison Table (Running vs Sliding vs Future)
--------------------------------
-- Expected (row: id=2):
-- RUNNING = ["Alice", "Bob"]
-- SLIDING_TWO = ["Alice", "Bob"]
-- FUTURE = ["Bob", "Cara"]

SELECT
    id,
    first_name,
    ARRAY_AGG(first_name) OVER (ORDER BY id) AS running,
    ARRAY_AGG(first_name) OVER (
        ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS sliding_two,
    ARRAY_AGG(first_name) OVER (
        ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS future
FROM people
ORDER BY id;



