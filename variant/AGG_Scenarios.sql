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
