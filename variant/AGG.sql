-------------------------------
-- HANDS‑ON LAB: Different forms od aggregation
-------------------------------

-- Context
use database VARIANT;
create schema IF NOT EXISTS OBJECT_CONSTRUCT;
USE SCHEMA  OBJECT_CONSTRUCT;

-------------------------------
-- AGG limitations
-------------------------------
-- ✔ OBJECT_AGG key must be VARCHAR
-- ✔ OBJECT_AGG value must be VARIANT
-- ✔ You must cast manually in strict environments
-- ✔ ARRAY_AGG does not require these casts
-- ✔ MAP_AGG has its own typing rules
-- So Exercise 9 should be updated to include the explicit casts.

-------------------------------
-- Exercise 1 — OBJECT_AGG Basics (Row → Key/Value Object) -- {"1": "Alice","2": "Bob","3": "Cara"} 
-------------------------------
-- Goal:Convert multiple rows into a single JSON‑style OBJECT using OBJECT_AGG.
-- Expected: {"1": "Alice","2": "Bob","3": "Cara"} 
-- ✔ OBJECT_AGG key must be VARCHAR
-- ✔ OBJECT_AGG value must be VARIANT
SELECT OBJECT_AGG(TO_VARCHAR(id), TO_VARIANT(first_name)) AS obj
FROM people;

-------------------------------
-- Exercise 2 — OBJECT_AGG With GROUP BY : TAG = premium, OBJ = {"1": "Alice","3": "Cara"}
-------------------------------
-- Goal: Build one object per group.
SELECT 
    tag,
    OBJECT_AGG(
        TO_VARCHAR(id),
        TO_VARIANT(first_name)
    ) AS obj
FROM people
GROUP BY tag;
-- another example -- Group by city
SELECT 
    city,
    OBJECT_AGG(
        TO_VARCHAR(id),
        TO_VARIANT(first_name)
    ) AS obj
FROM people
GROUP BY city;


-------------------------------
-- Exercise 3 — ARRAY_AGG Basics (Row → Array) -- ["Alice","Bob","Cara"]
-------------------------------
-- Goal:Build an array of values.
SELECT ARRAY_AGG(first_name) AS arr
FROM people;
-- Challenge: Sort -- Instead of ordering inside the aggregate, order the rows before the aggregate.
SELECT ARRAY_AGG(first_name) AS arr
FROM (
    SELECT first_name
    FROM people
    ORDER BY first_name
);


-------------------------------
-- Exercise 4 — ARRAY_AGG of Objects -- Returns a list of 3 objects
-------------------------------
--  Goal: Turn each row into an object, then aggregate into an array.
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS arr
FROM people;
-- Challenge: An array of full json objects containing only: first_name, last_name, city
SELECT ARRAY_AGG(OBJECT_CONSTRUCT('first_name',first_name,'last_name',last_name,'city',city)) AS arr
FROM people;

-------------------------------
-- Exercise 5 — ARRAY_AGG With GROUP BY
-------------------------------
--  Goal:Group rows and build arrays per group.
SELECT tag, ARRAY_AGG(first_name) AS names
FROM people
GROUP BY tag;

--  Challenge: Return arrays of full objects instead of names.
SELECT tag, ARRAY_AGG(object_construct(*)) AS names
FROM people
GROUP BY tag;

-------------------------------
-- Exercise 6 — MAP_AGG Basics
-------------------------------
-- Goal: Build typed MAPs.
-- SELECT MAP_AGG(last_name, age) AS age_map  <-- MAP_AGG not available in all account types
-- FROM people;
SELECT OBJECT_AGG(    <-- Do this instead
    TO_VARCHAR(last_name),
    TO_VARIANT(age)
) AS age_map
FROM people;

-------------------------------
-- Replacement for Exercise 7 (MAP_AGG + GROUP BY)
-------------------------------
SELECT 
    tag,
    OBJECT_AGG(
        TO_VARCHAR(id),
        TO_VARIANT(city)
    ) AS id_to_city
FROM people
GROUP BY tag;
