-------------------------------
-- HANDS‑ON LAB: Mastering OBJECT_CONSTRUCT
-------------------------------
use database VARIANT;
create schema IF NOT EXISTS OBJECT_CONSTRUCT;
USE SCHEMA  

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

------------------------------
-- Exercise 1 — Basic OBJECT_CONSTRUCT
------------------------------
-- Goal: Build a JSON object from columns.
-- id = 1  person_json = { "age": 30, "first": "Alice",   "last": "Jones" }

SELECT
    id,
    OBJECT_CONSTRUCT(
        'first', FIRST_NAME,
        'last',  LAST_NAME,
        'age',   AGE
    ) AS person_json
FROM people;
------------------------------
-- Exercise 2 — Automatic Key Naming
------------------------------
-- id = 1  FULL_OBJ = { "AGE": 30,   "CITY": "Seattle",  "FIRST_NAME": "Alice", "ID": 1, "LAST_NAME": "Jones",   "PROFILE_SSN": "111-22-3333",  "TAG": "premium" }
SELECT 
    id,
    OBJECT_CONSTRUCT(*) AS full_obj FROM people;


------------------------------
-- Exercise 3 — Conditional Keys (NULL‑safe construction)
------------------------------
-- Goal: Only include keys when values are NOT NULL.
-- Learning:  
--   OBJECT_CONSTRUCT drops NULL keys.
--   OBJECT_CONSTRUCT_KEEP_NULL keeps them.
-- Expected:For Cara (id=3), "profile_ssn" should not appear in the object.
-- ID = 3, KEEP_FULL_OBJ = { "first": "Cara", "last": "Lee", "ssn": null} , DROP_NULL_OBJ = {"first": "Cara", "last": "Lee"}
SELECT
    id,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'first', first_name,
        'last',  last_name,
        'ssn',   profile_ssn
    ) AS keep_null_obj,
    OBJECT_CONSTRUCT(
        'first', first_name,
        'last',  last_name,
        'ssn',   profile_ssn
    ) AS drop_null_obj
FROM people;

------------------------------
-- Exercise 4 — Nested Objects
------------------------------
-- Goal: Build a nested JSON structure.
-- Expected:  
-- For id=1:, NESTED_OBJ = {"meta": {"age": 30, "city": "Seattle" },"name": {"first": "Alice", "last": "Jones"}}
SELECT
    id,
    OBJECT_CONSTRUCT(
        'name', OBJECT_CONSTRUCT('first', first_name, 'last', last_name),
        'meta', OBJECT_CONSTRUCT('age', age, 'city', city)
    ) AS nested_obj
FROM people;
------------------------------
-- Exercise 5 — Dynamic Keys Using Column Values
------------------------------
-- Goal: Use a column value as the key inside the object.
-- Expected:  
-- For id=1: DYNAMIC_KEY_OBJ = {"premium": {"age": 30, "city": "Seattle" }}
SELECT
    id,
    OBJECT_CONSTRUCT(
        tag, OBJECT_CONSTRUCT('city', city, 'age', age)
    ) AS dynamic_key_obj
FROM people;
------------------------------
-- Exercise 6 — Merge Objects 
------------------------------
-- Goal: Combine two objects into one.
-- Method 1 — Use OBJECT_CONSTRUCT + : (object unpacking)
-- id = 1, NAME_OBJ = {  "first": "Alice", "last": "Jones"} , META_OBJ = { "age": 30, "city": "Seattle"}, MERGED_OBJ = { "age": 30,"city": "Seattle","first": "Alice", "last": "Jones"}
SELECT
    id,
    OBJECT_CONSTRUCT(
        'first', first_name,
        'last',  last_name
    ) AS name_obj,
    OBJECT_CONSTRUCT(
        'age', age,
        'city', city
    ) AS meta_obj,
    OBJECT_CONSTRUCT(
        'first', first_name,
        'last',  last_name,
        'age',   age,
        'city',  city
    ) AS merged_obj
FROM people;
-- Method 2 — Use OBJECT_CONSTRUCT AND object_insert
-- id = 1, { "age": 30, "city": "Seattle",  "first": "Alice", "last": "Jones"}

SELECT
    id,
    -- OBJECT_CONSTRUCT('first', first_name, 'last', last_name) AS name_obj,
    -- OBJECT_CONSTRUCT('age', age, 'city', city)               AS meta_obj,
    OBJECT_INSERT(
        OBJECT_INSERT(                                                  -- { "age": 30, "first": "Alice", "last": "Jones"}
            OBJECT_CONSTRUCT('first', first_name, 'last', last_name),
            'age', age
        ),
        'city', city
    ) AS merged_obj                                                     -- { "age": 30, "city": "Seattle",  "first": "Alice", "last": "Jones"}
FROM people;

------------------------------
-- Exercise 7 — Pattern-based inclusion via ILIKE
------------------------------

SELECT OBJECT_CONSTRUCT(*) AS obj
FROM (
    SELECT * ILIKE '%_NAME', age
    FROM people
);

------------------------------
-- Exercise 8 — Exclusion‑based selection
------------------------------
-- { "AGE": 30, "CITY": "Seattle","FIRST_NAME": "Alice","ID": 1,"LAST_NAME": "Jones"}
SELECT OBJECT_CONSTRUCT(*) AS obj
FROM (
    SELECT * EXCLUDE (profile_ssn, tag)
    FROM people
);
