-----------------------------
--  Context
-----------------------------
use database variant;
use schema OBJECT_CONSTRUCT;
-- Data is from OBJECT_CONSTRUCT.SQL

------------------------------
-- Exercise 1 — OBJECT_DELETE
------------------------------
-- Goal: Remove a key from an object.
-- We’ll build a full object, then delete "LAST_NAME" AND "PROFILE_SSN".
SELECT
    id,
    OBJECT_CONSTRUCT(*) AS full_obj,  -- { "AGE": 30, "CITY": "Seattle", "FIRST_NAME": "Alice", "ID": 1, "LAST_NAME": "Jones", "PROFILE_SSN": "111-22-3333", "TAG": "premium"}
    OBJECT_DELETE(OBJECT_CONSTRUCT(*),-- { "AGE": 30, "CITY": "Seattle", "FIRST_NAME": "Alice", "ID": 1, "TAG": "premium"}
        'PROFILE_SSN', 'LAST_NAME') AS no_ssn_obj
FROM people;

------------------------------
-- Exercise 2 — OBJECT_DELETE with Nested Paths (GOTCHA -- This wont work as OBJECT_DELETE only deletes top-level keys)
------------------------------
-- Goal: Delete a nested key using dot‑notation. <--- Does not work on nested paths... you can't do it other than constructing w/o the attribute from get-go!

SELECT
    id,
    OBJECT_CONSTRUCT(
        'name', OBJECT_CONSTRUCT('first', first_name, 'last', last_name),
        'meta', OBJECT_CONSTRUCT('age', age, 'city', city)
    ) AS nested_obj,
    OBJECT_DELETE(
        OBJECT_CONSTRUCT(
            'name', OBJECT_CONSTRUCT('first', first_name, 'last', last_name),
            'meta', OBJECT_CONSTRUCT('age', age, 'city', city)
        ),
        'meta.age' --   <-- Note the dot notation on the delete WILL NOT WORK but is syntactically valid
    ) AS removed_age
FROM people;

------------------------------
-- Exercise 3 — OBJECT_PICK
------------------------------
-- Goal: Keep only specific keys from an object.
SELECT
    id,
    OBJECT_CONSTRUCT(*) AS full_obj,
    OBJECT_PICK(OBJECT_CONSTRUCT(*), 'FIRST_NAME', 'CITY') AS picked_obj
FROM people;

------------------------------
-- Exercise 4 — OBJECT_PICK with Missing Keys
------------------------------
-- Goal: Understand how Snowflake handles keys that don’t exist.
-- id = 1, PICKED_OBJ = { "FIRST_NAME": "Alice"}
SELECT
    id,
    OBJECT_PICK(OBJECT_CONSTRUCT(*), 'FIRST_NAME', 'DOES_NOT_EXIST') AS picked_obj
FROM people;



------------------------------
-- Exercise 5 — OBJECT_AGG (Group Rows into an Object)
----------------------------
-- Goal: Build an object where each key is a person’s ID and each value is their name.
-- {"1": {"first": "Alice", "last": "Jones" }, "2": { "first": "Bob", "last": "Smith" },"3": {"first": "Cara", "last": "Lee"}}

SELECT
    OBJECT_AGG(
        id, 
        OBJECT_CONSTRUCT('first', first_name, 'last', last_name)
    ) AS people_by_id
FROM people;

------------------------------
-- Exercise 6 — OBJECT_AGG with Duplicate Keys
------------------------------
-- Goal: See how Snowflake resolves key collisions.
-- EXPECTED: -- ERROR -- Duplicate field key 'premium'
SELECT
    OBJECT_AGG(
        tag,  -- duplicate key for Alice & Cara ("premium")
        id
    ) AS agg_by_tag
FROM people;
------------------------------
-- Exercise 6  FIXES 
------------------------------
-- FAILED FIX 1 — Use ARRAY_AGG as the value (Snowflake does not allow aggregates inside aggregates)
SELECT OBJECT_AGG(tag, ARRAY_AGG(id)) FROM people GROUP BY tag; -- SQL compilation error: Aggregate functions cannot be nested: [ARRAY_AGG(PEOPLE.ID)] nested in [OBJECT_AGG(PEOPLE.TAG, CAST(ARRAY_AGG(PEOPLE.ID) AS VARIANT))]

-- SUCCESSFUL FIX 2: Use a Subquery (Two‑Stage Aggregation) 
-- This is the only valid pattern in Snowflake when you want:
-- keys that repeat
-- values aggregated into arrays
-- then wrapped into an object
-- ✔️ Stage 1: group by tag
-- ✔️ Stage 2: object_agg the results
-- {  "premium": [ 1, 3],   "trial": [2]}

SELECT
    OBJECT_AGG(tag, ids) AS agg_by_tag
FROM (
    SELECT
        tag,
        ARRAY_AGG(id) AS ids
    FROM people
    GROUP BY tag
);
