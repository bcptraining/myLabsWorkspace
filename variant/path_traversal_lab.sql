-----------------------------------------------
-- HANDS‑ON LAB: VARIANT Path Traversal & Arrays
--
-- Purpose:
--   Learn how Snowflake navigates nested JSON,
--   especially arrays and multi-level paths.
--
-- Focus:
--   • Direct array indexing
--   • Why some paths return NULL
--   • When explicit ::VARIANT casting is required
--   • Why FLATTEN works when direct paths fail
--
-- This lab uses TWO tables:
--   1) orders_raw              → direct paths work
--   2) orders_raw_misaligned   → direct paths fail (exam scenario)

-- Summary from the exam question explains it well
-- VARIANT path traversal: 
--    colon notation for fields (data:field), 
--    bracket notation for arrays (data:array[0]), 
--    type casting for nested access (data:array[0]::VARIANT:nested). 
--       Without type casting after array access, nested field access returns NULL. 
--       FLATTEN alternative for complex nested structures. Use GET_PATH for dynamic paths. Type cast at each nesting level when using bracket notation with additional traversal.

-- Mental Note: FLATTEN does two things:
--   Splits arrays into rows
--   Exposes each element as a clean VARIANT, making path traversal easy
-----------------------------------------------

USE DATABASE VARIANT;
CREATE SCHEMA IF NOT EXISTS JSON_PATHS;
USE SCHEMA JSON_PATHS;

-----------------------------------------------
-- 1 — Table With Normal JSON Structure
--     (Direct path access works)
-----------------------------------------------
CREATE OR REPLACE TABLE orders_raw AS
SELECT PARSE_JSON('{
  "customer_id": 123,
  "orders": [
    {
      "order_id": 1,
      "items": [
        {"sku": "A1", "qty": 2},
        {"sku": "B2", "qty": 1}
      ]
    },
    {
      "order_id": 2,
      "items": [
        {"sku": "C3", "qty": 5}
      ]
    }
  ]
}') AS data;

SELECT data FROM orders_raw;

-----------------------------------------------
-- 1.1 — Direct Array Access Works
-----------------------------------------------
-- orders[0] IS the order object:
--   { "order_id": 1, "items": [...] }
--
-- Therefore :items exists at this level.
SELECT
    data:orders[0]:items AS items_direct
FROM orders_raw;

-----------------------------------------------
-- 1.2 — Optional: Inspect Types
-----------------------------------------------
SELECT
    TYPEOF(data:orders)        AS orders_type,  -- Array
    TYPEOF(data:orders[0])     AS element_type, -- Object
    TYPEOF(data:orders[0]:items) AS items_type  -- Array
FROM orders_raw;

-- Compare this to Section 2 later.

-----------------------------------------------
-- 2 — Table That Reproduces Exam NULL Behavior
--     WHY this table behaves differently:
--
-- In Section 1:
--   orders[0] = { "order_id": 1, "items": [...] }
--
-- In THIS table:
--   orders[0] = { "value": { "order_id": 1, "items": [...] } }
--
-- Because of this wrapper, :items does NOT exist at this level.
-- Direct path traversal returns NULL.
--
-- After completing this section, COMPARE with Section 1.
-----------------------------------------------
CREATE OR REPLACE TABLE orders_raw_misaligned AS
SELECT PARSE_JSON('{
  "customer_id": 999,
  "orders": [
    {
      "value": {
        "order_id": 1,
        "items": [
          {"sku": "X1", "qty": 10},
          {"sku": "Y2", "qty": 20}
        ]
      }
    },
    {
      "value": {
        "order_id": 2,
        "items": [
          {"sku": "Z3", "qty": 30}
        ]
      }
    }
  ]
}') AS data;

SELECT data FROM orders_raw_misaligned;

-----------------------------------------------
-- 2.1 — Direct Path Access Fails (Returns NULL)
-----------------------------------------------
-- orders[0] = { "value": { ... } }
-- There is NO "items" field at this level.
SELECT
    data:orders[0]:items AS items_direct_fails
FROM orders_raw_misaligned;

-----------------------------------------------
-- 2.2 — Correct Access Using Explicit VARIANT Cast
-----------------------------------------------
-- Casting forces Snowflake to treat orders[0] as a VARIANT object,
-- enabling traversal into :value and then :items.
SELECT
    data:orders[0]::VARIANT:value:items AS items_after_cast,
    data:orders[0]::VARIANT:value:items[0]:sku AS first_sku_after_cast
FROM orders_raw_misaligned;

-----------------------------------------------
-- 2.3 — FLATTEN Works Because It Exposes Inner Object
-----------------------------------------------
-- FLATTEN unwraps the array element, so f.value is the element itself.
SELECT
    f.value:value:order_id      AS order_id,
    f.value:value:items         AS items_array,
    f.value:value:items[0]:sku  AS first_sku
FROM orders_raw_misaligned,
LATERAL FLATTEN(input => data:orders) f;

-----------------------------------------------
-- 2.4 — Diagnostic: Inspect Types
-----------------------------------------------
-- This reveals WHY direct access fails.
SELECT
    TYPEOF(data:orders)                 AS orders_type,         -- Array
    TYPEOF(data:orders[0])              AS element_type,        -- Object   
    TYPEOF(data:orders[0]:value)        AS wrapped_value_type,  -- Object 
    TYPEOF(data:orders[0]:value:items)  AS items_type           -- Array
FROM orders_raw_misaligned;

-----------------------------------------------
-- 3 — Challenge Exercises
-----------------------------------------------

-- Challenge 1:
-- Using orders_raw (Section 1), return the qty of the first item
-- in the second order using direct path access.

SELECT
     data:orders[1]:items[0]:qty AS ord2_item1_qty
FROM orders_raw;

SELECT
     data:orders[1]::VARIANT:items[0]:qty AS ord2_item1_qty_after_cast
FROM orders_raw;


-- Expected pattern:
--   data:orders[1]:items[0]:qty


-- Challenge 2:
-- Using orders_raw_misaligned (Section 2), return the same value.
-- Must use explicit cast.
SELECT
     data:orders[1]::VARIANT:value:items[0]:qty AS ord2_item1_qty_after_cast
FROM orders_raw_misaligned;

-- Expected pattern:
--   data:orders[1]::VARIANT:value:items[0]:qty


-- Challenge 3:
-- Write a generic reminder pattern for nested arrays + objects:
--   root:array_field[index]::VARIANT:nested_field[more_index]:deeper_field

-----------------------------------------------
-- 4 — GET_PATH: Dynamic and Safe JSON Navigation
--
-- Why GET_PATH matters:
--   • Works even when colon notation fails
--   • Accepts dynamic paths (variables, columns)
--   • Returns NULL safely without breaking traversal
--   • Essential for programmatic JSON exploration
-----------------------------------------------

-----------------------------------------------
-- 4.1 — GET_PATH Works on Normal Structure
-----------------------------------------------
SELECT
    data,
    TYPEOF(data:orders[0]:items) as items_typeof,                       -- Array
    GET_PATH(data, 'orders[0].items') AS items_via_get_path,            -- Array
    TYPEOF(data:orders[0]:items[0]) as item_typeof,                     -- Object
    GET_PATH(data, 'orders[0].items[0].sku') AS first_sku_via_get_path
FROM orders_raw;

-----------------------------------------------
-- 4.2 — GET_PATH Works on Misaligned Structure
-----------------------------------------------
-- Note: It is okay (bad practice tho) to mix colon and dot notation between segments. 
-- However, a segment ends at a dot so you can see one invalid example below
SELECT
    TYPEOF(data:orders) as orders_typeof,
    TYPEOF(data:orders[0].value) as orders_value_typeof,
    TYPEOF(data:orders[0]:value.items) as items_typeof,                  -- ARRAY of value objects
    data:orders[0]:value:items as items_colon,  
    data:orders[0]:value.items[0]:qty as items_colon_0_qty,
    data:orders[0]:value:items[0].qty as items_colon_0_dot_qty,
    data:orders[0].value:items[0]:qty as items_qty_mixed_notation,
    data:orders[0].value:items[0]:qty as items_qty_mixed_doesitwork,
    data:orders[0].value.items[0]:qty as items_qty_invalid1,
    data:orders[0]:value.items[0]:qty as items_qty_invalid2,
    -- data.orders[0]:value.items[0]:qty as items_qty_invalid3, -- SQL compilation error: error line 236 at position 4 invalid identifier 'DATA.ORDERS'
    --  The above error is because a segment ends at a dot and cannot mix notation within same segment   
    data:orders[0].value.items as items_dot,
    data:orders[0].value.items[0].qty as items_dot_0_qty,
    GET_PATH(data, 'orders[0].value.items') AS items_via_get_path,
    GET_PATH(data, 'orders[0].value.items[0].sku') AS first_sku_via_get_path
FROM orders_raw_misaligned;

-----------------------------------------------
-- 4.3 — Compare GET_PATH vs Colon Notation
-----------------------------------------------
SELECT
    data:orders[0]:items                         AS colon_direct,
    GET_PATH(data, 'orders[0].items')            AS get_path_direct,

    data:orders[0]:value:items                   AS colon_wrapped,
    GET_PATH(data, 'orders[0].value.items')      AS get_path_wrapped
FROM orders_raw_misaligned;

-----------------------------------------------
-- 4.4 — Dynamic Path Example (Colon Notation Cannot Do This)
-----------------------------------------------
WITH params AS (
    SELECT 'orders[1].value.items[0].qty' AS dynamic_path
)
SELECT
    GET_PATH(data, dynamic_path) AS dynamic_lookup
FROM orders_raw_misaligned, params;

-----------------------------------------------
-- 4.5 — GET_PATH Is Safer for Missing Fields
-----------------------------------------------
SELECT
    GET_PATH(data, 'orders[0].items')            AS safe_path,
    GET_PATH(data, 'orders[0].does_not_exist')   AS safe_missing,
    data:orders[0]:does_not_exist                AS colon_missing
FROM orders_raw;

-----------------------------------------------
-- 5 — JSON_EXTRACT_PATH_TEXT: Legacy but Useful
--
-- Purpose:
--   Provide a TEXT-returning alternative to colon notation.
--   Works even when nested structures are misaligned.
--
-- Key Traits:
--   • Accepts exactly TWO arguments (variant, string path)
--   • Path must be a single dot-notation string
--   • Returns TEXT (not VARIANT)
--   • Safe traversal similar to GET_PATH
--   • Supports arrays using bracket notation inside the string
-----------------------------------------------


-----------------------------------------------
-- 5.1 — Basic Usage on Normal Structure
-----------------------------------------------
-- Equivalent to: data:orders[0]:items[0]:sku
-- But returns TEXT instead of VARIANT.
SELECT
    JSON_EXTRACT_PATH_TEXT(
        data,
        'orders[0].items[0].sku'
    ) AS first_sku_text
FROM orders_raw;


-----------------------------------------------
-- 5.2 — Works on Misaligned Structure
-----------------------------------------------
-- Equivalent to:
--   data:orders[0]::VARIANT:value:items[0]:qty
SELECT
    JSON_EXTRACT_PATH_TEXT(
        data,
        'orders[0].value.items[0].qty'
    ) AS qty_text
FROM orders_raw_misaligned;


-----------------------------------------------
-- 5.3 — Why It Works When Colon Notation Fails
-----------------------------------------------
-- Colon notation fails because:
--   orders[0] = { "value": { ... } }
-- There is no :items at this level.
--
-- JSON_EXTRACT_PATH_TEXT walks the full path safely.
SELECT
    data:orders[0]:items AS colon_fails,
    JSON_EXTRACT_PATH_TEXT(
        data,
        'orders[0].value.items'
    ) AS json_extract_succeeds
FROM orders_raw_misaligned;


-----------------------------------------------
-- 5.4 — Compare All Four Approaches Side-by-Side
-----------------------------------------------
SELECT
    data:orders[0]:value:items[0]:sku                     AS colon_sku,
    data:orders[0]::VARIANT:value:items[0]:sku            AS cast_sku,
    GET_PATH(data, 'orders[0].value.items[0].sku')        AS get_path_sku,
    JSON_EXTRACT_PATH_TEXT(
        data,
        'orders[0].value.items[0].sku'
    ) AS json_extract_sku
FROM orders_raw_misaligned;


-----------------------------------------------
-- 5.5 — When to Use JSON_EXTRACT_PATH_TEXT
-----------------------------------------------
-- Use when:
--   • You want TEXT output directly
--   • You need safe traversal without casting
--   • JSON structure is unpredictable or misaligned
--   • Reading or migrating legacy code
--
-- Avoid when:
--   • You need arrays or objects (returns TEXT only)
--   • You want clean VARIANT traversal (colon or GET_PATH is better)
-----------------------------------------------