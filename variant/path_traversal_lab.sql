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
    TYPEOF(data:orders)        AS orders_type,
    TYPEOF(data:orders[0])     AS element_type,
    TYPEOF(data:orders[0]:items) AS items_type
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
    TYPEOF(data:orders)                 AS orders_type,
    TYPEOF(data:orders[0])              AS element_type,
    TYPEOF(data:orders[0]:value)        AS wrapped_value_type,
    TYPEOF(data:orders[0]:value:items)  AS items_type
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
