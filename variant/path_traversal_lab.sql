-----------------------------------------------
-- HANDS‑ON LAB: VARIANT Path Traversal & Arrays
-- Purpose: Understand how Snowflake navigates
--          nested JSON, arrays, and multi-level
--          paths inside VARIANT columns.
--
-- Focus:
--   • Direct array indexing
--   • Why nested access sometimes returns NULL
--   • When explicit ::VARIANT casting is required
--   • Why FLATTEN works when direct paths fail
-----------------------------------------------

USE DATABASE VARIANT;
CREATE SCHEMA IF NOT EXISTS JSON_PATHS;
USE SCHEMA JSON_PATHS;

-----------------------------------------------
-- 1 — Setup: Nested JSON with Array of Orders
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
desc table orders_raw; -- DATA column is a variant
select typeof(DATA) from orders_raw; -- OBJECT

SELECT data FROM orders_raw;

-----------------------------------------------
-- 2 — Direct Array Access That Returns NULL
--     (Exam Scenario)
-----------------------------------------------
-- Expectation: first order's items
-- Actual: NULL (due to missing cast)
SELECT
    data:orders[0]:items AS items_direct
FROM orders_raw;

-----------------------------------------------
-- 3 — Correct Access Using Explicit VARIANT Cast
-----------------------------------------------
SELECT
    data:orders[0]::VARIANT:items          AS items_cast_after_index,
    data:orders[0]::VARIANT:items[0]:sku   AS first_sku_with_cast
FROM orders_raw;

-----------------------------------------------
-- 4 — Why FLATTEN Works (Implicit VARIANT Exposure)
-----------------------------------------------
SELECT
    o.value:order_id          AS order_id,
    o.value:items             AS items_array,
    o.value:items[0]:sku      AS first_sku
FROM orders_raw,
LATERAL FLATTEN(input => data:orders) o;

-----------------------------------------------
-- 5 — Side-by-Side Comparison
-----------------------------------------------
SELECT
    -- Direct path with cast (correct)
    data:orders[0]::VARIANT:items[0]:sku      AS direct_with_cast,

    -- FLATTEN-based access (also correct)
    (SELECT f.value:items[0]:sku
     FROM LATERAL FLATTEN(input => data:orders) f
     WHERE f.value:order_id = 1)              AS via_flatten
FROM orders_raw;

-----------------------------------------------
-- 6 — Challenge Exercises
-----------------------------------------------

-- Challenge 1:
-- Return qty of first item in second order using direct path.
-- Expected pattern:
--   data:orders[1]::VARIANT:items[0]:qty

-- Challenge 2:
-- Return same value using FLATTEN.
-- Expected pattern:
--   Filter FLATTEN on order_id = 2, then value:items[0]:qty

-- Challenge 3:
-- Write a generic pattern reminder:
--   root:array_field[index]::VARIANT:nested_field[more_index]:deeper_field
