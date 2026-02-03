-------------------
-- Context
-------------------
create database variant;
create schema json;

--------------------
-- DDL -- JSON tables
-------------------

CREATE OR REPLACE TABLE events ( id INTEGER, sessiondata VARIANT );
-------------------
-- DML 
-------------------

-- events has 3 nested levels: users, session, action


INSERT INTO events
SELECT
    1, 
    -- session data
    PARSE_JSON('{  
        "users": [
            {
                "name": "Alice",
                "sessions": [
                    { "actions": ["login", "view", "logout"] },
                    { "actions": ["login", "purchase"] }
                ]
            },
            {
                "name": "Bob",
                "sessions": [
                    { "actions": ["login"] }
                ]
            }
        ]
    }');


-------------------
-- LATERAL FLATTEN
-------------------
select e.id,
       l1.value:name::string as user_name, -- Alice
       l2.value as actions_array,          -- { "actions": ["login","view","logout"}
       l3.value as action                  -- "login"
from events e,
     lateral flatten(input => e.sessiondata:users) l1,
     lateral flatten(input => l1.value:sessions) l2,
     lateral flatten(input => l2.value:actions) l3;

-------------------
-- LATERAL W/O FLATTEN
-------------------
-- Think of LATERAL as: “This subquery can reference columns from the row before it.”
-- KEY IDEA: LATERAL is about row‑by‑row dependency, not arrays.

-- Example Table
CREATE OR REPLACE TABLE demo (id INT, base INT);
INSERT INTO demo VALUES (1, 10), (2, 20);

-- Example: LATERAL subquery that depends on the outer row
SELECT
    d.id,           -- 1,  2
    d.base,         -- 10. 20
    l.double_value, -- 20, 40
    l.triple_value  -- 30, 60
FROM demo d,
     LATERAL (SELECT    d.base * 2 AS double_value,
                        d.base * 3 AS triple_value  
     ) l;

----------------------
-- FLATTEN W/O LATERAL (RARELY A VALID THING TO DO)
----------------------
SELECT f.value
FROM TABLE(FLATTEN(input => PARSE_JSON('[1,2,3]'))) f;

SELECT *
FROM TABLE(FLATTEN(input => PARSE_JSON('["a","b","c"]')));

