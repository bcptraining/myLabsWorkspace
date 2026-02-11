/*---------------------------------------------------------------
Create example table with a variant column
-----------------------------------------------------------------*/
CREATE OR REPLACE TABLE raw_events (
    id NUMBER,
    event VARIANT,
    USER_NAME varchar
);

/*---------------------------------------------------------------
DML:  Load a few rows of data manually
-----------------------------------------------------------------*/
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

-- Popuilate USER_NAME from the event data
-- select * from raw_events limit 20;

-- update raw_events
-- set USER_NAME = event:user.name where id = 101;

/*---------------------------------------------------------------
 Generate some json data
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