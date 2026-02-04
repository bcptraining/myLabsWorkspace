-----------------------------
--  Context
-----------------------------
use database variant;
use schema OBJECT_CONSTRUCT;
-- Data is from OBJECT_CONSTRUCT.SQL

SELECT
    OBJECT_AGG(
    id,
    object_delete(
    OBJECT_CONSTRUCT_KEEP_NULL(*),'PROFILE_SSN' 
    )
    )AS full_obj
 from people;   