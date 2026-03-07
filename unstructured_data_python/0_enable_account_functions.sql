/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;


/*--------------------------
Create internal stage to hold python libraries
--------------------------*/
CREATE OR REPLACE STAGE python_libraries_stage; -- '@"UNSTRUCTURED_DATA"."PDF"."PYTHON_LIBRARIES_STAGE"/pypdf2-3.0.1-py3-none-any.whl'



GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE PUBLIC;
/*-----------------------------
Below is irrelevant right now as am pursuing python udf rather than cortex solution
-----------------------------*/
-- Enable cortex functions needed to extract text from pdf files

-- GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR
--     TO ROLE accountadmin;
-- SHOW FUNCTIONS LIKE 'AI_EXTRACT%';
