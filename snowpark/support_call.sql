/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

SELECT BUILD_SCOPED_FILE_URL(
    '@UNSTRUCTURED_DATA.PDF.PDF_STAGE',
    TRUE
) AS base_url;


LIST @"UNSTRUCTURED_DATA"."PDF"."PDF_STAGE";

CREATE OR REPLACE FUNCTION read_text_file(path STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'handler'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
from snowflake.snowpark.files import SnowflakeFile

def handler(path):
    with SnowflakeFile.open(path, 'r') as f:
        return f.read()
$$;



SELECT read_text_file('@UNSTRUCTURED_DATA.PDF.PDF_STAGE/helloworld.txt');
