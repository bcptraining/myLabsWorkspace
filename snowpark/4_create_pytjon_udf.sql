/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Step 0. Prerequisites: storage integration + stage
--------------------------*/
CREATE OR REPLACE STORAGE INTEGRATION pdf_s3_si
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/pdf/');

desc integration pdf_s3_si;


-- CREATE OR REPLACE STAGE pdf_s3_stage
--   URL='s3://vdw-dev-ingest/loadingdatalabs/pdf/'
--   CREDENTIALS = (
--     AWS_KEY_ID='mykey'
--     AWS_SECRET_KEY='mysecret'
--   )
--   DIRECTORY = (ENABLE = TRUE);\

CREATE OR REPLACE STAGE pdf_s3_stage
  URL = 's3://vdw-dev-ingest/loadingdatalabs/pdf/'
  STORAGE_INTEGRATION = pdf_s3_si
  DIRECTORY = (ENABLE = TRUE);


SHOW STAGES LIKE 'PDF_S3_STAGE';

list @pdf_s3_stage;
list @pdf_stage;

/*--------------------------
Step 1. Create python udf: Function EXTRACT_PDF_TEXT successfully created.
'@"UNSTRUCTURED_DATA"."PDF"."PYTHON_LIBRARIES_STAGE"/pypdf2-3.0.1-py3-none-any.whl'
--------------------------*/
LIST @"UNSTRUCTURED_DATA"."PDF"."PYTHON_LIBRARIES_STAGE";

SHOW PARAMETERS LIKE 'python_imports_enabled';
SHOW PARAMETERS LIKE 'enable_anaconda_packages';
SHOW PARAMETERS LIKE '%python%';
ALTER ACCOUNT SET ENABLE_ANACONDA_PACKAGES = TRUE;
ALTER ACCOUNT SET PYTHON_IMPORTS_ENABLED = TRUE;
LIST @UNSTRUCTURED_DATA.PDF.PDF_STAGE;

-- The udf is getting created but erroring out when called -- copilot says udfs cannot interact with files and to try a stored procedure instead
CREATE OR REPLACE FUNCTION extract_pdf(file_path STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('pypdf', 'snowflake-snowpark-python')
HANDLER = 'handler'
AS
$$
import pypdf
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import build_scoped_file_url

def handler(file_path):
    scoped = build_scoped_file_url(file_path)
    with SnowflakeFile.open(scoped, 'rb') as f:
        reader = pypdf.PdfReader(f)
        return "\n".join(page.extract_text() or "" for page in reader.pages)
$$;



SELECT extract_pdf('@UNSTRUCTURED_DATA.PDF.PDF_STAGE/pdf.stage/CollabriaSeptember2025.pdf');

-- Here is the stored procedure version
CREATE OR REPLACE PROCEDURE extract_pdf_sp(file_path STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('pypdf', 'snowflake-snowpark-python')
HANDLER = 'handler'
AS
$$
import pypdf
from snowflake.snowpark.files import SnowflakeFile

def handler(session, file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
        reader = pypdf.PdfReader(f)
        return "\n".join(page.extract_text() or "" for page in reader.pages)
$$;


CALL extract_pdf_sp('@UNSTRUCTURED_DATA.PDF.PDF_STAGE/pdf.stage/CollabriaSeptember2025.pdf');




CREATE OR REPLACE FUNCTION extract_pdf(file_path STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'handler'
IMPORTS = ('@UNSTRUCTURED_DATA.PDF.PYTHON_LIBRARIES_STAGE/pypdf-3.17.4-py3-none-any.whl')
AS
$$
import pypdf
from snowflake.snowpark.files import SnowflakeFile

def handler(file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
        reader = pypdf.PdfReader(f)
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
        return text
$$;






CREATE OR REPLACE FUNCTION extract_pdf_text(file_bytes BINARY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('PyPDF2')
HANDLER = 'handler'
AS
$$
import PyPDF2
from io import BytesIO

def handler(file_bytes):
    if file_bytes is None:
        return None

    reader = PyPDF2.PdfReader(BytesIO(file_bytes))
    text = []

    for page in reader.pages:
        try:
            text.append(page.extract_text() or "")
        except:
            text.append("")

    return "\n".join(text)
$$;
-- Example 2: count words in pdf
CREATE OR REPLACE FUNCTION count_words(file FILE) -- Function COUNT_WORDS successfully created.
RETURNS INTEGER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'run'
AS
$$
def run(file):
    import fitz  # PyMuPDF

    if file is None:
        return None

    pdf = fitz.open(stream=file.read(), filetype="pdf")
    text = ""
    for page in pdf:
        text += page.get_text()

    return len(text.split())
$$;



/*--------------------------
Step 2.Test the UDFs
--------------------------*/
SELECT
    relative_path AS file_name,
    count_words('@pdf_s3_stage/' || relative_path) AS word_count
FROM DIRECTORY(@pdf_s3_stage)
WHERE relative_path ILIKE '%.pdf';


------------------------------

CREATE OR REPLACE FILE FORMAT binary_passthrough
  TYPE = 'CSV'
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = NONE
  ESCAPE = NONE;

CREATE OR REPLACE TABLE pdf_files (
    file_name STRING,
    file_bytes BINARY
);



COPY INTO pdf_files
FROM (
    SELECT
        METADATA$FILENAME AS file_name,
        $1 AS file_bytes
    FROM @pdf_s3_stage
)
FILE_FORMAT = (TYPE = 'BINARY');


-- stg scan: loadingdatalabs/pdf/Collabria_July-Sep2025.pdf
SELECT metadata$filename
FROM @pdf_s3_stage (FILE_FORMAT => 'binary_passthrough');
--directory table scan: Collabria_July-Sep2025.pdf
SELECT relative_path
FROM DIRECTORY(@pdf_s3_stage);
