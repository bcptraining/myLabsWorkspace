/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Step 0. Prerequisites: storage integration + stage
--------------------------*/
-- CREATE OR REPLACE STORAGE INTEGRATION pdf_s3_si
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role' 
--   STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/pdf/');

-- desc integration pdf_s3_si;


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

-- What packages are available?
SELECT *
FROM information_schema.packages
WHERE language = 'python'
  AND package_name ILIKE '%pypdf%';

-- Create output file 
CREATE OR REPLACE TABLE PDF_TEXT_OUTPUT (
    file_name STRING,
    extracted_text STRING,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



/*--------------------------
Step 1. Create python udf: Function EXTRACT_PDF_TEXT successfully created.
'@"UNSTRUCTURED_DATA"."PDF"."PYTHON_LIBRARIES_STAGE"/pypdf2-3.0.1-py3-none-any.whl'
--------------------------*/



-- SELECT extract_pdf('@UNSTRUCTURED_DATA.PDF.PDF_STAGE/pdf.stage/CollabriaSeptember2025.pdf');

-- Here is the stored procedure version
-- CREATE OR REPLACE PROCEDURE extract_pdf_sp(file_path STRING)
-- RETURNS STRING
-- LANGUAGE PYTHON
-- RUNTIME_VERSION = '3.10'
-- PACKAGES = ('pypdf', 'snowflake-snowpark-python')
-- HANDLER = 'handler'
-- AS
-- $$
-- import pypdf
-- from snowflake.snowpark.files import SnowflakeFile

-- def handler(session, file_path):
--     with SnowflakeFile.open(file_path, 'rb') as f:
--         reader = pypdf.PdfReader(f)
--         return "\n".join(page.extract_text() or "" for page in reader.pages)
-- $$;


-- CALL extract_pdf_sp('@UNSTRUCTURED_DATA.PDF.PDF_STAGE/pdf.stage/CollabriaSeptember2025.pdf');






CREATE OR REPLACE FUNCTION extract_pdf_from_bytes(pdf_bytes BINARY) -- Function EXTRACT_PDF_FROM_BYTES successfully created.
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('PyPDF2')
HANDLER = 'handler'
AS
$$
from PyPDF2 import PdfReader
from io import BytesIO

def handler(pdf_bytes):
    reader = PdfReader(BytesIO(pdf_bytes))
    return "\n".join(page.extract_text() or "" for page in reader.pages)
$$;

-- python procedure that uses the python udf
-- 4. Snowpark stored procedure to process all PDFs in the stage
CREATE OR REPLACE PROCEDURE PROCESS_PDFS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf, current_timestamp
import traceback

def run(session: Session):

    stage_name = '@UNSTRUCTURED_DATA.PDF.PDF_STAGE'

    # List all files in the stage
    files = session.sql(f"LIST {stage_name}").collect()

    results = []

    for f in files:
        # LIST returns paths like "pdf_stage/filename.pdf"
        # We only want the actual file name
        full_name = f['name']
        file_name = full_name.split('/')[-1]

        try:
            # Read file bytes from stage
            with session.file.get_stream(f"{stage_name}/{file_name}") as stream:
                pdf_bytes = stream.read()

            # Call existing UDF to extract text
            df = session.create_dataframe([(pdf_bytes,)], schema=["pdf_bytes"])
            extracted = df.select(
                call_udf("extract_pdf_from_bytes", df["pdf_bytes"])
            ).collect()[0][0]

            results.append((file_name, extracted))

        except Exception:
            err = f"ERROR processing {file_name}: {traceback.format_exc()}"
            results.append((file_name, err))

    # Persist results if any
    if results:
        out_df = session.create_dataframe(
            results,
            schema=["file_name", "extracted_text"]
        ).with_column("processed_at", current_timestamp())

        out_df.write.mode("append").save_as_table("PDF_TEXT_OUTPUT")

    return f"Processed {len(results)} files."




/*--------------------------
Step 2.Test the UDFs
--------------------------*/
-- SELECT
--     relative_path AS file_name,
--     count_words('@pdf_s3_stage/' || relative_path) AS word_count
-- FROM DIRECTORY(@pdf_s3_stage)
-- WHERE relative_path ILIKE '%.pdf';


-- ------------------------------

-- CREATE OR REPLACE FILE FORMAT binary_passthrough
--   TYPE = 'CSV'
--   FIELD_DELIMITER = NONE
--   RECORD_DELIMITER = NONE
--   ESCAPE = NONE;

-- CREATE OR REPLACE TABLE pdf_files (
--     file_name STRING,
--     file_bytes BINARY
-- );



-- COPY INTO pdf_files
-- FROM (
--     SELECT
--         METADATA$FILENAME AS file_name,
--         $1 AS file_bytes
--     FROM @pdf_s3_stage
-- )
-- FILE_FORMAT = (TYPE = 'BINARY');


-- -- stg scan: loadingdatalabs/pdf/Collabria_July-Sep2025.pdf
-- SELECT metadata$filename
-- FROM @pdf_s3_stage (FILE_FORMAT => 'binary_passthrough');
-- --directory table scan: Collabria_July-Sep2025.pdf
-- SELECT relative_path
-- FROM DIRECTORY(@pdf_s3_stage);
