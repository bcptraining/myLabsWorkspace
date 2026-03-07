
/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Internal Stages
--------------------------*/
CREATE stage if not exists pdf_stage
  DIRECTORY = ( ENABLE = TRUE );

list @pdf_stage;
select * from directory(@pdf_stage);  -- This adds: RELATIVE_PATH, ETAG and FILE_URL (compared to list @pdf_stage;) 

/*--------------------------
Tables
--------------------------*/
CREATE OR REPLACE TABLE PDF_TEXT_OUTPUT (  -- Stores the output of the python stored to make the extracted text available 
    file_name STRING,
    extracted_text STRING,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- This table was dropped from the solution
-- CREATE OR REPLACE TABLE pdf_files AS
-- SELECT
--     relative_path AS file_name,
--     BUILD_SCOPED_FILE_URL(
--         '@UNSTRUCTURED_DATA.PDF.PDF_STAGE',
--         relative_path
--     ) AS pdf_url
-- FROM DIRECTORY(@UNSTRUCTURED_DATA.PDF.PDF_STAGE)
-- WHERE LOWER(relative_path) LIKE '%.pdf';

-- select * from pdf_files;
-- drop table pdf_files;
/*-----------------------------------
External Stages not used right now
-----------------------------------*/

CREATE OR REPLACE STAGE pdf_s3_stage
  URL = 's3://vdw-dev-ingest/loadingdatalabs/pdf/'
  STORAGE_INTEGRATION = pdf_s3_si
  DIRECTORY = (ENABLE = TRUE);

list @pdf_s3_stage;

