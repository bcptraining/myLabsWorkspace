/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;


/*--------------------------
Confirm the expected pdf files were loaded into the internal stage
--------------------------*/
list @pdf_stage;

/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Load a few pdf files via the streamlit app creasted in step 2
--------------------------*/

/*--------------------------
Confirm the expected pdf files were loaded into the internal stage
--------------------------*/

list @pdf_stage;
select * from directory(@pdf_stage);  -- This adds: RELATIVE_PATH, ETAG and FILE_URL (compared to list @pdf_stage;) 


/*--------------------------
Create a table of pdf urls (uses the internal stage)
--------------------------*/
-- CREATE OR REPLACE TABLE pdf_files (
--     file_path STRING
-- );

CREATE OR REPLACE TABLE pdf_files AS
SELECT
    relative_path AS file_name,
    BUILD_SCOPED_FILE_URL(
        '@UNSTRUCTURED_DATA.PDF.PDF_STAGE',
        relative_path
    ) AS pdf_url
FROM DIRECTORY(@UNSTRUCTURED_DATA.PDF.PDF_STAGE)
WHERE LOWER(relative_path) LIKE '%.pdf';


select * from pdf_files;







