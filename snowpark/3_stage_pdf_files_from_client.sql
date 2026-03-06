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
Create a table of pdf urls
--------------------------*/
CREATE OR REPLACE TABLE pdf_files (
    file_path STRING
);

INSERT INTO pdf_files
SELECT 'pdf_stage/' || relative_path
FROM DIRECTORY(@pdf_stage);
-- Confirm tge urls are available
select * from pdf_files;

/*--------------------------
Build scoped urls for extraction
--------------------------*/
SELECT
  relative_path,
  BUILD_SCOPED_FILE_URL(@pdf_stage, relative_path) AS file_url
FROM DIRECTORY(@pdf_stage);





