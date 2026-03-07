/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Test pdf extraction
--------------------------*/
CALL PROCESS_PDFS();

SELECT * FROM PDF_TEXT_OUTPUT;

-- truncate table PDF_TEXT_OUTPUT;
