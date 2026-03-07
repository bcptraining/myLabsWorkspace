/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Test pdf extraction
--------------------------*/
CALL PROCESS_PDFS();
CALL PROCESS_PDFS('@UNSTRUCTURED_DATA.PDF.PDF_STAGE');
CALL PROCESS_PDFS('@UNSTRUCTURED_DATA.PDF.PDF_STAGE','my_pdf_text_output');

SELECT * FROM PDF_TEXT_OUTPUT;
SELECT * FROM MY_PDF_TEXT_OUTPUT;

-- desc table PDF_TEXT_OUTPUT;

-- truncate table PDF_TEXT_OUTPUT;
