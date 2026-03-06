/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;



/*--------------------------
Extract text using cortex
--------------------------*/

SELECT
    relative_path,
    AI_EXTRACT(
        GET_FILE(@pdf_s3_stage, relative_path),
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT('document', OBJECT_CONSTRUCT())
    ):"document"::string AS text
FROM DIRECTORY(@pdf_s3_stage);





