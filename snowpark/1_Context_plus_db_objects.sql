create database if not exists unstructured_data;
create schema if not exists pdf;
-- Set context
use database unstructured_data;
use schema pdf;
-- explore context
-- SHOW DATABASES LIKE 'UNSTRUCTURED_DATA';
-- SHOW SCHEMAS LIKE 'PDF'

/*--------------------------
Create stage and load dome pdf files via snowsight
--------------------------*/
CREATE stage if not exists pdf_stage
  DIRECTORY = ( ENABLE = TRUE );



